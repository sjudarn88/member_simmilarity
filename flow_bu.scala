import org.apache.spark.sql.functions._

import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}

case class Rating(entityId: String, productId: String, rating: Double)

val level = "cls"
val soar = "105"


val entityColumnId = "member_id"
val metricColumn = "UnitQty"
val divFilterSetStr = ""
val divFilterSet = if(divFilterSetStr.isEmpty) None else Some(divFilterSetStr.split(",").toSet)
val inputFile = "gs://syw-analytics-ff/member_trans_assortment_proc/als_proc/aditya/bu_level/" + level + "_" + soar + ".csv"
val outputFile = "gs://syw-analytics-ff/member_trans_assortment_proc/als_proc/aditya/bu_level/" + level + "_" + soar + "_etl.csv"
val outputFile_final = "gs://syw-analytics-ff/member_trans_assortment_proc/als_proc/aditya/bu_level/" + level + "_" + soar + "_jaccard.csv"

def processForAls(spark: SparkSession,
                      entityColumnId: String, //store or member_id
                      metricColumn: String, // UnitQty, MemberCount
                      divFilterSet: Option[Set[String]],
                      storeAgg: DataFrame): Dataset[Rating] = {

    import spark.sqlContext.implicits._
    val storeAggPreFilter = if(divFilterSet.isDefined) storeAgg.filter(s"div_no in (${divFilterSet.get.toSeq.mkString(",")})").as("data") else storeAgg.as("data")

    storeAggPreFilter.select(entityColumnId, "product_assortment_feature", metricColumn).
      withColumnRenamed(entityColumnId, "entityId").
      withColumnRenamed("product_assortment_feature", "productId").
      withColumnRenamed(metricColumn, "rating").as[Rating]
  }

  def save(spark: SparkSession, outputFile: String, df: Dataset[Rating]): Unit = {
    //val outputFile = "gs://syw-analytics-ff/member_trans_assortment_proc/als_proc/input_als_store_cls_41.csv"
    df.write.option("overwrite", "true").option("header", "true").csv(outputFile)
  }
  val inputDF = spark.read.option("header", "true").option("inferSchema", "true").csv(inputFile)
  val outputDF = processForAls(spark, entityColumnId, metricColumn,divFilterSet, inputDF)
  val outputDF1 = outputDF.filter($"UnitQty" > 0)
 // val outputDF2 = outputDF1.sample(false, 0.005)
  save(spark, outputFile, outputDF1)

/*---------------Index--------------------*/

import org.apache.spark.ml.feature.StringIndexer
import org.apache.spark.sql.SparkSession

val inputFile = "gs://syw-analytics-ff/member_trans_assortment_proc/als_proc/aditya/bu_level/" + level + "_" + soar + "_etl.csv"
val columnsToIndex = "entityId,productId".split(",").toList


val inputDF = spark.read.option("header", "true").csv(inputFile)
var colName = ""
var tmpDF = inputDF
for (colName <- columnsToIndex) {
  val df1 = new StringIndexer().setInputCol(colName).setOutputCol(s"${colName}Index").fit(tmpDF).transform(tmpDF)
  tmpDF = df1
}

tmpDF.printSchema
//tmpDF.show()
print("-----> Count of Indexed File: %s", tmpDF.count())

tmpDF.write.option("header", "true").option("overwrite", "true").csv("gs://syw-analytics-ff/member_trans_assortment_proc/als_proc/aditya/bu_level/" + level + "_" + soar + "_indexed.csv")

//-----Select-------
val selectColumns = "entityIdIndex,productIdIndex,rating".split(",")
val selectDF = tmpDF.select(selectColumns.head, selectColumns.tail:_*)

selectDF.write.option("header", "true").option("overwrite", "true").csv("gs://syw-analytics-ff/member_trans_assortment_proc/als_proc/aditya/bu_level/" + level + "_" + soar + "_indexed_selected.csv")

/*
selectDF.printSchema
//selectDF.show()
//-----Jaccard-------
import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val entityCol = "entityIdIndex"
val prodCol = "productIdIndex"
val ratingCol = "rating"

val toInt: (String) => Int = (value) => {
      value.toString.toDouble.toInt
    }
val toIntUDF = udf(toInt)

val inputDFtemp = selectDF.
      withColumn(s"${prodCol}_num", toIntUDF(selectDF.col(prodCol))).drop(prodCol).
      withColumnRenamed(s"${prodCol}_num", prodCol).
      select(entityCol, prodCol, ratingCol)

val inputDF1 = inputDFtemp 
//.sample(false, 0.005)

val howManyProds = inputDF1.select(prodCol).distinct().orderBy(desc(prodCol)).take(1).map(_.getInt(0)).head + 1
val howManyProdsB = spark.sparkContext.broadcast(howManyProds)

import spark.sqlContext.implicits._

val df1 = inputDF1.rdd.map(row => {
      val entityId = row.get(0).toString.toDouble.toInt
      val prodId = row.get(1).toString.toDouble.toInt
      val rating = row.get(2).toString.toDouble

      (entityId, (prodId, rating))
    }).groupByKey().map {
      case (entityId, iter) => {
        (entityId, Vectors.sparse(howManyProdsB.value, iter.toSeq))
      }
    }.toDF("id", "features")

val mh = new MinHashLSH().
          setNumHashTables(100).
          setInputCol("features").
          setOutputCol("hashes")

val model = mh.fit(df1)

val trdf = model.transform(df1)

print("-----> Count of Transformed DF  after MinHash: %s", trdf.count())

val simDF = model.approxSimilarityJoin(trdf, trdf, 100.6, "JaccardDistance").
          select(col("datasetA.id").alias("idA"), col("datasetB.id").alias("idB"), col("JaccardDistance"))

val entityColNameInOrigFile = "entityId"
val entityColIndexNameInOrigFile = "entityIdIndex"

val idMappingDF = tmpDF.select(entityColIndexNameInOrigFile, entityColNameInOrigFile).distinct()

val idMappingDF1 = idMappingDF.withColumn(s"${entityColIndexNameInOrigFile}_int",
      toIntUDF(idMappingDF.col(entityColIndexNameInOrigFile))).drop(entityColIndexNameInOrigFile)

val simDFT1 = simDF.join(idMappingDF1, simDF.col("idA") === idMappingDF1.col(s"${entityColIndexNameInOrigFile}_int"),
      "left").select(entityColNameInOrigFile, "idB", "JaccardDistance").
      withColumnRenamed(entityColNameInOrigFile, "entityA")

val simDFT2 = simDFT1.join(idMappingDF1, simDFT1.col("idB") === idMappingDF1.col(s"${entityColIndexNameInOrigFile}_int"),
      "left").select("entityA", entityColNameInOrigFile, "JaccardDistance").
      withColumnRenamed(entityColNameInOrigFile, "entityB")

simDFT2.write.option("header", "true").option("overwrite", "true").csv(outputFile_final)

//print("-----> Count of Final DF: %s", simDFT2.count())
*/
System.exit(0)
