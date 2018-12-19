import org.apache.spark.ml.feature.MinHashLSH
import org.apache.spark.ml.linalg.Vectors
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

val level = "cls"
val soar = "105"

val inputFile = "gs://syw-analytics-ff/member_trans_assortment_proc/als_proc/aditya/bu_level/" + level + "_" + soar + "_indexed_selected.csv"
val entityCol = "entityIdIndex"
val prodCol = "productIdIndex"
val ratingCol = "rating"
val originalInputFile = "gs://syw-analytics-ff/member_trans_assortment_proc/als_proc/aditya/bu_level/" + level + "_" + soar + "_indexed.csv"
val entityColNameInOrigFile = "entityId"
val entityColIndexNameInOrigFile = "entityIdIndex"
val outputFile = "gs://syw-analytics-ff/member_trans_assortment_proc/als_proc/aditya/bu_level/" + level + "_" + soar + "_jaccard.csv"

val toInt: (String) => Int = (value) => {
      value.toString.toDouble.toInt
    }

    val toIntUDF = udf(toInt)

    val inputDF = spark.read.option("header", "true").csv(inputFile)

val inputDFtemp = inputDF.
      withColumn(s"${prodCol}_num", toIntUDF(inputDF.col(prodCol))).drop(prodCol).
      withColumnRenamed(s"${prodCol}_num", prodCol).
      select(entityCol, prodCol, ratingCol)


val inputDF1 = inputDFtemp //.sample(false, 0.005)

//inputDF.count()
//inputDF1.count()

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

    val simDF = model.approxSimilarityJoin(trdf, trdf, 100.6, "JaccardDistance").
      select(col("datasetA.id").alias("idA"), col("datasetB.id").alias("idB"), col("JaccardDistance"))

    val idMappingDF = spark.read.option("header", "true").csv(originalInputFile).
      select(entityColIndexNameInOrigFile, entityColNameInOrigFile).distinct()

    val idMappingDF1 = idMappingDF.withColumn(s"${entityColIndexNameInOrigFile}_int",
      toIntUDF(idMappingDF.col(entityColIndexNameInOrigFile))).drop(entityColIndexNameInOrigFile)

    val simDFT1 = simDF.join(idMappingDF1, simDF.col("idA") === idMappingDF1.col(s"${entityColIndexNameInOrigFile}_int"),
      "left").select(entityColNameInOrigFile, "idB", "JaccardDistance").
      withColumnRenamed(entityColNameInOrigFile, "entityA")

    val simDFT2 = simDFT1.join(idMappingDF1, simDFT1.col("idB") === idMappingDF1.col(s"${entityColIndexNameInOrigFile}_int"),
      "left").select("entityA", entityColNameInOrigFile, "JaccardDistance").
      withColumnRenamed(entityColNameInOrigFile, "entityB")

    simDFT2.write.option("header", "true").option("overwrite", "true").csv(outputFile)

System.exit(0)
