#!/bin/bash


for level in class line prod na
	do
	for soar in 105 104 103 102 101
		do
		for ssn in basic ss fw
			do
			echo 
			bq query --use_legacy_sql=False --destination_table apparel_jaccard_data.$level'_'$soar'_'$ssn --parameter=level::$level --parameter=soar:INT64:$soar --parameter=ssn::$ssn "with result as ( select member_id, case when @level = 'line' then concat(cast(div_no as string), Product_Type, cast(Season as string), cast(Season_Correspondence as string),cast(ProdIrlNbr as string), assortment_id_line) when @level = 'class' then concat(cast(div_no as string), Product_Type, cast(ln_no as string), cast(Season as string), cast(Season_Correspondence as string),cast(ProdIrlNbr as string), assortment_id_cls) when @level = 'prod' then concat(cast(div_no as string), Product_Type, cast(Season as string), cast(Season_Correspondence as string),cast(ProdIrlNbr as string), assortment_id_prod) when @level = 'na' then concat(cast(div_no as string), Product_Type, cast(Season as string), cast(Season_Correspondence as string),cast(ProdIrlNbr as string), assortment_id_na) end as product_assortment_feature, sum(UnitQty) as UnitQty, count(distinct member_id) as MemberCount from apparel_als_all_app.all_apparel_data1 where div_no in (select div_no from apparel_jaccard_data.soar_div where soar_no = cast(@soar as int64) group by 1) and Season1 = @ssn group by 1,2 ) , one_txn_mem as ( select member_id, count(UnitQty) as ttl_txn from result where UnitQty > 0 group by 1 having ttl_txn = 1) select a.* from (select * from result where UnitQty > 0 and UnitQty <= 20 ) as a left join one_txn_mem as b on a. member_id = b.member_id where b.member_id is null"
		       done
	       done
done
exit 0
