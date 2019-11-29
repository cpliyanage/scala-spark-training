import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql._
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions.{col, _}
import java.util.Date
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.temporal.ChronoUnit
import scala.collection.mutable.LinkedHashSet

import scala.math.exp

/**
  * Created by akash on 2/14/19.
  */
object viewview2 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark =  SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //Start -- Calculating score for product

    val parquetFileDF = spark.read.parquet("in/viewview2.parquet")
    //parquetFileDF.printSchema

//    val flattenSource = parquetFileDF.select($"post_visid_high", $"post_visid_low", $"visit_num", $"date_time", explode($"sku_product"), $"post_cust_hit_time_gmt").select($"post_visid_high", $"post_visid_low", $"visit_num", $"date_time", $"col.sku_id", $"col.qty",  $"col.price", $"col.display_name", $"col.sku_displ_clr_desc", $"col.sku_sz_desc",$"col.parent_product_id",$"col.delivery_mthd", $"col.pick_up_store_id",$"col.delivery", $"post_cust_hit_time_gmt" )
//    val flattenSource: Dataset[Row] = parquetFileDF.select(col("post_visid_high"), col("post_visid_low"), col("visit_num"), col("date_time"), explode(col("sku_product")), col("post_cust_hit_time_gmt")).select($"post_visid_high", $"post_visid_low", $"visit_num", $"date_time", col("col.sku_id"), $"post_cust_hit_time_gmt" )

    val flattenSource: Dataset[Row] = parquetFileDF.select(col("post_visid_high"), col("post_visid_low")
      , col("visit_num"), col("date_time"), explode(col("sku_product")), col("post_cust_hit_time_gmt"))
      .select(col("post_visid_high"), col("post_visid_low"), col("visit_num"), col("date_time")
        , col("col.sku_id"), col("col.qty"),  col("col.price"), col("col.display_name")
        , col("col.sku_displ_clr_desc"), col("col.sku_sz_desc"),col("col.parent_product_id")
        ,col("col.delivery_mthd"), col("col.pick_up_store_id"),col("col.delivery")
        , col("post_cust_hit_time_gmt") )

    //flattenSource.printSchema
    //flattenSource.show()

    val datasetWithVscore = flattenSource.withColumn("vscore", vscore(0.01)(col("post_cust_hit_time_gmt")))

    val filterProductID = datasetWithVscore.select(col("post_visid_high"), col("post_visid_low"),
      col("visit_num"), col("vscore"), col("sku_id"), col("qty"), col("price"),
      col("parent_product_id") as "productid")
      .filter(col("parent_product_id") =!= "")
    //filterProductID.show()


    val cs_productviews_productscore_session_1 = filterProductID.groupBy(col("post_visid_high"), col("post_visid_low"), col("productid"))
                                                                .agg(sum(col("vscore")))
                                                                .withColumnRenamed("sum(vscore)", "viewscore")
    val cs_productviews_productscore_session_2 = cs_productviews_productscore_session_1.
      select(col("post_visid_high") as "post_visid_high_s2",
        col("post_visid_low") as "post_visid_low_s2",
        col("productid") as "productid_s2",
        col("viewscore") as "viewscore_s2")
    //cs_productviews_productscore_session.show()

    val product_scores: DataFrame = cs_productviews_productscore_session_1.groupBy(col("productid"))
                                                        .agg(sum(col("viewscore")))
                                                        .withColumnRenamed("sum(viewscore)", "product_score")
    //group_product_scores.show()

    //End -- Calculating score for product


    //Start -- Joining clickstraet score with product data


    val productcatalog = spark.read.parquet("in/product")
    //productcatalog.printSchema
    //productcatalog.show()

    val productcatalog_selected_columns: DataFrame = productcatalog.select(col("product_id") as "productid", col("product_primary_type_value") as "p1", col("product_type_value") as "p2", col("product_sub_type_value")  as "p3" )
    //productcatalog_selected_columns.printSchema()

    val join_product_score_productcatalog: DataFrame = product_scores.join(productcatalog_selected_columns,"productid")
    //join_product_score_productcatalog.printSchema()
   // join_product_score_productcatalog.show()

    val productscores_p1p2p3 = join_product_score_productcatalog.select(col("productid") as "product_id", col("product_score"), col("p1"), col("p2"), col("p3"))
    //productscores_p1p2p3.show()
    //productscores_p1p2p3.sort(col("productid").desc).show()
    //productscores_p1p2p3.withColumn("a", when($"productid" === "1002470", "df")).show()

    //End -- Joining clickstraet score with product data




//    val paramMapAtgSkuSource: Params = new Params(Map(Params.KEY_TBL_NM -> tableName))
//    val sku_filtered_data: AtgSkuSource = AtgSkuSource(paramMapAtgSkuSource)(AtgSkuSource.viewview2Schema)
//      .filterProducts
//      .rename_FilterProducts

    val sku_data = spark.read.parquet("in/spark_sku")
    //sku_data.show()

    val sku_filtered_data_step1 = sku_data.filter(col("product_primary_type_value") =!= "" && col("product_type_value") =!= "" && col("product_sub_type_value") =!= "")
    //sku_filtered_data_step1.show()

    val sku_filtered_data = sku_filtered_data_step1.select(col("sku_id") as "sku_id",
      col("parent_product_id") as "product_id",
      col("product_primary_type_value") as "dept",
      col("product_type_value") as "cat",
      col("product_sub_type_value") as "sub_cat")
      //sku_filtered_data.show()


    val endeca_sku = spark.read.format("com.databricks.spark.avro").load("in/endeca/part-00088.avro")
    //endeca_sku.show()
    val sku_ids = endeca_sku.select(col("s_code") as "sku_id")
    val inventory_skus = sku_ids.distinct()
//    inventory_skus.show()

    val inventory_sku_data_join = inventory_skus.join(sku_filtered_data,"sku_id")
   // inventory_sku_data_join.show()
    val distinct_products = inventory_sku_data_join.select(col("product_id")).distinct()

    val inventory_products = distinct_products
//    inventory_products.orderBy(col("product_id").asc).show()
//    productscores_p1p2p3.orderBy(col("product_id").asc).show()

    val available_products = productscores_p1p2p3.join(inventory_products, "product_id")
//    available_products.orderBy(col("product_id").asc).show()
    val available_productscores_p1p2p3 = available_products.withColumnRenamed("product_id", "productid")
//    available_productscores_p1p2p3.show()
//    available_productscores_p1p2p3.printSchema()

    val filtered_products = available_productscores_p1p2p3.filter(col("product_score") > 1.75)
    val filtered_products_1 = available_productscores_p1p2p3.filter(col("product_score") > 1.75)
//    filtered_products.show()




//--------------------------------------------------------------------------------------------------------------------------


    val join_sessions = cs_productviews_productscore_session_1.as("a").join(cs_productviews_productscore_session_1.as("b")
    , $"a.post_visid_low" === $"b.post_visid_low" &&
        $"a.post_visid_high" === $"b.post_visid_high")
    join_sessions.show()
    join_sessions.printSchema()
    val productpairs_session = join_sessions.filter(col("a.productid") =!= col("b.productid") &&
                                                col("a.viewscore") <= col("b.viewscore") )
//    productpairs_session.show()
//    productpairs_session.printSchema()


//    val productpair_scores = productpairs_session.groupBy(col("productid"), col("productid_s2"))
//      .agg(sum(col("vscore")))
//      .withColumnRenamed("sum(vscore)", "viewscore")
    val aa = "viewscore"


    val get_Score = productpairs_session.withColumn("Socre", when(col("a.viewscore") <= col("b.viewscore"), col("a."+aa)).otherwise(col("b."+aa)))
    get_Score.show()

    val productpair_session_scores = get_Score.select(col("a.post_visid_high") as "post_visid_high", col("a.post_visid_low") as "post_visid_low",
      col("a.productid") as "productid_A", col("b.productid") as "productid_B", col("Socre") as "viewscore")
//    productpair_session_scores.show()

    val productpair_scores = productpair_session_scores.groupBy(col("productid_A"), col("productid_B"))
                                              .agg(sum(col("viewscore")))
                                                    .withColumnRenamed("sum(viewscore)", "productpair_score")

//    productpair_scores.show()

    val filtered_productpairs = productpair_scores.filter(col("productpair_score") > 1.0)
//    filtered_productpairs.show()

    //----------------------------------------------------------------------------------------------------------------------------------


//    filtered_productpairs.show()
//    filtered_products.show()

    val join_productpair_product_details_A = filtered_productpairs.join(filtered_products
                                                   , filtered_productpairs("productid_A") === filtered_products("productid") )

    join_productpair_product_details_A.show()
    filtered_products.printSchema()
    filtered_products_1.printSchema()

    val productpair_details_A = join_productpair_product_details_A.select(col("productid_A") as "productid_A", col("productid_B") as "productid_B"
                                  , col("productpair_score") as "AB_score", col("product_score") as "A_score", col("p1") as "A_p1"
                                    , col("p2") as "A_p2", col("p3") as "A_p3")
//    productpair_details_A.show()


    val join_productpair_product_details_B = productpair_details_A.join(filtered_products_1
                                            , productpair_details_A("productid_B") === filtered_products_1("productid") )

//    join_productpair_product_details_B.printSchema()

    val calculate_ismeasure_score = join_productpair_product_details_B.select(col("productid_A") as "productid_A", col("productid_B") as "productid_B"
                    , col("AB_score")  as "AB_score", col("A_score") as "A_score", col("A_p1"), col("A_p2"), col("A_p3"),
      col("product_score") as "B_score", col("p1") as "B_p1", col("p2") as "B_p2", col("p3") as "B_p3"
          , calculate_confidence(col("AB_score"), col("A_score")) as "confidence"
              , calculate_ismeasure(col("AB_score"), col("A_score"), col("product_score")) as "ismeasure")


//    calculate_ismeasure_score.show()

//    val filter_p1p2p3 = calculate_ismeasure_score.filter(col("A_p1") === col("B_p1")
//                                                && col("A_p2") === col("B_p2") && col("A_p3") === col("B_p3"))
      val filter_p1p2p3 = calculate_ismeasure_score.filter(col("A_p1") === col("B_p1")
                                                    )

//    filter_p1p2p3.show()
//    filter_p1p2p3.printSchema()


    val filter_ismeasure_scores = filter_p1p2p3.filter(col("ismeasure") > 0.01)

    val productpairs_finalscore: DataFrame = filter_ismeasure_scores.select(col("productid_A"), col("productid_B"), col("ismeasure") as "finalcalscore")
//    productpairs_finalscore.show()



    //---------------------------------------------------------------------------------------------------------------------------------


//    val productid_recommendations: DataFrame = productpairs_finalscore.groupBy(col("productid_A")).agg(collect_list(array(col("productid_A"), col("productid_B"), col("finalcalscore"))) as "values")
//
//    val ordered_records: Dataset[Row] = productpairs_finalscore.orderBy(col("finalcalscore").desc)
//    val ordered_top: DataFrame = ordered_records.limit(40).toDF()

//    productid_recommendations.show()

//    val productid_recommendations = productpairs_finalscore.groupBy(col("productid_A"))calculate_confidence


    val result = productpairs_finalscore.groupBy(col("productid_A"))
      .agg(collect_list(array(col("productid_A"), col("productid_B"), col("finalcalscore"))) as "collectedColumn")
      .map{r =>
        val key: String = r.getAs[String]("productid_A")
        val recs: Seq[(String, String, String)] = r.getAs("collectedColumn").asInstanceOf[Seq[Seq[String]]]
          .map(r => (r(0), r(1), r(2)))
          .sortBy(_._3)

        val a = recs.map(r => (r._1,r._2))

        (key,a.take(40).toList.mkString(","))
      }.toDF("key", "values")

//    result.show()



  }

//  vscore(0.01)(col("post_cust_hit_time_gmt")
//  col("AB_score")/(math.sqrt((Double)col("A_score")) * math.sqrt((Double)col("product_score")))

  def convertLongToDate(l: Long): Date = new Date(l)

  def vscore(decayValue: Double) = udf((longDate: Long) => {
    val date = new Date
    val dateFmt = "yyyy-MM-dd"
    val sdf = new SimpleDateFormat(dateFmt)
    val currentDate1 = sdf.format(date)
    val formated_post_cust_hit_gmt1 = sdf.format(convertLongToDate(longDate * 1000l))
    val currentDate = LocalDate.parse(currentDate1)
    val formated_post_cust_hit_gmt = LocalDate.parse(formated_post_cust_hit_gmt1)
    val noOfDaysBetween = ChronoUnit.DAYS.between(formated_post_cust_hit_gmt, currentDate)

    exp(decayValue * noOfDaysBetween.toDouble)
  })

  def calculate_ismeasure = udf((AB_score: Double,A_score: Double, product_score: Double) => {
    AB_score / (math.sqrt(A_score) * math.sqrt(product_score))
  })

  def calculate_confidence = udf((AB_score: Double,A_score: Double) => {
    AB_score / A_score
  })
  def getOrderRecords = udf((productpairs_finalscore: DataFrame) => {
    productpairs_finalscore.orderBy(col("finalcalscore").desc).limit(40)
  })

}
