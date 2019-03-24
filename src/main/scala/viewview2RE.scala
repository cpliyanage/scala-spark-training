import java.util.Optional

import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.spark.sql.functions.col


import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions._
import org.apache.spark.sql._
import java.util.Date
import java.text.SimpleDateFormat
import java.time.LocalDate
import java.time.temporal.ChronoUnit

import scala.math.exp
import scala.util.{Failure, Success, Try}
import scala.language.postfixOps

/**
  * Created by akash on 2/28/19.
  */
object viewview2RE {

  val channel: Option[String] = Some("tablet web")

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark =  SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val parquetFileDF = spark.read.parquet("in/viewview2.parquet")
    val flattenSource: Dataset[Row] = filterbyPagetype( channel, parquetFileDF)
    val a1 = flattenSkuProductColumn(flattenSource)
    val a2 = calcuateVscore(0.01, a1)
    a2.show(5)

    val cs_filtered_productviews = filterProductViews(a2)
    //flattenSource.show()

    // ************ first part ************************

    val cs_productviews_productscore_session_1 = combinedScoreOfProductInSession(cs_filtered_productviews)
    //cs_productviews_productscore_session_1.show()

    val product_scores = calculateScoresPerProductid(cs_productviews_productscore_session_1)
    //product_scores.show()

    // *************************First session with click stream done ********************************

    val productcatalog = spark.read.parquet("in/product")
//    productcatalog.printSchema()
//    productcatalog.show(5)
    val sessionProduct = renameProductFields(productcatalog)
    //product_scores.printSchema()
    //sessionProduct.printSchema()
    val productscores_p1p2p3 = sessionProduct.join(product_scores, "productid")
    //productscores_p1p2p3.show()

    // ***********************Product done ************************************

    val sku_data = spark.read.parquet("in/spark_sku")
    val c1 = sku_data.select(col("sku_id"), col("parent_product_id"), col("product_primary_type_value"), col("product_type_value"), col("product_sub_type_value"))
    val c2 = skuFilterProducts(c1)
    val sku_filtered_data = renameFilterProducts(c2)
    sku_filtered_data.show()


    val endeca_sku = spark.read.format("com.databricks.spark.avro").load("in/endeca/part-00088.avro")
    val inventory_skus = endeca_sku.select(col("s_code") as "sku_id").distinct()

    val inventory_products = inventory_skus.join(sku_filtered_data, "sku_id").select(col("product_id")).distinct().withColumnRenamed("product_id", "productid")
    //productscores_p1p2p3.printSchema()
    inventory_skus.printSchema()


    val available_productscores_p1p2p3 = productscores_p1p2p3.join(inventory_products, "productid")
    //available_productscores_p1p2p3.printSchema()
    //available_productscores_p1p2p3.show()

    val filtered_products = available_productscores_p1p2p3.filter(col("product_score") > 1.75)
    val filtered_products_1 = available_productscores_p1p2p3.filter(col("product_score") > 1.75)




    val cs_productviews_productscore =  cs_productviews_productscore_session_1
    val join_sessions = cs_productviews_productscore.as("s1").join(cs_productviews_productscore.as("s2")
      , col("s1."+POST_VISID_LOW) === col("s2."+POST_VISID_LOW) && col("s1."+POST_VISID_HIGH) === col("s2."+POST_VISID_HIGH))

    val productpairs_session = join_sessions.filter(col("s1."+PRODUCTID) =!= col("s2."+PRODUCTID) && col("s1.viewscore") <= col("s2.viewscore") )
    //productpairs_session.show()

    val get_Score = productpairs_session.withColumn("score", when(col("s1."+"viewscore") <= col("s2."+"viewscore"), col("s1."+"viewscore"))
      .otherwise(col("s2."+"viewscore")))
    //get_Score.show()
    val productpair_session_scores = get_Score.select(col("s1."+POST_VISID_HIGH) as POST_VISID_HIGH, col("s1."+POST_VISID_LOW) as POST_VISID_LOW,
      col("s1."+PRODUCTID) as "productid_A", col("s2."+PRODUCTID) as "productid_B", col("score") as "viewscore")
    //productpair_session_scores.show()

    val productpair_scores = productpair_session_scores.groupBy(col("productid_A"), col("productid_B"))
      .agg(sum(col("viewscore")))
      .withColumnRenamed("sum(viewscore)", "productpair_score")
    val filtered_productpairs = productpair_scores.filter(col("productpair_score") > 1.0)
    //filtered_productpairs.show()

    val join_productpair_product_details_A = filtered_productpairs.join(filtered_products
      , filtered_productpairs("productid_A") === filtered_products(PRODUCTID) )
    //join_productpair_product_details_A.show()
    val productpair_details_A = join_productpair_product_details_A.select(col("productid_A") , col("productid_B")
      , col("productpair_score") as "AB_score", col(PRODUCT_SCORE) as "A_score", col(P1) as "A_p1", col(P2) as "A_p2", col(P3) as "A_p3")
    //productpair_details_A.show()

    val someDF = Seq(
      (1089854, 1067778, 1.2712491503214047, 3.826523717330551, "Baby Gear",  "Nursery Furniture", "Changing Tables"),
      (1002470, 1034288, 1.2712491503214047, 3.826523717330551, "Baby Gear",  "Nursery Furniture", "Changing Tables"),
      (1067783, 1042666, 1.2712491503214047, 3.826523717330551, "Baby Gear",  "Nursery Furniture", "Changing Tables")
    ).toDF("productid_A", "productid_B", "AB_score", "A_score", "A_p1", "A_p2", "A_p3")


    val join_productpair_product_details_B = someDF.join(filtered_products_1,
      someDF("productid_B") === filtered_products_1(PRODUCTID))

    //someDF.show()
    //filtered_products_1.show()

    //join_productpair_product_details_B.show()

    val calculate_ismeasure_score = join_productpair_product_details_B.select(col("productid_A"), col("productid_B")
      , col("AB_score"), col("A_score") , col("A_p1"), col("A_p2"), col("A_p3")
      , col(PRODUCT_SCORE) as "B_score", col(P1) as "B_p1", col(P2) as "B_p2", col(P3) as "B_p3"
      , calculate_confidence(col("AB_score"), col("A_score")) as "confidence"
      , calculate_ismeasure(col("AB_score"), col("A_score"), col(PRODUCT_SCORE)) as "ismeasure")

    //calculate_ismeasure_score.show()

    val someDF1 = Seq(
      (1089854,1067778, 1.2712491503214047 , 3.826523717330551, "Baby Gear", "Nursery Furniture", "Changing Tables", 2.5424983006428095,    "Baby Gear","Nursery Furniture","Changing Tables", 0.33222037656890574, 0.40756617657069255)
         ).toDF("productid_A", "productid_B", "AB_score", "A_score", "A_p1", "A_p2", "A_p3", "B_score", "B_p1", "B_p2", "B_p3", "confidence", "ismeasure")

    val filter_p1p2p3 = someDF1.filter(col("A_p1") === col("B_p1")
      && col("A_p2") === col("B_p2")
      && col("A_p3") === col("B_p3"))
    //someDF1.show()
    //filter_p1p2p3.show()

    val filter_ismeasure_scores = filter_p1p2p3.filter(col("ismeasure") > 0.01)
    //filter_ismeasure_scores.show()

    val productpairs_finalscore: DataFrame = filter_ismeasure_scores.select(col("productid_A"), col("productid_B"), col("ismeasure") as "finalcalscore")
    //productpairs_finalscore.show()

    val someDF11 = Seq(
      ("1089854", "1067778",  0.40756617657069255 ),
      ("1089854", "1067777",  2.40756617657069255 ),
      ("1089854", "1067775",  8.40756617657069255 ),
      ("1089854", "1054512",  10.40756617657069255 ),
      ("1067778", "1064645",  0.40756617657069255 ),
      ("1067778", "1054552",  2.40756617657069255 ),
      ("1067778", "1021565",  5.40756617657069255 )

    ).toDF("productid_A", "productid_B", "finalcalscore")

    val result = someDF11.groupBy(col("productid_A"))
      .agg(collect_list(array(col("productid_A"), col("productid_B"), col("finalcalscore"))) as "collectedColumn")
      .map{r =>
        val key = r.getAs[String]("productid_A")
        val recs = r.getAs("collectedColumn").asInstanceOf[Seq[Seq[String]]]
          .map(r => (r(0), r(1), r(2)))
          .sortBy(_._3)

        val a = recs.map(r => r._2)

        (key,a.take(40).toList.mkString(","))
      }.toDF("key", "values")

    //someDF11.printSchema()
    result.show(20, false)





























  }

  def calculate_ismeasure = udf((AB_score: Double,A_score: Double, product_score: Double) => {
    AB_score / (math.sqrt(A_score) * math.sqrt(product_score))
  })

  def calculate_confidence = udf((AB_score: Double,A_score: Double) => {
    AB_score / A_score
  })

  def renameFilterProducts(df: Dataset[Row]) = {
    val rename_sku_filtered_data = df.select(col(SKU_ID) as SKU_ID, col(PARENT_PRODUCT_ID) as PRODUCT_ID,
      col(PRODUCT_PRIMARY_TYPE_VALUE) as DEPT, col(PRODUCT_TYPE_VALUE) as CAT, col(PRODUCT_SUB_TYPE_VALUE) as SUB_CAT)
    rename_sku_filtered_data
  }

  def skuFilterProducts(df: Dataset[Row]) = {
    val result = df.filter(validateIsNumbersUDF(col(SKU_ID)) &&
      (col(PARENT_PRODUCT_ID).isNotNull && validateIsNumbersUDF(col(PARENT_PRODUCT_ID))) &&
      col(PRODUCT_PRIMARY_TYPE_VALUE).isNotNull &&
      col(PRODUCT_TYPE_VALUE).isNotNull &&
      col(PRODUCT_SUB_TYPE_VALUE).isNotNull)

    result
  }

  //validate for numeric
  val validateIsNumbersfunc: (String => Boolean) = (arg: String) => Try(arg.matches(numberFilterRegex)).isSuccess
  val validateIsNumbersUDF = udf(validateIsNumbersfunc)
  val numberFilterRegex = "^[0-9]*$"

  def renameProductFields(df: Dataset[Row]) = {
    val productcatalog_selected_columns: DataFrame = df.select(col("product_id") as "productid", col("product_primary_type_value") as "p1",
      col("product_type_value") as "P2", col("product_sub_type_value")  as "P3" )
    productcatalog_selected_columns
  }

  def calculateScoresPerProductid (df: Dataset[Row]) = {
    val product_scores = df.groupBy(col(S_PRODUCT_ID))
      .agg(sum(col(VIEW_SCORE)))
      .withColumnRenamed("sum(viewscore)", PRODUCT_SCORE)
    product_scores
  }

  def combinedScoreOfProductInSession(df: Dataset[Row]) = {
    val cs_productviews_productscore_session = df.groupBy(col(POST_VISID_HIGH),col(POST_VISID_LOW), col(S_PRODUCT_ID))
      .agg(sum(col(VSCORE)))
      .withColumnRenamed("sum(vscore)", VIEW_SCORE)
    cs_productviews_productscore_session
  }

  def filterbyPagetype (channel :Option[String], df: Dataset[Row])  = channel match {

    case Some("mobile web") => (df.filter(col(EVENT_NM) === EVENT_PRODUCT_VIEW && col("host_name_class") === "mobile web" ))
    case Some("tablet web")  => (df.filter(col(EVENT_NM) === EVENT_PRODUCT_VIEW && col("host_name_class") === "tablet web" ))
    case _ => (df.filter(col(EVENT_NM) === EVENT_PRODUCT_VIEW))
  }

  def flattenSkuProductColumn(df: Dataset[Row])  = {
    val flattenSource: Dataset[Row] = df.select(col(POST_VISID_HIGH), col(POST_VISID_LOW), col(VISIT_NUM), col(DATE_TIME),
      explode(col(SKU_PRODUCT)), col(POST_CUST_HIT_GMT))
      .select(col(POST_VISID_HIGH), col(POST_VISID_LOW), col(VISIT_NUM),
        col(DATE_TIME), col(SKU_PRODUCT_ID), col(SKU_PRODUCT_QTY),  col(SKU_PRODUCT_PRICE), col(SKU_PRODUCT_DISPLAY_NAME),
        col(SKU_PRODUCT_DISPL_CLR_DESC), col(SKU_PRODUCT_SZ_DESC),col(SKU_PRODUCT_PARENT_PRODUCT_ID),col(SKU_PRODUCT_DELIVERY_METHOD),
        col(SKU_PRODUCT_PICKUP_SOTRE_ID),col(SKU_PRODUCT_DELIVERY), col(POST_CUST_HIT_GMT) )

    flattenSource
  }


  def vscore(decayValue: Double) = udf((longDate: Long) => {
    println(longDate.toInt)
    println(longDate)
    val date = new Date
    val dateFmt = "yyyy-MM-dd"
    val sdf = new SimpleDateFormat(dateFmt)
    val currentDate1 = sdf.format(date)
    val formated_post_cust_hit_gmt1 = sdf.format(convertLongToDate(longDate.toInt * 1000l))
    val currentDate = LocalDate.parse(currentDate1)
    val formated_post_cust_hit_gmt = LocalDate.parse(formated_post_cust_hit_gmt1)
    val noOfDaysBetween = ChronoUnit.DAYS.between(formated_post_cust_hit_gmt, currentDate)

    exp(decayValue * noOfDaysBetween.toDouble)
  })

  def calcuateVscore(decay_Value: Double, df: Dataset[Row])  = {
    val datasetWithVscore = df.withColumn(VSCORE, vscore(decay_Value)(col(POST_CUST_HIT_GMT)))
    datasetWithVscore
  }

  def convertLongToDate(l: Long): Date = new Date(l)

  def filterProductViews(df: Dataset[Row]) = {
    val filterProductID = df.select(col(POST_VISID_HIGH), col(POST_VISID_LOW), col(VISIT_NUM), col(VSCORE), col(SKU_ID),
      col(SKU_QTY), col(SKU_PRICE), col(SKU_PARENT_PRODUCT_ID) as S_PRODUCT_ID)
      .filter(col(SKU_PARENT_PRODUCT_ID) =!= "")
    filterProductID
  }



  val mobile = "mobile web"
  val tablet = "tablet web"
  val POST_VISID_HIGH = "post_visid_high"
  val POST_VISID_LOW = "post_visid_low"
  val VISIT_NUM = "VISIT_NUM"
  val PROP4 = "PROP4"
  val PROP5 = "PROP5"
  val POST_PROP4 = "POST_PROP4"
  val POST_PROP5 = "POST_PROP5"
  val POST_PROP6 = "POST_PROP6"
  val POST_EVAR8 = "POST_EVAR8"
  val POST_EVAR39 = "POST_EVAR39"
  val POST_EVAR49 = "POST_EVAR49"
  val PAGENAME = "PAGENAME"
  val PAGE_TYPE = "PAGE_TYPE"
  val DATE_TIME = "DATE_TIME"
  val EVENT_LIST = "EVENT_LIST"
  val PRODUCT_LIST = "PRODUCT_LIST"
  val POST_EVENT_LIST = "POST_EVENT_LIST"
  val POST_PRODUCT_LIST = "POST_PRODUCT_LIST"
  val TRANS_DT = "TRANS_DT"
  val SRC_CHNL_DESC = "src_chnl_desc"
  val CHANNEL = "CHANNEL"
  val EVENT_NM = "EVENT_NM"
  val SKU_PRODUCT = "SKU_PRODUCT"
  val GEO_ZIP = "GEO_ZIP"
  val GEO_REGION = "GEO_REGION"
  val POST_EVAR47 = "POST_EVAR47"
  val POST_EVAR15 = "POST_EVAR15"
  val productId = "parent_product_id"
  val CUSTOMER_ATG_ID = "customer_atg_id"
  val HOST_NAME_CLASS = "host_name_class"
  val POST_CUST_HIT_GMT = "post_cust_hit_time_gmt"
  val POST_EVAR3 = "post_evar3"
  val EVENT_PRODUCT_VIEW = "product view"
  val CHANEL_MOBILE_WEB = "mobile web"
  val CHANEL_TABLET_WEB = "tablet web"

  val SKU_PRODUCT_ID = "col.sku_id"
  val SKU_PRODUCT_QTY = "col.qty"
  val SKU_PRODUCT_PRICE = "col.price"
  val SKU_PRODUCT_DISPLAY_NAME = "col.display_name"
  val SKU_PRODUCT_DISPL_CLR_DESC = "col.sku_displ_clr_desc"
  val SKU_PRODUCT_SZ_DESC = "col.sku_sz_desc"
  val SKU_PRODUCT_PARENT_PRODUCT_ID = "col.parent_product_id"
  val SKU_PRODUCT_DELIVERY_METHOD = "col.delivery_mthd"
  val SKU_PRODUCT_PICKUP_SOTRE_ID = "col.pick_up_store_id"
  val SKU_PRODUCT_DELIVERY = "col.delivery"

  val SKU_ID = "sku_id"
  val SKU_QTY = "qty"
  val SKU_PRICE = "price"
  val SKU_DISPLAY_NAME = "display_name"
  val SKU_DISPL_CLR_DESC = "sku_displ_clr_desc"
  val SKU_SZ_DESC = "sku_sz_desc"
  val SKU_PARENT_PRODUCT_ID = "parent_product_id"
  val SKU_DELIVERY_METHOD = "delivery_mthd"
  val SKU_PICKUP_SOTRE_ID = "pick_up_store_id"
  val SKU_DELIVERY = "delivery"
  val S_PRODUCT_ID = "productid"

  val VSCORE = "vscore"
  val VIEW_SCORE = "viewscore"
  val PRODUCT_SCORE = "product_score"

  val POST_VISID_HIGH_2 = "post_visid_high_s2"
  val POST_VISID_LOW_2 = "post_visid_low_s2"
  val PRODUCT_ID_2 = "productid_s2"
  val VIEWSCORE_2 = "viewscore_s2"







  val year = "year"
  val month = "month"
  val day = "day"
  val isClickEvent = "isClickEvent"

  val id = "id"
  val quantity = "quantity"
  val productCode = "productCode"

  val ProductSkuListPattern = ";([c0-9]+?);".r
  val WebProductDetailsPage = "product detail page"
  val TableProductDetailsPage = "t|product details"

  val eventDelimeter = ","
  val comma = ","
  // Event types
  val clickEvent = 2

  //segment types
  val female = "female"
  val male = "male"
  val millenialMen = "millenialmen"
  val millenialWomen = "millenialwomen"
  val nonMillenialMen = "nonmillenialmen"
  val nonMillenialWomen = "nonmillenialwomen"
  val married = "married"
  val unmarried = "unmarried"
  val withChildren = "withchildren"
  val withoutChildren = "withoutchildren"


  val TABLE_NAME = "db_gold.clickstreamrecord_denorm"

  val rawClickstreamSchema = List( POST_VISID_HIGH, POST_VISID_LOW, VISIT_NUM, PROP4, PROP5, POST_PROP4,POST_PROP5,POST_PROP6,POST_EVAR8,POST_EVAR39, POST_EVAR49, PAGENAME, PAGE_TYPE,
    DATE_TIME, EVENT_LIST, PRODUCT_LIST, POST_EVENT_LIST, POST_PRODUCT_LIST,CHANNEL, EVENT_NM, SKU_PRODUCT, GEO_ZIP, GEO_REGION, POST_EVAR47, POST_EVAR15, CUSTOMER_ATG_ID)


  val viweview2Schema = List(POST_VISID_HIGH, POST_VISID_LOW, VISIT_NUM, DATE_TIME, EVENT_NM, POST_EVAR3, SKU_PRODUCT, POST_EVENT_LIST,
    HOST_NAME_CLASS, POST_CUST_HIT_GMT)



  val PRODUCTID = "productid"
  val PRODUCT_PRIMARY_TYPE_VALUE = "product_primary_type_value"
  val PRODUCT_TYPE_VALUE = "product_type_value"
  val PRODUCT_SUBTYPE_VALUE = "product_sub_type_value"


  val P1 = "p1"
  val P2 = "p2"
  val P3 = "p3"



  val productSchema = List (PRODUCTID, PRODUCT_PRIMARY_TYPE_VALUE, PRODUCT_TYPE_VALUE, PRODUCT_SUBTYPE_VALUE)


  val PARENT_PRODUCT_ID = "parent_product_id"
  val PRODUCT_SUB_TYPE_VALUE = "product_sub_type_value"
  val PRODUCT_WORKFLOW_STATUS = "product_workflow_status"
  val SKU_STATUS_CDE = "sku_status_cde"
  val PRODUCT_META_TITLE = "product_meta_title"
  val AGE_APPROPRIATE = "age_appropriate"
  val GENDER = "gender"
  val LEG_OPENING = "leg_opening"
  val LENGTH = "length"
  val MATERIAL_TYPE = "material_type"
  val NECKLINE = "neckline"
  val PERSONA_THEME = "persona_theme"
  val SILHOUETTE = "silhouette"
  val SKU_SIZE_RANGE = "sku_size_range"
  val SLEEVE_LENGTH = "sleeve_length"
  val PERSONA_SUBJECT = "persona_subject"
  val PERSONA_GROUP = "persona_group"
  val FIT = "fit"
  val PRODUCT_BRAND = "product_brand"
  val PATTERN = "pattern"
  val RISE = "rise"
  val ECOM_TREND = "ecom_trend"
  val ACTIVITY = "activity"
  val FORMALITY_OCCASION = "formality_occasion"
  val CHILD_AGE_RANGE = "child_age_range"
  val PRODUCT_WEB_DISPLAY_NM = "product_web_display_nm"
  val FEATURE = "feature"
  val PRODUCT_LONG_DESC = "product_long_desc"
  val PRODUCT_META_DESC = "product_meta_desc"
  val PRODUCT_META_KEYWORDS = "product_meta_keywords"
  val SPORTS_TEAM = "sports_team"

  val PRODUCT_ID = "product_id"
  val DEPT = "dept"
  val CAT = "cat"
  val SUB_CAT = "sub_cat"



  val FLOW_STATUS = 60
  val STATUS_CDE = "A"

  // Store Only product code -------------------------------------------------------------------------------------------
  val storeOnlyProductCode = "0"

  val viewview2Schema = List(SKU_ID, PARENT_PRODUCT_ID, PRODUCT_PRIMARY_TYPE_VALUE, PRODUCT_TYPE_VALUE, PRODUCT_SUB_TYPE_VALUE)


}
