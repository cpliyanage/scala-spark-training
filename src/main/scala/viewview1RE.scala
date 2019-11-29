import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql._

import scala.math.exp
import scala.util.{Failure, Success, Try}
import scala.language.postfixOps


object viewview1RE {
  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //Start -- Calculating score for product

//    val channel: Option[String] = Some("mobile web")
//    val usePostEvent: Option[String] = Some("true")
//
//    val parquetFileDF = spark.read.parquet("in/viewview1")
//    parquetFileDF.printSchema()

//    val endeca_sku: DataFrame = spark.read.format("com.databricks.spark.avro").load("in/endeca/part-00088.avro")
//    val a = filterClickEvent(channel, usePostEvent, parquetFileDF)

//    a.show()


    var arr1: Array[String] = Array("ef", "erfgwr", "", null)

    arr1.filter(map => map != null && !map.isEmpty).foreach(a => println(a))


  }

  def filterClickEvent (channel: Option[String], postEvent: Option[String], df: DataFrame)  = channel match {
    case None  => filterClickEvents(postEvent, df)
    case Some(value)  => filterByChannelAndClickEvent(value, df)
  }

  def filterClickEvents(usePostData: Option[String], df: DataFrame) =  usePostData match {
    case Some(value) => {
      value.trim match {
        case TRUE =>filterClickEventFromPostEvent(df)
        case _ =>filterForClickEvents(df)
      }
    }
    case None =>filterForClickEvents(df)

  }

  def filterClickEventFromPostEvent(df: DataFrame): DataFrame = {
    val result: DataFrame = df.filter(eventFilter(ClickEvent)(col(POST_EVENT_LIST)))
    (result)
  }

  def eventFilter(eventNo: Int) = udf((events: Seq[String]) => {
    events.flatMap { event: String =>
      Try(event.toInt) match {
        case Success(e) => Some(e)
        case Failure(ex) => None
      }
    }.contains(eventNo)
  })

  def filterForClickEvents(df: DataFrame): DataFrame = {
    val result: Dataset[Row] = df.filter((col(PAGE_TYPE) isNotNull) && (eventFilter(clickEvent)(col(POST_EVENT_LIST))) &&
      ((col(PAGE_TYPE) contains WebProductDetailsPage) || (col(PAGE_TYPE) contains TableProductDetailsPage)))

    (result)
  }

  def filterByChannelAndClickEvent(channelType: String, df: DataFrame): DataFrame = {
    val result: DataFrame = df.filter((col(CHANNEL) === channelType) && eventFilter(ClickEvent)(col(POST_EVENT_LIST)))
    (result)
  }



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
  val TRANS_DT = "trans_dt"
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

  val SKU_PRODUCT_PARENT_PRODUCT_ID = "sku.parent_product_id"
  val SKU_PARENT_PRODUCT_ID = "parent_product_id"
  val S_PRODUCT_ID = "productid"
  val VSCORE = "vscore"
  val VIEW_SCORE = "viewscore"
  val PRODUCT_SCORE = "product_score"

  val date = "date"
  val ei_str_id = "ei_str_id"

  //Channels
  val CHANEL_MOBILE_WEB = "mobile web"
  val CHANEL_TABLET_WEB = "tablet web"
  val CHANEL_All = "all"

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
  val purchaseEvent = 1
  val clickEvent = 2

  val TABLE_NAME = "db_gold.clickstreamrecord_denorm"

  val TRUE = "true"
  val ClickEvent = 2
  val event_list = "event_list"

  val location = "location"
  val outputSchema = List(id,location,date,productCode,quantity)

  val groupedProductCodes = "groupedProductCodes"

}



