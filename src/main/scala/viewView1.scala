import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.functions.{col, explode}
import org.apache.spark.sql.{Dataset, Row, SparkSession}
import org.apache.commons.lang3.StringUtils
import org.apache.spark.sql.functions._
import org.apache.spark.storage.StorageLevel
import org.apache.spark.sql._

import scala.collection.JavaConversions.seqAsJavaList
import scala.math.exp
import scala.util.{Failure, Success, Try}
import scala.language.postfixOps
import scala.collection.mutable.ListBuffer

object viewView1 {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark = SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

//    //Start -- Calculating score for product
//
//    val parquetFileDF = spark.read.parquet("in/viewview2.parquet")
////    parquetFileDF.show(false)
//
//    val line = "PE018201804527901%E018201804527901"
//    val regex =  """(.{2})(.{4})(.{9})(.{2})""".r
//    val results = ListBuffer[List[String]]()
//
//    val mi = regex.findAllIn(line)
//    while (mi.hasNext) {
//      val d = mi.next
//      results += List(mi.group(1), mi.group(2), mi.group(3), mi.group(4))
//    }
//    // Demo printing
//    results.foreach { m =>
//      println("------")
//      println(m)
//      m.foreach { l => println(l) }

//    val s = "91nn-011-23413627"
//    val pattern = """([0-9]{1,3})[ -]([0-9]{1,3})[ -]([0-9]{4,10})""".r
//    val allMatches = pattern.findAllMatchIn(s)
//    allMatches.foreach { m =>
//      println("CC=" + m.group(1) + "  AC=" + m.group(2) + "  Number=" + m.group(3))
//    }
//
//    val matched = pattern.findFirstMatchIn(s)
//    matched match {
//      case Some(m) =>
//        println("CC=" + m.group(1) + "  AC=" + m.group(2) + "  Number=" + m.group(3))
//      case None =>
//        println("There wasn't a match!")
//    }


//    val parquetFileDF = spark.read.parquet("in/viewview2.parquet")
//    //parquetFileDF.printSchema
//
//    //    val flattenSource = parquetFileDF.select($"post_visid_high", $"post_visid_low", $"visit_num", $"date_time", explode($"sku_product"), $"post_cust_hit_time_gmt").select($"post_visid_high", $"post_visid_low", $"visit_num", $"date_time", $"col.sku_id", $"col.qty",  $"col.price", $"col.display_name", $"col.sku_displ_clr_desc", $"col.sku_sz_desc",$"col.parent_product_id",$"col.delivery_mthd", $"col.pick_up_store_id",$"col.delivery", $"post_cust_hit_time_gmt" )
//    //    val flattenSource: Dataset[Row] = parquetFileDF.select(col("post_visid_high"), col("post_visid_low"), col("visit_num"), col("date_time"), explode(col("sku_product")), col("post_cust_hit_time_gmt")).select($"post_visid_high", $"post_visid_low", $"visit_num", $"date_time", col("col.sku_id"), $"post_cust_hit_time_gmt" )
//
//    val flattenSource: Dataset[Row] = parquetFileDF.select(col("post_visid_high"), col("post_visid_low")
//      , col("visit_num"), col("date_time"), explode(col("sku_product")), col("post_cust_hit_time_gmt"))
//      .select(col("post_visid_high"), col("post_visid_low")).withColumn("aaaa", lit("Akash"))
//
//    flattenSource.show(false)

    val df = Seq(
      ("111111", "222222|333333", 1),
      ("3549892", "2374545", 2),
      ("3355808", "c1193954", 3),
      ("3392850", "3590158", 11),
      ("3481373", "3305043", 3)
    ).toDF("productA", "productB", "pairCount")


    val aaaa = df.withColumn("aaaa", findOrderedPairPermutations(col("productA"), col("productB"))).select(explode(col("aaaa")) as "new", col("pairCount"))
//    aaaa.select(col("new._1") as "productA", col("new._2") as "productB", col("pairCount")).show(false)

//    val df2 = df.groupBy("date", "location").agg(
//      count(col("date")) as "groupedProductCodes"
//    )

//    df2.show()

//    df.groupBy("date", "location").agg(count("date")).show()
//    import spark.implicits._
//    val az: Dataset[(String, Seq[(String, String)])] = df2.map(r => {
//      val a = r.getAs[Seq[String]]("food")
//      val name = r.getAs[String]("groupedProductCodes")
//      println(s"a: $a")
//      println(s"name: $name")
//      val productGroupin = a.toList.distinct
//      val xx: Seq[(String, String)] = for{
//        combination <- 2 to 4
//        productCombinations <- productGroupin.combinations(combination)
//      } yield{ productCombinations match {
//        case p1 :: p2 =>
//          println(p1 ,p2.mkString("|"))
//          (p1 ,p2.mkString("|"))
//
//      }
//      }
//     // val yy = xx.map(y => (name,y._1, y._2))
//      (name,xx)
//    })//.toDF().show(false)
//    val asdf = az.withColumn("asd", explode(col("_2")))
//    val ak = asdf.withColumn("p1", col("asd._1")).withColumn("p2", col("asd._2")).select( col("p1"), col("p2"))
//    val aaa = ak.groupBy(col("p1"), col("p2"))
//    println(s"aa: $aaa")
   // df2.show(false)



//    val listt: List[String] = "tt" :: "tt" :: "aaa" :: "11" :: "jkg" :: Nil
//
//
//    val productGrouping = listt.distinct.sortBy{ p =>
//      Try(p.toInt) match {
//        case Success(p) => p
//        case Failure(ex) => 0
//      }
//    }
//    val asd = for{
//      combination <- 2 to 4
//      productCombinations: Seq[String] <- productGrouping.combinations(combination)
//    } yield{ productCombinations match {
//      case p1 :: p2 => ( p1, p2.mkString("|"))
//    }

    val parquetasdFileDF = spark.read.parquet("in/Test/tv3.parquet")
    val parquetasdFileDF1 = spark.read.parquet("in/Test/tv2.parquet")
    parquetasdFileDF.show(false)
    parquetasdFileDF1.show(false)

    val result = parquetasdFileDF.map(r => {
      val groupedCodes = r.getAs[Seq[String]](groupedProductCodes)
      //      val date1 = r.getAs[String](date)
      //      val location1 = r.getAs[String](location)

      val productGroupin = groupedCodes.toList.distinct
      val splitted: Seq[(String, String)] = for{
        combination <- 2 to 2
        productCombinations <- productGroupin.combinations(combination)
      } yield{ productCombinations match {
        case p1 :: p2 => (p1 ,p2.mkString("|"))

      }
      }
      (splitted)
    })

      val aaaaa = result.where(size(col("value")) =!= 0).withColumn("temp", explode(col("value")))
      .withColumn(productA, col("temp._1")).withColumn(productB, col("temp._2")).select(col(productA), col(productB))
      .groupBy(col(productA), col(productB)).agg(count(productA) as pairCount)


//    result.show(false)
//    result.printSchema()
//
//    aaaaa.filter(col(pairCount) =!= 1)show(false)
//    aaaaa.printSchema()
//
//    result.where(size(col("value")) =!= 0).show(false)


    val listt: Seq[List[String]] = List((List("sdaad", "dafadff")))

    val dim: Seq[List[Int]] = (1 :: (0 :: (0 :: Nil))) ::
      (0 :: (1 :: (0 :: Nil))) ::
      (0 :: (0 :: (1 :: Nil))) :: Nil

//    val re = findOrderedPairPermutations("111111", "222222|333333")
//    re.foreach(a=> println(a))


//    val endeca_sku = spark.read.format("com.databricks.spark.avro").load("in/Test/tv1.avro")
//    endeca_sku.show(false)


    }

  def aa = udf((ss: List[String]) => {
    for{
      combination <- 2 to 2
      productCombinations: List[String] <- ss.combinations(combination)
    } yield {
      productCombinations.foreach(a => {println(a)})

      //    productGrouping.foreach(a => {println(a)})
    }
  })

  def findOrderedPairPermutations = udf((a: String, b: String) => {
    val aa = (a, b)
    val productHead = List(a)
    val productTail = b.split("\\|").toList
    productTail match {
      case ph :: Nil => List(aa)
      case ph :: pt => (productHead ++ productTail).permutations.toList
        .map{ permList => List(permList.head) ++ permList.tail.sortBy{ p =>
          Try(p.toInt) match {
            case Success(p) => p
            case Failure(ex) => 0
          }
        }
        }
        .distinct.map{ products => (products.head, products.tail.mkString("|")) }
    }


  })

//  def findOrderedPairPermutations(a: String,b: String):List[(String,String)] = {
//
//    val aa = (a, b)
//    val productHead = List(a)
//    val productTail = b.split("\\|").toList
//    productTail match {
//      case ph :: Nil => List(aa)
//      case ph :: pt => (productHead ++ productTail).permutations.toList
//        .map{ permList => List(permList.head) ++ permList.tail.sortBy{ p =>
//          Try(p.toInt) match {
//            case Success(p) => p
//            case Failure(ex) => 0
//          }
//        }
//        }
//        .distinct.map{ products => (products.head, products.tail.mkString("|")) }
//    }
//  }




  val CLICKSTREAM_INPUT_ARG = "dbClickStreamTableName"
  val OUTPUT_ARG = "output"
  val PRODUCT_COUNTS_PATH = "/product-counts"
  val CART_COUNTS_PATH = "/cart-counts"
  val PRODUCT_PAIR_COUNTS_PATH = "/product-pair-counts"
  val CHANNEL_ARG = "channel"
  val POST_EVENT_FLAG_ARG = "usePostEvent"
  val sparkCheckpointDirArg = "spark-checkpoint"


  val location = "location"
  val date = "date"
  val productA = "productA"
  val productB = "productB"
  val pairCount = "pairCount"
  val groupedProductCodes = "groupedProductCodes"

}

