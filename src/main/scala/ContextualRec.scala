
import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql._
import org.apache.spark.sql.functions.{col, explode, _}

import scala.collection.mutable
import scala.io.Source
import scala.language.postfixOps
import scala.util.Try

object ContextualRec {

  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark =  SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    val parquetFileDF = spark.read.parquet("in/Contextual/click.parquet")
//    parquetFileDF.filter(col("post_prop5") === "1875785").show(false)


    //filter row  where product list array is empty
    val filter = parquetFileDF.filter(col("product_list") =!= "" && col("product_list").isNotNull)
//    filter.show(false)

    //extract product id from product list
    //Check for digit and return
    val p = parquetFileDF.withColumn("product",extractProductList(col("product_list")) )
    println("********************************")

//    p.show(false)

    //check for a valid query
    val fields_by_valid_sessions = p.withColumn("query", cleanseSearchQuery(3,10)(col("post_prop5")))
//    fields_by_valid_sessions.filter(col("query") =!= "" || col("query") =!= null).show(false)

    //drop rows with query null + drop unwanted columns
    val aaa = fields_by_valid_sessions.filter(col("query").isNotNull).drop("post_prop5", "product_list")

    println("********************************")
    val removeNull = fields_by_valid_sessions.filter(col("product") =!= "" || col("product") =!= null ).filter(col("query") =!= "" || col("query") =!= null)
//    removeNull.show(false)
//    aaa.show(false)



    //group data by post)visd high and low and visitnum
    //And collect list of product and query
    val valid_session_groups = fields_by_valid_sessions.groupBy(col("post_visid_high"), col("post_visid_low"), col("visit_num"))
                                          .agg(collect_list(col("product")) as "product",collect_list(col("query")) as "query")
//    valid_session_groups.show(false)
//    valid_session_groups.show()
   // valid_session_groups.printSchema()


    //get cros query product of product and query of two lists without any empty values
    val a = valid_session_groups.withColumn("cross", crossQueryProduct(col("product"), col("query")) )
//    a.show(false)

    //filter rows that are empty (cros == empty)
    val filteredCross = a.filter(size(col("cross")) =!=0).select(col("cross"))
    //filteredCross.show(false)
    //filteredCross.printSchema()

    // explode cross column to get pair of product query
    val explode1: DataFrame = filteredCross.select(explode(col("cross")) as "crossD")
//    explode1.show(false)

    //create two columns from key value pair
    val a2 = explode1.withColumn("query", col("crossD")(0))
                        .withColumn("product", col("crossD")(1))
//    a2.show(false)

    val cross_query_product = a2.select(col("query"), col("product"))
    //cross_query_product.show(false)
    //cross_query_product.printSchema()

    //group by product and query and count no.of product as frequency
    val per_query_product_count = cross_query_product.groupBy(col("query"), col("product")).agg(count(col("product"))).withColumnRenamed("count(product)", "freq")
//    per_query_product_count.show(false)
//    per_query_product_count.printSchema()

    val zipper = udf[Seq[(String, Long)], Seq[String], Seq[Long]](_.zip(_))

    val per_query = per_query_product_count.groupBy(col("query")).agg(collect_list(col("product")).as("product"),collect_list(col("freq")).as("freq"))
      .withColumn("groupedList", zipper(col("product"), col("freq"))).drop(col("freq")).drop(col("product"))
//    per_query.show(false)



    val long_list = per_query.filter(size(col("groupedList")) > 20)
    val short_list = per_query.filter(size(col("groupedList")) < 20)

//    long_list.show(false)
//    long_list.printSchema()
//-----------------------------------------------------------------------------------------------------------
    val group_long_list = long_list.select(col("query"), explode(col("groupedList")) as "aa")
//    group_long_list.show(false)
//    group_long_list.filter(col("aa._2")  > 2 ).groupBy(col("query")).agg(collect_list("aa")).show(false)

    //-- apply cut-off by freq for long-list only

//    To-Do Azkaban parameter 2
    val asd = group_long_list.filter(col("aa._2")  > 2 )
                        .orderBy(col("aa._2").desc).groupBy(col("query")).agg(collect_list("aa") as "a")
//    asd.show(false)

    val group_long_list_Final = asd.withColumn("productList", limitUDF(col("a._1"), lit(3))).drop(col("a"))
//    group_long_list_Final.printSchema()
//    group_long_list_Final.show(false)
//--------------------------------------------------------------------------------------------------------------

//    short_list.printSchema()
//    short_list.show(false)

    val s = short_list.select(col("query"), explode(col("groupedList")) as "aa")
    val ss = s.orderBy(col("aa._2").desc).groupBy(col("query")).agg(collect_list(col("aa")) as "list")
//    ss.show(false)

    //lit(3 )azkaban input of int
    val group_short_list = ss.withColumn("productList", limitUDF(col("list._1"), lit(3))).drop(col("list"))
//    group_short_list.show(false)

    val recomendation: Dataset[Row] = group_long_list_Final.union(group_short_list)
    recomendation.show(false)


    val result: RDD[(String, String)] = recomendation.map{ r =>
      val key = "contextualSearch"+FLOW_SEPARATOR+"searchTerm"+":"+r.getAs[String]("query")
      val rec: Seq[String] = r.getAs[Seq[String]]("productList")
      (key, rec.toList.mkString(","))
    }.rdd
    val finalRecomendation = result.toDF()
//    finalRecomendation.write.format("com.databricks.spark.csv").save("in/myFile.csv")


////    recomendation.show(false)
////    recomendation.printSchema()
//    val aaaaa: RDD[Row] = recomendation.rdd
//
//    val ak: RDD[(String, List[String])] = recomendation.map{ r =>
//      val key = r.getAs[String]("query")
//      val rec = r.getAs[Seq[String]]("productList")
//      (key, rec.toList)
//    }.rdd
//    ak.collect().foreach(println)


//    gs://kohls-dev-dse-recommendation/bigdata-spark-recommendation/stage/clickstream/view-view-2-0


  }


//   def getRecomendation(recomendations: DataFrame): RDD[(String, List[String])] = {
//
////    val result: RDD[(String, List[String])] = recomendations.map{ r =>
////      val key = "contextualSearch"+FLOW_SEPARATOR+"searchTerm"+":"+r.getAs[String]("query")
////      val rec = r.getAs[Seq[String]]("productList")
////      (key, rec.toList)
////    }.rdd
////    result
//  }
  val FLOW_SEPARATOR = "\u241E"

  val limitUDF = udf { (nums: Seq[String], limit: Int) => nums.take(limit) }

  val validate: (String => Boolean) = (arg: String) => Try(arg.matches(REGEX_ANY_ALPHA)).isSuccess
  val hasAnyAlpha = udf(validate)
  val REGEX_ANY_ALPHA = ".*[a-zA-Z].*"


  def crossQueryProduct = udf((product: Seq[String], query: Seq[String]) => {


    val builder = List.newBuilder[String]
    val builderQ = List.newBuilder[String]


      product.foreach(
        a =>
          if (a != ""){
            builder += a
          }
      )

    query.foreach(
      a =>
        if (a != "" && a.matches(".*[a-zA-Z].*")){

          builderQ += a
        }
    )

    val listP: List[String] = builder.result().distinct
    val listQ: List[String] = builderQ.result().distinct
    cartesianProduct(listQ, listP)



  })

  def cartesianProduct[T](lst: List[T]*): List[List[T]] = {

    /**
      * Prepend single element to all lists of list
      * @param e single elemetn
      * @param ll list of list
      * @param a accumulator for tail recursive implementation
      * @return list of lists with prepended element e
      */
    def pel(e: T,
            ll: List[List[T]],
            a: List[List[T]] = Nil): List[List[T]] =
      ll match {
        case Nil => a.reverse
        case x :: xs => pel(e, xs, (e :: x) :: a )
      }

    lst.toList match {
      case Nil => Nil
      case x :: Nil => List(x)
      case x :: _ =>
        x match {
          case Nil => Nil
          case _ =>
            lst.par.foldRight(List(x))( (l, a) =>
              l.flatMap(pel(_, a))
            ).map(_.dropRight(x.size))
        }
    }
  }

  def cleanseSearchQuery(minQ: Int, maxQ: Int) = udf((post_prop5: String) => {

    val builderQuery = List.newBuilder[String]
    if (post_prop5 == "" || post_prop5 == null){
      builderQuery
    }
    else {
      val stopWordList: List[String] = Source.fromFile("in/Contextual/stoplist.txt").getLines.toList
      val split: Array[String] = post_prop5.split(" ")

      for (word <- split){
        val a = passesAllRules(word, minQ, maxQ, stopWordList)
        if ( a){
          builderQuery += word
        }
      }
    }
    val ree = builderQuery.result().distinct.mkString(" ")
    ree
  })


  def passesAllRules (part: String, min: Int, max: Int, stopWordList: List[String]): Boolean = {
    return (passesLengthRule(part, min, max) && passesStopwordsRule(part, stopWordList))
  }

  def passesLengthRule (part: String, min: Int, max: Int): Boolean = {
    if(min <= 0){ return true }
    else return (part.length() >= min && part.length() <= max)
  }

  def passesStopwordsRule (part: String, stopWordList: List[String]): Boolean = {
    return  !stopWordList.contains(part)
  }






  def isAllDigits(x: String) = x forall Character.isDigit


  def extractProductList= udf((product_list: String) => {

    val product: Seq[String] = product_list.split(";")
    val productId: String = if (product.size > 1 && isAllDigits(product(1))) product(1) else ""

    productId
  })


//  val stopWordList = Source.fromFile("in/Contextual/stoplist.txt").getLines.toList
//  println(stopWordList)
//  for (line <- stopWordList) {
//    println(line)
//  }

}
