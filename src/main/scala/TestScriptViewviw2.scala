import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.{DataFrame, Dataset, SparkSession}



object TestScriptViewviw2 {


  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark =  SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._
    val dataFrameReader = spark.read

    val sparkdata = spark.read.format("csv").option("delimiter", "\t")
            .option("header", "false")
            .load("/home/akash/Downloads/spark.csv")

    val pigdata = spark.read.format("csv").option("delimiter", "\t")
      .option("header", "false")
      .load("/home/akash/Downloads/pig")

    val difference = sparkdata.unionAll(pigdata).except(sparkdata.intersect(pigdata))
    difference.show(false)


////    val spark =  SparkSession.builder().master("local").getOrCreate()
////    val sc = spark.sparkContext
////    import spark.implicits._
////    val dataFrameReader = spark.read
////
//    val sparkdata = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load("gs://kohls-dev-dse-recommendation/bigdata-spark-recommendation/stage/clickstream/view-view-2-0/productpairsFinalscore/*.csv")
//
//    val pigData = spark.read.format("csv").option("delimiter", "\t").option("header", "false").load("gs://kohls-dev-dse-recommendation/stage/clickstream/avro/view-view-2-0/0/productpairs_finalscore/part-r-*")
//
//    val difference = sparkdata.unionAll(pigData).except(sparkdata.intersect(pigData))
//    difference.show(false)
//
////    //    val a1 = pigData.filter(col("_c0") === "2330383")
//
//    sparkdata.count()
//    sparkdata.show()
//    pigData.count
//    sparkdata.orderBy(col("_c0")).show

  }

}
