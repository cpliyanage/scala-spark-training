import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession
//import org.apache.spark.sql.functions.{col, explode, collect_list, udf, to_json, str}
import org.apache.spark.sql._
import scala.math.BigDecimal

import org.apache.spark.sql.functions._

object BestSeller {

  def main(args: Array[String]): Unit = {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val spark =  SparkSession.builder().master("local").getOrCreate()
    val sc = spark.sparkContext
    import spark.implicits._

    //Start -- Calculating score for product

    val p1p2 = spark.read.format("csv").     // Use "csv" regardless of TSV or CSV.
      option("header", "false").  // Does the file have a header line?
      option("delimiter", "\t"). // Set delimiter to tab or comma.
      load("in/bestSeller.csv").toDF("p1", "p2", "p1p2_revenue", "p1_revenue", "p1p2_percentage")


    val a = p1p2.withColumn("s", (col("p1p2_percentage")*100).cast("Decimal(14,4)"))
    a.show(false)

//    p1p2.show(false)
//    p1p2.printSchema()



    val groupResult: RDD[(String, List[String])] = p1p2.groupBy(col("p1")).agg(
      collect_list(col("p2")) as "p22",
      collect_list("p1p2_percentage") as "presentage")
      .map(r => {
        val p1 = r.getAs[String]("p1")
        val p2 = r.getAs[Seq[String]]("p22")
        val percentage = r.getAs[Seq[String]]("presentage")

        val zip = p2 zip(percentage)

        val jsonFormatter = (zip.toMap).toList.map(x =>(x._1+  ":"+ x._2.toDouble))
        (p1, jsonFormatter)

      }).rdd



    groupResult.printSchema()
//    groupResult.show(false)
   // groupResult



//    val toMap = udf((keys: Array[String], values: Array[String]) => {
//      keys.zip(values).toMap
//    })
//
   /* groupResult.show(false)
    groupResult.printSchema()*/
//
////    groupResult.withColumn("map", toMap($"p2C", $"presentage")).select(col("p1"), col("map")).show(false)


  }

}
