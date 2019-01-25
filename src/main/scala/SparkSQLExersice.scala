import org.apache.log4j.{Level, Logger}
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._

/**
  * Created by akash on 1/24/19.
  */
object SparkSQLExersice {
  def main(args: Array[String]): Unit = {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val session = SparkSession.builder().appName("DataframeExample").master("local[1]").getOrCreate()

    val dataFrameReader = session.read


    val responses = dataFrameReader
      .option("header", "false")
      .option("inferSchema", value = false)
      .csv("in/clickstream.csv")

    val responseWithSelectedColumns = responses.select(responses("_c0"), responses("_c1"), responses("_c2"), responses("_c3"))

    val groupedData = responseWithSelectedColumns.groupBy(responses("_c0"))
    val groupDataWithCount = groupedData.count()
    groupDataWithCount.orderBy(groupDataWithCount.col("count").desc).show(5)

    session.stop()

  }

}
