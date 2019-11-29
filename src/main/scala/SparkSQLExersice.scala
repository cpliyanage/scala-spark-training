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
      .csv("/home/akash/Downloads/spark.csv")


    responses.show(1000,false)

    session.stop()

  }

}
