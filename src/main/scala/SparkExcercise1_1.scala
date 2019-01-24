import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}

/**
  * Created by akash on 1/24/19.
  */
object SparkExcercise1_1 {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("avgHousePrice").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines = sc.textFile("in/clickstream.csv")

    val Rdd = lines.map(line => (line.split(",")(2)))
//    val Rdd = lines.map(line => (line.split(",")(2)) +" "+ (line.split(",")(3)))

    val PairRdd = Rdd.map(word => (word, 1))

    val Count = PairRdd.reduceByKey((x, y) => x + y)
    val swapKeyValue = Count.map(c => (c._2 , c._1))
    val sortedRDD = swapKeyValue.sortByKey(ascending = false)

    val DescOrderPairRDD = sortedRDD.map( a => (a._2,a._1))

    for ((word, count) <- DescOrderPairRDD.collect()) println(word + " : " + count)


  }

}
