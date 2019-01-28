import org.apache.log4j.{Level, Logger}
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

import scala.io.BufferedSource

/**
  * Created by akash on 1/24/19.
  */
object SparkExcercise1_1 {

  def main(args: Array[String]) {
    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf().setAppName("SparkClickCount").setMaster("local[3]")
    val sc = new SparkContext(conf)

    val lines: RDD[String] = sc.textFile("in/clickstream.csv")


    val listArr = ScalaExcercise1.getUserChoiseOfGroupingToIntList(List("user", "category"))

    val Rdd = getLinesToListArr(lines,listArr)

    val PairRdd = Rdd.map(word => (word, 1))

    val Count = PairRdd.reduceByKey((x, y) => x + y)
    val swapKeyValue = Count.map(c => (c._2 , c._1))
    val sortedRDD = swapKeyValue.sortByKey(ascending = false)

    val DescOrderPairRDD: RDD[(String, Int)] = sortedRDD.map(a => (a._2,a._1))

    printResult(DescOrderPairRDD)


  }

  def printResult (PairRDD: RDD[(String, Int)]) = {

    for ((word, count) <- PairRDD.collect()) println(word + " : " + count)
  }

  def getLinesToListArr (lines: RDD[String], list: List[Int]) : RDD[String]  = {

    val listLen : Int = list.length
      listLen match {
        case 1  => return lines.map(line => (line.split(",")(list(0))))
        case 2  => return lines.map(line => (line.split(",")(list(0)) + "," + (line.split(",")(list(1)))))
        case 3  => return lines.map(line => (line.split(",")(list(0)) + "," + (line.split(",")(list(1)))+ "," + (line.split(",")(list(2)))))
        case 4  => return lines.map(line => (line.split(",")(list(0)) + "," + (line.split(",")(list(1)))+ "," + (line.split(",")(list(2)))+ "," + (line.split(",")(list(3)))))
      }
  }
}
