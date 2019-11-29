import scala.io.{BufferedSource, Source}
import scala.util.matching.Regex
import scala.language.postfixOps

/**
  * Created by akash on 1/24/19.
  */
object ScalaExcercise1 {
  val bufferedSource = Source.fromFile("in/clickstream.csv")

  def main(args: Array[String]): Unit = {

    val csvLines: List[String] = readCsvLinesToStringList(bufferedSource)
    val listArray: List[String] = getlistArrayByUserChoise(getUserChoiseOfGroupingToIntList(List("category")), csvLines)
    val sortedListData: List[(String, Int)] = aggregateTheResults(listArray)

    printResult(sortedListData)
  }


  def getlistArrayByUserChoise ( list: List[Int], lines: List[String]) : List[String] = {

    var listArr = List[String]()
    val listLen : Int = list.length

    for (line: String <- lines) {
      val cols = line.split(",").map(_.trim)
      listLen match {
        case 1  => listArr ::= cols(list(0))
        case 2  => listArr ::= cols(list(0)) + "," + cols(list(1))
        case 3  => listArr ::= cols(list(0))  + "," + cols(list(1))  + "," + cols(list(2))
        case 4  => listArr ::= cols(list(0))  + "," + cols(list(1))  + "," + cols(list(2))  + "," + cols(list(3))
      }
    }
    return listArr
  }

  def readCsvLinesToStringList (bufferedSrc: BufferedSource) : List[String] = {
    var lineList = List[String]()
    for (line: String <- bufferedSrc.getLines) {
      lineList ::= line
    }
    return lineList
  }

  def printResult (list :  Seq[(String, Int)]) = {
    for (user <- list.take(5)) {
      print(user._1)
      println(" : " + user._2)
    }
  }

  def getUserChoiseOfGroupingToIntList (list: List[String]) : List[Int] = {

    val builder = List.newBuilder[Int]
    val colors = Map("user" -> 0, "category" -> 1, "productId" -> 2, "channel" -> 3)

    for ((k,v) <- colors){
      list.foreach(
        a =>
          if (a == k){
            builder += v
          }
      )
    }
    return builder.result()
  }

  def aggregateTheResults (listArr: List[String]): List[(String, Int)] = {
    val groupedData: List[(String, Int)] = listArr.groupBy(identity).mapValues(_.size).toList
    return groupedData.sortBy(_._2).reverse
  }

}
