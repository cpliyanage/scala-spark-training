import scala.io.{BufferedSource, Source}

/**
  * Created by akash on 1/24/19.
  */
object ScalaExcercise1 {
  val bufferedSource = Source.fromFile("in/clickstream.csv")

  def main(args: Array[String]): Unit = {

    val listArray = getLinesToListArr(bufferedSource, getChoiseOfGrouping(List("user")))

    val groupedData = listArray.groupBy(identity).mapValues(_.size).toList
    val sortedListData = groupedData.sortBy(_._2).reverse

    printResult(sortedListData)

  }

  def getLinesToListArr (bufferedSourc: BufferedSource, list: List[Int]) : List[String] = {

    var listArr = List[String]()
    val listLen : Int = list.length

    for (line <- bufferedSourc.getLines) {
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

  def printResult (list :  Seq[(String, Int)]) = {
    for (user <- list.take(5)) {
      print(user._1)
      println(" : " + user._2)
    }
  }

  def getChoiseOfGrouping(list: List[String]) : List[Int] = {

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

}
