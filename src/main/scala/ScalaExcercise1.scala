import scala.io.Source

/**
  * Created by akash on 1/24/19.
  */
object ScalaExcercise1 {

  def main(args: Array[String]): Unit = {

    val bufferedSource = Source.fromFile("in/clickstream.csv")
    var listArr = List[String]()

    for (line <- bufferedSource.getLines) {
      val cols = line.split(",").map(_.trim)
      listArr ::= cols(2)
    }

    val userList = listArr.groupBy(identity).mapValues(_.size).toList
    val sortedList = userList.sortBy(_._2).reverse


    for (user <- sortedList) {
      print(user._1)
      println(" : " + user._2)
    }


  }
}
