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
      listArr ::= cols(0)
    }

    val userMap = listArr.groupBy(identity).mapValues(_.size)
    userMap.keys.foreach {
      i =>
        print(i)
        println(" : " + userMap(i))
    }
    bufferedSource.close
  }

}
