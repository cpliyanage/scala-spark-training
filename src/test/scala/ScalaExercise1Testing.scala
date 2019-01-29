import org.scalatest.FlatSpec
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by akash on 1/25/19.
  */
class ScalaExercise1Testing extends FlatSpec{

  val bufferedSource = Source.fromFile("in/Testing.csv")
  val listString: List[String] = List("user1,Clothing,product1,Webstore" , "user2,Kitchen,product2,Mobile", "user3,Clothing,product3,Webstore", "user1,Bathroom,product4,Tablet")
  val selectedColList = List("user1", "user3", "user2", "user1")
  val sortedList: List[(String, Int)] = List(("user1", 2),("user2", 1),("user3", 1))


  "readCsvLinesToStringList method" should "seperate lines of given csv file" in {
    val lines = ScalaExcercise1.readCsvLinesToStringList(bufferedSource)
    assert(lines === listString.reverse)
  }

  val userChoise = List("user")
  "getUserChoiseOfGroupingToIntList method" should "return list of Intergers that map the user choise" in {
    val list = ScalaExcercise1.getUserChoiseOfGroupingToIntList(userChoise)
    assert(list === List(0))
  }

  "getlistArrayByUserChoise method " should "return list consist with the fields specified by the user" in {
    val list1 = ScalaExcercise1.getlistArrayByUserChoise(List(0), listString)
    assert(list1 === selectedColList)
  }

  "aggregateTheResults method " should "return a list consist of aggregated resultrs" in {
    val sortedListData: List[(String, Int)] = ScalaExcercise1.aggregateTheResults(selectedColList)
    assert(sortedListData === sortedList)
  }


}
