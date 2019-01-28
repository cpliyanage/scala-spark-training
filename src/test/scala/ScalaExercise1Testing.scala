import org.scalatest.FlatSpec
import org.scalatest.{BeforeAndAfter, BeforeAndAfterAll, FlatSpec, Matchers}

import scala.collection.mutable
import scala.io.Source

/**
  * Created by akash on 1/25/19.
  */
class ScalaExercise1Testing extends FlatSpec{

//  "A Stack" should "pop values in last-in-first-out order" in {
//
//    val list = List("user4", "user1", "user3", "user2", "user1", "user4", "user1", "user3", "user2", "user1", "user5", "user5", "user3", "user2", "user5", "user4", "user1", "user3", "user2", "user1")
//    assert(ScalaExcercise1.getLinesToListArr(List(0)) === list)
//  }
  val bufferedSource = Source.fromFile("in/Testing.csv")
  val listString: List[String] = List("user1,Clothing,product1,Webstore" , "user2,Kitchen,product2,Mobile", "user3,Clothing,product3,Webstore", "user1,Bathroom,product4,Tablet")

  "readCsvLinesToStringList method" should "seperate lines of given csv file" in {


    val lines = ScalaExcercise1.readCsvLinesToStringList(bufferedSource)
    assert(lines === listString.reverse)

  }

  val userChoise = List("User")



  "getUserChoiseOfGroupingToIntList method" should "return list of Intergers that map the user choise" in {

    val list = ScalaExcercise1.getUserChoiseOfGroupingToIntList(userChoise)
    assert(list === List(0))

  }

  "getlistArrayByUserChoise method " should "return list consist with the fields specified by the user" in {


  }
sfbgdfg
//
//  it should "throw NoSuchElementException if an empty stack is popped" in {
//    val emptyStack = new mutable.Stack[String]
//    assertThrows[NoSuchElementException] {
//      emptyStack.pop()
//    }
//  }

//  "A Stack" should "pop values in last-in-first-out order" in {
//    val stack = new mutable.Stack[Int]
//    stack.push(1)
//    stack.push(2)
//    assert(stack.pop() === 2)
//    assert(stack.pop() === 1)
//  }
//
//  it should "throw NoSuchElementException if an empty stack is popped" in {
//    val emptyStack = new mutable.Stack[String]
//    assertThrows[NoSuchElementException] {
//      emptyStack.pop()
//    }
//  }

}
