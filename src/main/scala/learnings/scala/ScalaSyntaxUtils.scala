package learnings.scala

import scala.concurrent.Future
import scala.util.{Failure, Success}

object ScalaSyntaxUtils extends App {

  // values and variables
  val aBoolean: Boolean = false

  // expression
  val anIfExpression = if (2 > 3) "bigger" else "smaller"

  // instructions vs expression
  val theUnit: Unit = println("Hello") // Unit = "no meaningful value"

  // functions
  def myFunction(x: Int): Int = 42

  // OOP
  class Animal

  class Dog extends Animal // single parent class

  trait Carnivore { // interface
    def eat(animal: Animal): Unit
  }

  class crocodile extends Animal with Carnivore {
    override def eat(animal: Animal): Unit = println("Crunch")
  }

  // Singleton pattern
  object MySingleton {

  }

  // Companions
  object Carnivore // Companion of trait Carnivore: Can see each other private members

  // Generics
  trait MyList[A]

  // method notation
  val x = 1 + 2
  val y = 1.+(2)

  // Functional Programing: Functions in scala are instances of trait Function
  val incrementor: Function1[Int, Int] = new Function[Int, Int] {
    override def apply(v1: Int): Int = v1 + 1
  }

  // With Sugar
  val incrementerSugar: Int => Int = x => x + 1
  val increment = incrementerSugar(42)

  // Higher Order Functions
  val processedList = List(1, 2, 3).map(incrementerSugar)
  println(processedList)

  // Pattern matching
  val unknown: Any = 45
  val ordinal = unknown match {
    case 1 => "First"
    case 2 => "Second"
    case _ => "Unknown (default)"
  }
  println(ordinal)

  // try-catch
  try {
    throw new NullPointerException
  } catch {
    case _: NullPointerException => "got null"
    case _ => "Something else"
  }

  // Future

  import scala.concurrent.ExecutionContext.Implicits.global

  val aFuture = Future {
    462
  }

  aFuture.onComplete {
    case Success(value) => println(s"Value: $value")
    case Failure(exception) => println(s"Exception: $exception")
  }

  // Partial Function
  val aPartialFunction: PartialFunction[Int, Int] = {
    case 1 => 43
    case 8 => 56
    case _ => 990
  }

  // Implicits
  // auto-injected by the compiler
  def methodWithImplicitArgs(implicit x: Int) = x + 43

  implicit val implicitInt = 67
  val implicitCall = methodWithImplicitArgs

  // implicit conversions
  case class Person(name: String) {
    def greet = println(s"Hello $name")
  }

  implicit def fromStringTtoPerson(name: String) = Person(name)

  "Bob".greet // String automatically converted to Person fromStringTtoPerson("Bob").greet

  // implicit conversions - Implicit Class
  implicit class Cat(name: String) {
    def purr = println("Meow")
  }

  "Lassie".purr

  /* How Compiler decides implicits to pass
  - Local Scope
  - Global Scope - Imported
  - Companion objects of the type involved in method call
  */

}
