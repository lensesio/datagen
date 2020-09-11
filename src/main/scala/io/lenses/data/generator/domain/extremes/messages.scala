package io.lenses.data.generator.domain.extremes

case class SimpleMessage(key: String, value: String)

object Nested {
  type Nested1x1[A, B] = Nested2[Nested1[A], Nested1[B]]
  type Nested1x2[A, B, C] = Nested2[Nested1[A], Nested2[B, C]]
  type Nested2x1[A, B, C] = Nested2[Nested2[A, B], Nested1[C]]
  type Nested2x2[A, B, C, D] = Nested2[Nested2[A, B], Nested2[C, D]]
  type CrazyNested1[A, B, C] = Nested2x2[Nested2x1[A, B, C], B, Nested2[C, A], Nested1[B]]
  type CrazyNested2[A, B, C] = Nested2x1[Nested2x2[A, B, Nested1x2[A, C, B], C], Nested1[Nested1[A]], B]
  type CrazyNested3[A, B, C] = Nested1x1[Nested2x2[Nested1[A], A, C, B], Nested2[A, B]]
}

import Nested._

object NestedMessage {
  type N1 = CrazyNested3[CrazyNested1[String, Int, String], Nested1[String], CrazyNested2[Int, String, String]]
  type N2 = CrazyNested2[Nested1x2[Int, String, String], CrazyNested3[String, String, String], CrazyNested1[Int, String, String]]
}

case class NestedMessage(key: String, nested1: NestedMessage.N1, nested2: NestedMessage.N2)
case class Nested1[A](nested: A)
case class Nested2[A, B](nested1: A, nested2: B)

object Generator {
  import NestedMessage.{N1, N2}

  def get[A](implicit a: Random[A]): A = a.next

  implicit val randomInt: Random[Int] = Random[Int](scala.util.Random.nextInt())

  implicit val randomBool: Random[Boolean] = Random[Boolean](scala.util.Random.nextBoolean())
  
  implicit def randomNested1[A](implicit randA: Random[A]): Random[Nested1[A]] =
    Random[Nested1[A]](Nested1(randA.next))
  
  implicit def randomNested2[A, B](implicit randA: Random[A], randB: Random[B]): Random[Nested2[A, B]] =
    Random[Nested2[A, B]](Nested2(randA.next, randB.next))

  implicit def randomSimpleMessage(implicit randKeyStr: KeyGenerator[String], randStr: Random[String]) =
    Random[SimpleMessage](SimpleMessage(randKeyStr.key.next, randStr.next))

  implicit def randomNestedMessage(implicit randKeyStr: KeyGenerator[String], randStr: Random[String]) =
    Random[NestedMessage](NestedMessage(randKeyStr.key.next, Random[N1].next, Random[N2].next))
}

case class KeyGenerator[A](key: Random[A])

object Random {
  def apply[A](f: => A): Random[A] = new Random[A] {
    def next: A = f
  }

  def apply[A](implicit a: Random[A]): Random[A] = a
}

trait Random[A] {
  def next: A
}