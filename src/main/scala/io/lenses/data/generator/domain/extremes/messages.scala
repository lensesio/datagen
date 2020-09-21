package io.lenses.data.generator.domain.extremes

case class SimpleMessage(key: String, value: String)

object NestedMessage {
  type Nested2x1[A, B, C] = Nested2[Nested2[A, B], Nested1[C]]
  type Nested[A, B, C] = Nested2x1[Nested2x1[Nested1[A], Nested1[B], C], Nested2[A, C], Nested2[C, A]]
  type DeepNested = Nested[String, Int, Boolean]
}

case class NestedMessage(key: String, nested1: NestedMessage.DeepNested)
case class Nested1[A](nested: A)
case class Nested2[A, B](nested1: A, nested2: B)

object Generator {
  def get[A](implicit a: Random[A]): A = a.next

  implicit val randomInt: Random[Int] = Random[Int](scala.util.Random.nextInt())
  implicit val randomLong: Random[Long] = Random[Long](scala.util.Random.nextLong())
  implicit val randomBool: Random[Boolean] = Random[Boolean](scala.util.Random.nextBoolean())
  
  implicit def randomNested1[A](implicit randA: Random[A]) = Random(Nested1(randA.next))
  
  implicit def randomNested2[A, B](implicit randA: Random[A], randB: Random[B]) =
    Random(Nested2(randA.next, randB.next))

  implicit def randomSimpleMessage(implicit randKeyStr: KeyGenerator[String], randStr: Random[String]) =
    Random[SimpleMessage](SimpleMessage(randKeyStr.key.next, randStr.next))

  implicit def randomNestedMessage(implicit randKeyStr: KeyGenerator[String], randStr: Random[String]) =
    Random[NestedMessage](NestedMessage(randKeyStr.key.next, Random[NestedMessage.DeepNested].next))
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