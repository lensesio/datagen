package io.lenses.data.generator.domain.recursive

import io.lenses.data.generator.domain.DataGenerator

object CustomerGenerator extends DataGenerator[Customer] {
  override protected def generate(): Seq[(String, Customer)] = {
    List(
      "Jackie Chan" -> Customer("Jackie Chan", "123A", Nil),
      "Gica Petrescu" -> Customer("Gica Petrescu", "35436B", List(
        Customer("Ana Petrescu", "35436B", Nil)
      )),
      "Andri Popa" -> Customer("Andri Popa", "123t122", List(
        Customer("Leana Popa", "123t122", Nil),
        Customer("Micul Andri Popa", "123t122", Nil)
      ))
    )
  }
}