package com.landoop.data.generator.domain.payments

case class CreditCard(number: String,
                      customerFirstName: String,
                      customerLastName: String,
                      country: String,
                      currency: String,
                      blocked: Boolean)

object CreditCard {
  val Cards = Vector(
    CreditCard("5162258362252394", "April", "Paschall", "GBR", "GBP", false),
    CreditCard("5290441401157247", "Charisse", "Daggett", "USA", "USD", false),
    CreditCard("5397076989446422", "Gibson", "Chunn", "USA", "USD", true),
    CreditCard("5248189647994492", "Hector", "Swinson", "NOR", "NOR", false),
    CreditCard("5196864976665762", "Booth", "Spiess", "CNA", "CAD", false),
    CreditCard("5423023313257503", "Hitendra", "Sibert", "CHE", "CHF", false),
    CreditCard("5337899393425317", "Larson", "Asbell", "SWE", "SEK", false),
    CreditCard("5140590381876333", "Zechariah", "Schwarz", "GER", "EUR", false),
    CreditCard("5524874546065610", "Shulamith", "Earles", "FRA", "EUR", true),
    CreditCard("5204216758311612", "Tangwyn", "Gorden", "GBR", "GBP", false),
    CreditCard("5336077954566768", "Miguel", "Gonzales", "ESP", "EUR", true),
    CreditCard("5125835811760048", "Randie", "Ritz", "NOR", "CAD", true),
    CreditCard("5317812241111538", "Michelle", "Fleur", "FRA", "EUR", true),
    CreditCard("5373595752176476", "Thurborn", "Asbell", "GBR", "GBP", true),
    CreditCard("5589753170506689", "Noni", "Gorden", "AUT", "EUR", true),
    CreditCard("5588152341005179", "Vivian", "Glowacki", "POL", "EUR", false),
    CreditCard("5390713494347532", "Elward", "Frady", "USA", "USD", true),
    CreditCard("5322449980897580", "Severina", "Bracken", "AUT", "EUR", false),
    CreditCard("5280153815233678", "Kara", "Moretti", "ITA", "EUR", false),
    CreditCard("5574906917600002", "Menaka", "Harsh", "GER", "EUR", false)
  )
}
