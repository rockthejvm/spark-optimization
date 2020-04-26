package generator

case class LatopModel(make: String, model: String)
case class Laptop(registration: String, make: String, model: String, procSpeed: Double)
case class LaptopOffer(make: String, model: String, procSpeed: Double, salePrice: Double)
