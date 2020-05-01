package part4rddjoins

import generator.{DataGenerator, Laptop, LaptopOffer}
import org.apache.spark.sql.SparkSession

object RDDSkewedJoins {

  val spark = SparkSession.builder()
    .appName("RDD Skewed Joins")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
    An online store selling gaming laptops.
    2 laptops are "similar" if they have the same make & model, but proc speed within 0.1

    For each laptop configuration, we are interested in the average sale price of "similar" models.

    Acer Predator 2.9Ghz aylfaskjhrw -> average sale price of all Acer Predators with CPU speed between 2.8 and 3.0 GHz
   */

  val laptops = sc.parallelize(Seq.fill(40000)(DataGenerator.randomLaptop()))
  val laptopOffers = sc.parallelize(Seq.fill(100000)(DataGenerator.randomLaptopOffer()))

  def plainJoin() = {
    val preparedLaptops = laptops.map {
      case Laptop(registration, make, model, procSpeed) => ((make, model), (registration, procSpeed))
    }

    val preparedOffers = laptopOffers.map {
      case LaptopOffer(make, model, procSpeed, salePrice) => ((make, model), (procSpeed, salePrice))
    }

    val result = preparedLaptops.join(preparedOffers) // RDD[(make, model), ((reg, cpu), (cpu, salePrice)))]
      .filter {
        case ((make, model), ((reg, laptopCpu), (offerCpu, salePrice))) => Math.abs(laptopCpu - offerCpu) <= 0.1
      }
      .map {
        case ((make, model), ((reg, laptopCpu), (offerCpu, salePrice))) => (reg, salePrice)
      }
      .aggregateByKey((0.0, 0))(
        {
          case ((totalPrice, numPrices), salePrice) => (totalPrice + salePrice, numPrices + 1) // combine state with record
        },
        {
          case ((totalPrices1, numPrices1), (totalPrices2, numPrices2)) => (totalPrices1 + totalPrices2, numPrices1 + numPrices2) // combine 2 states into one
        }
      ) // RDD[(String, (Double, Int))]
      .mapValues {
        case (totalPrices, numPrices) => totalPrices / numPrices
      }

    result.count()
  }

  def noSkewJoin() = {
    val preparedLaptops = laptops
      .flatMap { laptop =>
        Seq(
          laptop,
          laptop.copy(procSpeed = laptop.procSpeed - 0.1),
          laptop.copy(procSpeed = laptop.procSpeed + 0.1),
        )
      }
      .map {
        case Laptop(registration, make, model, procSpeed) => ((make, model, procSpeed), registration)
      }

    val preparedOffers = laptopOffers.map {
      case LaptopOffer(make, model, procSpeed, salePrice) => ((make, model, procSpeed), salePrice)
    }

    val result = preparedLaptops.join(preparedOffers) // RDD[(make, model, procSpeed), (reg, salePrice))
      .map(_._2)
      .aggregateByKey((0.0, 0))(
        {
          case ((totalPrice, numPrices), salePrice) => (totalPrice + salePrice, numPrices + 1) // combine state with record
        },
        {
          case ((totalPrices1, numPrices1), (totalPrices2, numPrices2)) => (totalPrices1 + totalPrices2, numPrices1 + numPrices2) // combine 2 states into one
        }
      ) // RDD[(String, (Double, Int))]
      .mapValues {
      case (totalPrices, numPrices) => totalPrices / numPrices
    }

    result.count()
  }


  def main(args: Array[String]): Unit = {
    noSkewJoin()
    Thread.sleep(1000000)
  }
}
