package part3dfjoins

import generator.DataGenerator
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SkewedJoins {

  val spark = SparkSession.builder()
    .appName("Skewed Joins")
    .master("local[*]")
    .config("spark.sql.autoBroadcastJoinThreshold", -1) // deactivate broadcast joins
    .getOrCreate()

  import spark.implicits._

  /*
    An online store selling gaming laptops.
    2 laptops are "similar" if they have the same make & model, but proc speed within 0.1

    For each laptop configuration, we are interested in the average sale price of "similar" models.

    Acer Predator 2.9Ghz aylfaskjhrw -> average sale price of all Acer Predators with CPU speed between 2.8 and 3.0 GHz
   */

  val laptops = Seq.fill(40000)(DataGenerator.randomLaptop()).toDS
  val laptopOffers = Seq.fill(100000)(DataGenerator.randomLaptopOffer()).toDS

  val joined = laptops.join(laptopOffers, Seq("make", "model"))
    .filter(abs(laptopOffers.col("procSpeed") - laptops.col("procSpeed")) <= 0.1)
    .groupBy("registration")
    .agg(avg("salePrice").as("averagePrice"))
  /*
    == Physical Plan ==
    *(4) HashAggregate(keys=[registration#4], functions=[avg(salePrice#20)])
    +- Exchange hashpartitioning(registration#4, 200), true, [id=#99]
       +- *(3) HashAggregate(keys=[registration#4], functions=[partial_avg(salePrice#20)])
          +- *(3) Project [registration#4, salePrice#20]
             +- *(3) SortMergeJoin [make#5, model#6], [make#17, model#18], Inner, (abs((procSpeed#19 - procSpeed#7)) <= 0.1)
                :- *(1) Sort [make#5 ASC NULLS FIRST, model#6 ASC NULLS FIRST], false, 0
                :  +- Exchange hashpartitioning(make#5, model#6, 200), true, [id=#77]
                :     +- LocalTableScan [registration#4, make#5, model#6, procSpeed#7]
                +- *(2) Sort [make#17 ASC NULLS FIRST, model#18 ASC NULLS FIRST], false, 0
                   +- Exchange hashpartitioning(make#17, model#18, 200), true, [id=#78]
                      +- LocalTableScan [make#17, model#18, procSpeed#19, salePrice#20]
   */

  val laptops2 = laptops.withColumn("procSpeed", explode(array($"procSpeed" - 0.1, $"procSpeed", $"procSpeed" + 0.1)))
  val joined2 = laptops2.join(laptopOffers, Seq("make", "model", "procSpeed"))
    .groupBy("registration")
    .agg(avg("salePrice").as("averagePrice"))
  /*
    == Physical Plan ==
    *(4) HashAggregate(keys=[registration#4], functions=[avg(salePrice#20)])
    +- Exchange hashpartitioning(registration#4, 200), true, [id=#107]
       +- *(3) HashAggregate(keys=[registration#4], functions=[partial_avg(salePrice#20)])
          +- *(3) Project [registration#4, salePrice#20]
             +- *(3) SortMergeJoin [make#5, model#6, knownfloatingpointnormalized(normalizenanandzero(procSpeed#43))], [make#17, model#18, knownfloatingpointnormalized(normalizenanandzero(procSpeed#19))], Inner
                :- *(1) Sort [make#5 ASC NULLS FIRST, model#6 ASC NULLS FIRST, knownfloatingpointnormalized(normalizenanandzero(procSpeed#43)) ASC NULLS FIRST], false, 0
                :  +- Exchange hashpartitioning(make#5, model#6, knownfloatingpointnormalized(normalizenanandzero(procSpeed#43)), 200), true, [id=#85]
                :     +- Generate explode(array((procSpeed#7 - 0.1), procSpeed#7, (procSpeed#7 + 0.1))), [registration#4, make#5, model#6], false, [procSpeed#43]
                :        +- LocalTableScan [registration#4, make#5, model#6, procSpeed#7]
                +- *(2) Sort [make#17 ASC NULLS FIRST, model#18 ASC NULLS FIRST, knownfloatingpointnormalized(normalizenanandzero(procSpeed#19)) ASC NULLS FIRST], false, 0
                   +- Exchange hashpartitioning(make#17, model#18, knownfloatingpointnormalized(normalizenanandzero(procSpeed#19)), 200), true, [id=#86]
                      +- LocalTableScan [make#17, model#18, procSpeed#19, salePrice#20]
   */

  def main(args: Array[String]): Unit = {
    joined2.show()
    joined2.explain()
    Thread.sleep(1000000)
  }
}
