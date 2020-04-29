package part3dfjoins

import org.apache.spark.sql.SparkSession

object Bucketing {

  val spark = SparkSession.builder()
    .appName("Bucketing")
    .master("local")
    .getOrCreate()

  import spark.implicits._

  // deactivate broadcasting
  spark.conf.set("spark.sql.autoBroadcastJoinThreshold", -1)

  val large = spark.range(1000000).selectExpr("id * 5 as id").repartition(10)
  val small = spark.range(10000).selectExpr("id * 3 as id").repartition(3)

  val joined = large.join(small, "id")
  joined.explain()
  /*
    == Physical Plan ==
    *(5) Project [id#2L]
    +- *(5) SortMergeJoin [id#2L], [id#6L], Inner
       :- *(2) Sort [id#2L ASC NULLS FIRST], false, 0
       :  +- Exchange hashpartitioning(id#2L, 200), true, [id=#40]
       :     +- Exchange RoundRobinPartitioning(10), false, [id=#39]
       :        +- *(1) Project [(id#0L * 5) AS id#2L]
       :           +- *(1) Range (0, 1000000, step=1, splits=1)
       +- *(4) Sort [id#6L ASC NULLS FIRST], false, 0
          +- Exchange hashpartitioning(id#6L, 200), true, [id=#47]
             +- Exchange RoundRobinPartitioning(3), false, [id=#46]
                +- *(3) Project [(id#4L * 3) AS id#6L]
                   +- *(3) Range (0, 10000, step=1, splits=1)

   */

  // bucketing
  large.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_large")

  small.write
    .bucketBy(4, "id")
    .sortBy("id")
    .mode("overwrite")
    .saveAsTable("bucketed_small") // bucketing and saving almost as expensive as a regular shuffle

  spark.sql("use default")
  val bucketedLarge = spark.table("bucketed_large")
  val bucketedSmall = spark.table("bucketed_small")
  val bucketedJoin = bucketedLarge.join(bucketedSmall, "id")
  bucketedJoin.explain()
  /*
    *(3) Project [id#11L]
    +- *(3) SortMergeJoin [id#11L], [id#13L], Inner
       :- *(1) Sort [id#11L ASC NULLS FIRST], false, 0
       :  +- *(1) Project [id#11L]
       :     +- *(1) Filter isnotnull(id#11L)
       :        +- *(1) ColumnarToRow
       :           +- FileScan parquet default.bucketed_large[id#11L] Batched: true, DataFilters: [isnotnull(id#11L)], Format: Parquet, Location: InMemoryFileIndex[file:/Users/daniel/dev/rockthejvm/courses/spark-optimization/spark-warehouse/bu..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4
       +- *(2) Sort [id#13L ASC NULLS FIRST], false, 0
          +- *(2) Project [id#13L]
             +- *(2) Filter isnotnull(id#13L)
                +- *(2) ColumnarToRow
                   +- FileScan parquet default.bucketed_small[id#13L] Batched: true, DataFilters: [isnotnull(id#13L)], Format: Parquet, Location: InMemoryFileIndex[file:/Users/daniel/dev/rockthejvm/courses/spark-optimization/spark-warehouse/bu..., PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 4 out of 4

   */

  // bucketing for groups
  val flightsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/flights/flights.json")
    .repartition(2)

  val mostDelayed = flightsDF
    .filter("origin = 'DEN' and arrdelay > 1")
    .groupBy("origin", "dest", "carrier")
    .avg("arrdelay")
    .orderBy($"avg(arrdelay)".desc_nulls_last)
  mostDelayed.explain()

  /*
    == Physical Plan ==
    *(4) Sort [avg(arrdelay)#53 DESC NULLS LAST], true, 0
    +- Exchange rangepartitioning(avg(arrdelay)#53 DESC NULLS LAST, 200), true, [id=#111]
       +- *(3) HashAggregate(keys=[origin#27, dest#24, carrier#18], functions=[avg(arrdelay#17)])
          +- Exchange hashpartitioning(origin#27, dest#24, carrier#18, 200), true, [id=#107]
             +- *(2) HashAggregate(keys=[origin#27, dest#24, carrier#18], functions=[partial_avg(arrdelay#17)])
                +- Exchange RoundRobinPartitioning(2), false, [id=#103]
                   +- *(1) Project [arrdelay#17, carrier#18, dest#24, origin#27]
                      +- *(1) Filter (((isnotnull(origin#27) AND isnotnull(arrdelay#17)) AND (origin#27 = DEN)) AND (arrdelay#17 > 1.0))
                         +- BatchScan[arrdelay#17, carrier#18, dest#24, origin#27] JsonScan Location: InMemoryFileIndex[file:/Users/daniel/dev/rockthejvm/courses/spark-optimization/src/main/resources..., ReadSchema: struct<arrdelay:double,carrier:string,dest:string,origin:string>
   */

//  flightsDF.write
//    .partitionBy("origin")
//    .bucketBy(4, "dest", "carrier")
//    .saveAsTable("flights_bucketed") // just as long as a shuffle
//
//  val flightsBucketed = spark.table("flights_bucketed")
//  val mostDelayed2 = flightsBucketed
//    .filter("origin = 'DEN' and arrdelay > 1")
//    .groupBy("origin", "dest", "carrier")
//    .avg("arrdelay")
//    .orderBy($"avg(arrdelay)".desc_nulls_last)
//  mostDelayed2.explain()
  /*
    == Physical Plan ==
    *(2) Sort [avg(arrdelay)#140 DESC NULLS LAST], true, 0
    +- Exchange rangepartitioning(avg(arrdelay)#140 DESC NULLS LAST, 200), true, [id=#172]
       +- *(1) HashAggregate(keys=[origin#114, dest#111, carrier#105], functions=[avg(arrdelay#104)])
          +- *(1) HashAggregate(keys=[origin#114, dest#111, carrier#105], functions=[partial_avg(arrdelay#104)])
             +- *(1) Project [arrdelay#104, carrier#105, dest#111, origin#114]
                +- *(1) Filter (isnotnull(arrdelay#104) AND (arrdelay#104 > 1.0))
                   +- *(1) ColumnarToRow
                      +- FileScan parquet default.flights_bucketed[arrdelay#104,carrier#105,dest#111,origin#114] Batched: true, DataFilters: [isnotnull(arrdelay#104), (arrdelay#104 > 1.0)], Format: Parquet, Location: PrunedInMemoryFileIndex[file:/Users/daniel/dev/rockthejvm/courses/spark-optimization/spark-wareho..., PartitionFilters: [isnotnull(origin#114), (origin#114 = DEN)], PushedFilters: [IsNotNull(arrdelay), GreaterThan(arrdelay,1.0)], ReadSchema: struct<arrdelay:double,carrier:string,dest:string>, SelectedBucketsCount: 4 out of 4
   */

  /**
    * Bucket pruning
    */
  val the10 = bucketedLarge.filter($"id" === 10)
  the10.show()
  the10.explain()
  /*
    == Physical Plan ==
    *(1) Project [id#11L]
    +- *(1) Filter (isnotnull(id#11L) AND (id#11L = 10))
       +- *(1) ColumnarToRow
          +- FileScan parquet default.bucketed_large[id#11L] Batched: true, DataFilters: [isnotnull(id#11L), (id#11L = 10)], Format: Parquet, Location: InMemoryFileIndex[file:/Users/daniel/dev/rockthejvm/courses/spark-optimization/spark-warehouse/bu..., PartitionFilters: [], PushedFilters: [IsNotNull(id), EqualTo(id,10)], ReadSchema: struct<id:bigint>, SelectedBucketsCount: 1 out of 4
   */

  def main(args: Array[String]): Unit = {
    // joined.count() // 4-5s
    // bucketedJoin.count() // 4s for bucketing + 0.5s for counting
    // mostDelayed.show() // ~1s
    // mostDelayed2.show() // ~0.2s = 5x perf!
  }

}
