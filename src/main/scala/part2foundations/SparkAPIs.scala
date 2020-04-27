package part2foundations

import org.apache.spark.sql.SparkSession

object SparkAPIs {

  /**
    * This application contains the code we wrote during the "Different Spark APIs" video.
    */

  val spark = SparkSession.builder()
  .config("spark.master", "local")
  .appName("Different Spark APIs")
  .getOrCreate()

  // for toDF
  import spark.implicits._

  val sc = spark.sparkContext

  // small count comparison
  val numbers = 1 to 1000000000
  val rdd = sc.parallelize(1 to 1000000000)
  rdd.count() // ~10s on camera - might vary on your PC

  val df = rdd.toDF("id")
  df.count() // ~16s - might vary
  val dfCount = df.selectExpr("count(*)") // same
  // look at the Spark UI - there's a wholestagecodegen step in the stage - that's Spark generating the appropriate bytecode to process RDDs behind the scenes
  // most of the time taken is just the RDD transformation - look at the time taken in stage 1

  val ds = spark.range(1, 1000000000)
  ds.count() // instant, 0.1s
  val dsCount = ds.selectExpr("count(*)")
  dsCount.show() // same
  ds.toDF("value").count() // same

  ds.rdd.count() // ~25s
  // cmd-click on the `rdd` implementation to see why this is so slow.

  /**
    * Notice that inside the same "realm", i.e. RDDs or DFs, the computation time is small.
    * Converting between them takes a long time.
    * That's because each row is processed individually.
    * Conversions are particularly bad in Python, because the data needs to go from the Python interpreter to the JVM AND back.
    *
    * Lesson 1: once decided on the API level, STAY THERE.
    */

  val rddTimes5 = rdd.map(_ * 5)
  rddTimes5.count() // ~20s
  // one stage

  val dfTimes5 = df.select("id * 5 as id")
  val dfTimes5Count = dfTimes5.selectExpr("count(*)")
  dfTimes5Count.show() // still 11-12s
  /*
    Notice there's no difference in the time taken, comparing with the original count.
    The RDD version multiplied every single row, but here, the multiplication is instant.
    Or is it?

    WHY?

    scala> dfTimes5Count.explain
    == Physical Plan ==
    *(2) HashAggregate(keys=[], functions=[count(1)])
    +- Exchange SinglePartition
       +- *(1) HashAggregate(keys=[], functions=[partial_count(1)])
          +- *(1) Project
             +- *(1) SerializeFromObject [input[0, int, false] AS value#2]
                +- Scan[obj#1]

    scala> dfCount.explain
    == Physical Plan ==
    *(2) HashAggregate(keys=[], functions=[count(1)])
    +- Exchange SinglePartition
       +- *(1) HashAggregate(keys=[], functions=[partial_count(1)])
          +- *(1) Project
             +- *(1) SerializeFromObject [input[0, int, false] AS value#2]
                +- Scan[obj#1]

    Same query plan! Spark removed the select altogether.
   */

  /**
    * Exercise: measure the time it takes to count the number of elements from the DS, multiplied by 5.
    * Try to explain the difference. It's ok if you have like an 80% explanation.
    */
  val dsTimes5 = ds.map(_ * 5)
  val dsTimes5Count = dsTimes5.selectExpr("count(*)")
  dsTimes5Count.show()
  /*
    7 seconds from 0.1 seconds! That's a 70x time increase.
    Let's explain:

    scala> dsCount.explain
    == Physical Plan ==
    *(2) HashAggregate(keys=[], functions=[count(1)])
    +- Exchange SinglePartition
       +- *(1) HashAggregate(keys=[], functions=[partial_count(1)])
          +- *(1) Project
             +- *(1) Range (1, 1000000000, step=1, splits=6)

    scala> dsTimes5Count.explain
    == Physical Plan ==
    *(2) HashAggregate(keys=[], functions=[count(1)])
    +- Exchange SinglePartition
       +- *(1) HashAggregate(keys=[], functions=[partial_count(1)])
          +- *(1) Project
             +- *(1) SerializeFromObject [input[0, bigint, false] AS value#71L]
                +- *(1) MapElements <function1>, obj#70: bigint
                   +- *(1) DeserializeToObject staticinvoke(class java.lang.Long, ObjectType(class java.lang.Long), valueOf, id#13L, true, false), obj#69: java.lang.Long
                      +- *(1) Range (1, 1000000000, step=1, splits=6)

    Different query plans. Because we're using a lambda there, Spark can't optimize it.
    So Spark has to "deserializeObject" by invoking Long.valueOf on each element in the DS, then map each element with the function, then serialize it back as a DS.

    The reason why Spark has to do that is that Spark doesn't have any information on the lambda, and thus is forced to apply it to each element.
   */

  /**
    * Lesson 2: use DFs most of the time. Spark optimizes most stuff away.
    * Lesson 3: Lambdas are impossible to optimize.
    */

}
