package part2foundations

import org.apache.spark.sql.SparkSession

object ReadingDAGs {

  ///////////////////////////////////////////////////////////////////// Boilerplate
  // you don't need this code in the Spark shell
  // this code is needed if you want to run it locally in IntelliJ

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Reading Query Plans")
    .getOrCreate()

  val sc = spark.sparkContext

  ///////////////////////////////////////////////////////////////////// Boilerplate

  // job 1
  sc.parallelize(1 to 1000000).count()
  // DAG with a single "box" - the creation of the RDD

  val rdd1 = sc.parallelize(1 to 1000000)

  // job 2
  rdd1.map(_ * 2).count()
  // DAG with one stage and two "boxes": one for creating the RDD and one for the map

  // job 3
  rdd1.repartition(23).count()
  // DAG with two stages:
  // stage 1 - the creation of the RDD + exchange
  // stage 2 - computation of the count

  // job 4 - same as query plans:
  val ds1 = spark.range(1, 10000000)
  val ds2 = spark.range(1, 20000000, 2)
  val ds3 = ds1.repartition(7)
  val ds4 = ds2.repartition(9)
  val ds5 = ds3.selectExpr("id * 3 as id")
  val joined = ds5.join(ds4, "id")
  val sum = joined.selectExpr("sum(id)")
  // complex DAG

  /**
    * Takeaway: the DAG is a visual representation of the steps Spark will perform to run a job.
    * It's the "drawing" version of the physical query plan.
    * Unlike query plans, which are only available for DataFrames/Spark SQL, DAGs show up for ANY job.
    */

}
