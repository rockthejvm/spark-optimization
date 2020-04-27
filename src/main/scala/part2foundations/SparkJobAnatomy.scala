package part2foundations

import org.apache.spark.sql.SparkSession


object SparkJobAnatomy {

  ///////////////////////////////////////////////////////////////////// Boilerplate
  // you don't need this code in the Spark shell
  // this code is needed if you want to run it locally in IntelliJ

  val spark = SparkSession.builder()
    .config("spark.master", "local")
    .appName("Spark Job Anatomy")
    .getOrCreate()

  val sc = spark.sparkContext

  ///////////////////////////////////////////////////////////////////// Boilerplate

  /**
    * Cluster prep
    *
    * 1. Navigate to the spark-optimization folder, go to spark-cluster/
    * 2. docker-compose up --scale spark-worker=3
    * 3. In another terminal:
    *    - docker-exec -it spark-cluster_spark-master_1 bash
    *    - cd spark/bin
    *    - ./spark-shell
    * 4. In (yet) another terminal:
    *    - go to spark-optimization
    *    - docker cp (the data folder) spark-cluster_spark-master_1:/tmp
    * 5. Open http//:localhost:4040 for the Spark UI
    */

  // job 1 - a count
  val rdd1 = sc.parallelize(1 to 1000000)
  rdd1.count
  // inspect the UI, one stage with 6 tasks
  // task = a unit of computation applied to a unit of data (a partition)

  // job 2 - a count with a small transformation
  rdd1.map(_ * 2).count
  // inspect the UI, another job with (still) one stage, 6 tasks
  // all parallelizable computations (like maps) are done in a single stage

  // job 3 - a count with a shuffle
  rdd1.repartition(23).count
  // UI: 2 stages, one with 6 tasks, one with 23 tasks
  // each stage is delimited by shuffles

  // job 4, a more complex computation: load a file and compute the average salary of the employees by department
  val employees = sc.textFile("/tmp/employees.csv")
  // process the lines
  val empTokens = employees.map(line => line.split(","))
  // extract relevant data
  val empDetails = empTokens.map(tokens => (tokens(4), tokens(7)))
  // group the elements
  val empGroups = empDetails.groupByKey(2)
  // process the values associated to each group
  val avgSalaries = empGroups.mapValues(salaries => salaries.map(_.toInt).sum / salaries.size)
  // show the result
  avgSalaries
    .collect() // this is an action
    .foreach(println)

  // look at the Spark UI: one job, 2 stages
  // the groupByKey triggers a shuffle, and thus the beginning of another stage
  // all other computations (maps, mapValues) are done in their respective stage
  // the number of tasks = the number of partitions processed in a given stage
}