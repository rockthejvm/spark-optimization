package playground

import org.apache.spark.sql.SparkSession

/**
  * A simple Scala application where I'll invite you to play and fiddle with the code that we write in this course.
  * (not that you couldn't create your own, mind you.)
  *
  * If you can compile and run this application, it means that the libraries were downloaded correctly.
  * In that case, you should be good to go for the rest of the course.
  *
  * Enjoy!
  *
  * Daniel @ Rock the JVM
  */
object Playground {

  val spark = SparkSession.builder()
    .appName("Spark Optimization Playground")
    .master("local")
    .getOrCreate()

  val sc = spark.sparkContext

  def main(args: Array[String]): Unit = {
    val rdd = sc.parallelize(1 to 1000)
    println(s"I have my first RDD, it has ${rdd.count} rows. Now let me go optimize massive jobs.")
  }
}
