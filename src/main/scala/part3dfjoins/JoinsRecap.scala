package part3dfjoins

import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object JoinsRecap {

  val spark = SparkSession.builder()
    .master("local[2]")
    .appName("Joins Recap")
    .getOrCreate()

  val sc = spark.sparkContext

  val guitarsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitars")

  val guitaristsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/guitarPlayers")

  val bandsDF = spark.read
    .option("inferSchema", "true")
    .json("src/main/resources/data/bands")

  // inner joins
  val joinCondition = guitaristsDF.col("band") === bandsDF.col("id")
  val guitaristsBandsDF = guitaristsDF.join(bandsDF, joinCondition, "inner")

  // outer joins
  // left outer = everything in inner join + all the rows in the LEFT table, with nulls in the rows not passing the condition in the RIGHT table
  guitaristsDF.join(bandsDF, joinCondition, "left_outer")
  // right outer = everything in inner join + all the rows in the RIGHT table, with nulls in the rows not passing the condition in the LEFT table
  guitaristsDF.join(bandsDF, joinCondition, "right_outer")
  // outer join = everything in left_outer + right_outer
  guitaristsDF.join(bandsDF, joinCondition, "outer")

  // semi joins = everything in the left DF for which THERE IS a row in the right DF satisfying the condition
  // essentially a filter
  guitaristsDF.join(bandsDF, joinCondition, "left_semi")

  // anti join = everything in the left DF for which THERE IS NOT a row in the right DF satisfying the condition
  // also a filter
  guitaristsDF.join(bandsDF, joinCondition, "left_anti")

  // cross join = everything in the left table with everything in the right table
  // dangerous: NRows(crossjoin) = NRows(left) x NRows(right)
  // careful with outer joins with non-unique keys

  // RDD joins
  val colorsScores = Seq(
    ("blue", 1),
    ("red", 4),
    ("green", 5),
    ("yellow", 2),
    ("orange", 3),
    ("cyan", 0)
  )
  val colorsRDD: RDD[(String, Int)] = sc.parallelize(colorsScores)
  val text = "The sky is blue, but the orange pale sun turns from yellow to red"
  val words = text.split(" ").map(_.toLowerCase()).map((_, 1)) // standard technique for counting words with RDDs
  val wordsRDD = sc.parallelize(words).reduceByKey(_ + _) // counting word occurrence
  val scores: RDD[(String, (Int, Int))] = wordsRDD.join(colorsRDD) // implied join type is INNER


  def main(args: Array[String]): Unit = {

  }
}
