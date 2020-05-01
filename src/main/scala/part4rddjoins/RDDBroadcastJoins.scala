package part4rddjoins

import org.apache.spark.sql.SparkSession

import scala.util.Random

/**
  * Shown on camera in the Spark Shell.
  */
object RDDBroadcastJoins {

  val spark = SparkSession.builder()
    .appName("Broadcast Joins")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext

  val random = new Random()

  /*
    Scenario: assign prizes to a wide-scale competition (10M+ people).
    Goal: find out who won what.
   */

  // small lookup table
  val prizes = sc.parallelize(List(
    (1, "gold"),
    (2, "silver"),
    (3, "bronze")
  ))

  // the competition has ended - the leaderboard is known
  val leaderboard = sc.parallelize(1 to 10000000).map((_, random.alphanumeric.take(8).mkString))
  val medalists = leaderboard.join(prizes)
  medalists.foreach(println) // 38s for 10M elements!

  /*
    We know from SQL joins that the small RDD can be broadcast so that we can avoid the shuffle on the big RDD.
    However, for the RDD API, we'll have to do this manually.
    This lesson is more about how to actually implement the broadcasting technique on RDDs.
  */

  // need to collect the RDD locally, so that we can broadcast to the executors
  val medalsMap = prizes.collectAsMap()
  // after we do this, all executors can refer to the medalsMap locally
  sc.broadcast(medalsMap)
  // need to avoid shuffles by manually going through the partitions of the big RDD
  val improvedMedalists = leaderboard.mapPartitions { iterator => // iterator of all the tuples in this partition; all the tuples are local to this executor
    iterator.flatMap { record =>
      val (index, name) = record
      medalsMap.get(index) match { // notice you can refer to the name medalsMap, which you now have access to locally after the broadcast
        case None => Seq.empty
        case Some(medal) => Seq((name, medal))
      }
    }
  }

  improvedMedalists.foreach(println) // 2s, blazing fast, no shuffles or anything at all.

  def main(args: Array[String]): Unit = {
    Thread.sleep(1000000)
  }
}
