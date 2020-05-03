package part5rddtransformations

import org.apache.spark.sql.SparkSession

import scala.io.Source
import scala.util.Random

object ByKeyFunctions {

  val spark = SparkSession.builder()
    .appName("Skewed Joins")
    .master("local[2]")
    .getOrCreate()

  val sc = spark.sparkContext

  /*
    In the video, we copied the file from src/main/resources/data/lipsum/words.txt to spark-cluster/data.
    This will make it available under /opt/spark-data/.
    Alternatively, if you copied the entire src/main/resources/data folder to Docker, you will need to use the path where you copied it.

    Scenario: assume we have a dataset with (word, occurrences) which we obtained after scraping a big document or website.
    We want to aggregate and sum all values under a single map.
   */
  val words = Source.fromFile("/opt/spark-data/words.txt").getLines().toSeq

  // generate data
  val random = new Random
  val wordCounts = sc.parallelize(Seq.fill(2000000)(words(random.nextInt(words.length)), random.nextInt(1000)))

  // the most intuitive solution can be the most dangerous
  val totalCounts = wordCounts
    .groupByKey() // RDD of key = word, value = iterable of all previous values
    .mapValues(_.sum)

  // call an action
  totalCounts.collectAsMap()
  // ^^ 6s for 2M laptop sales [maybe adjust numbers]
  // ^^ look at the shuffle write - it shuffles the entire data

  /*
    groupByKey is dangerous in 2 ways:
      - it causes a shuffle so that data associated with one key stays on the same machine
      - it can cause memory errors if the data is skewed, i.e. data associated to one key has disproportionate representation and may not fit in an executor mem
   */

  /**
    Other byKey functions
   */

  /*
    ReduceByKey is the simplest - like a collection
    Also faster and safer because
    - it does a partial aggregate on the executor (operations done on the executors without shuffling are called map-side)
    - avoids the data skew problem
    - shuffles much less data
  */
  val totalCountsReduce = wordCounts.reduceByKey(_ + _)
  totalCountsReduce.collectAsMap()

  /*
    FoldByKey is similar to the collection fold function
    - needs a 0 value to start with
    - needs a combination function

    Similar performance
   */
  val totalCountsFold = wordCounts.foldByKey(0)(_ + _)
  totalCountsFold.collectAsMap()

  /*
    Aggregate by key is more general and needs a zero value and 2 combination functions
    - one that combines the current aggregated value with a new element
    - one that combines two aggregated values from different executors

    Similar performance
   */
  val totalCountsAggregate = wordCounts.aggregateByKey(0.0)(_ + _, _ + _)
  totalCountsAggregate.collectAsMap()

  /*
    CombineByKey is the most general function available that can combine values inside your RDD. You need
    - a function that turns a value into an aggregate value so that further aggregates can start from it
    - a function to combine a current aggregate with a value in the RDD inside the executor
    - a function to combine 2 aggregates between executors
    - a number of partitions, or a partitioner so that you can do further operations e.g. joins without additional shuffles

    CombineByKey can be as dangerous as groupByKey when the combination functions grow the data instead of shrinking it.
    Used correctly (i.e. when the functions are reduction functions), combineByKey is efficient and potentially much more efficient later on if you do joins.
   */
  val totalCountsCombine = wordCounts.combineByKey(
    (count: Int) => count,
    (currentSum: Int, newValue: Int) => currentSum + newValue,
    (partialSum1: Int, partialSum2: Int) => partialSum1 + partialSum2,
    numPartitions = 10
  )
  totalCountsCombine.collectAsMap()
  // collect still takes 2 seconds

  def main(args: Array[String]): Unit = {
    Thread.sleep(10000000)
  }
}
