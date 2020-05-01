package part4rddjoins

import generator.DataGenerator
import org.apache.spark.HashPartitioner
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SparkSession

object SimpleRDDJoins {

  val spark = SparkSession.builder()
    .appName("RDD joins")
    .master("local[*]")
    .getOrCreate()

  val sc = spark.sparkContext
  val rootFolder = "src/main/resources/generated/examData"

  // DataGenerator.generateExamData(rootFolder, 1000000, 5)

  def readIds() = sc.textFile(s"$rootFolder/examIds.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1))
    }
    .partitionBy(new HashPartitioner(10))

  def readExamScores() = sc.textFile(s"$rootFolder/examScores.txt")
    .map { line =>
      val tokens = line.split(" ")
      (tokens(0).toLong, tokens(1).toDouble)
    }

  // goal: the number of students who passed the exam (= at least one attempt > 9.0)

  def plainJoin() = {
    val candidates = readIds()
    val scores = readExamScores()

    // simple join
    val joined: RDD[(Long, (Double, String))] = scores.join(candidates) // (score attempt, candidate name)
    val finalScores = joined
      .reduceByKey((pair1, pair2) => if(pair1._1 > pair2._1) pair1 else pair2)
      .filter(_._2._1 > 9.0)

    finalScores.count
  }

  def preAggregate() = {
    val candidates = readIds()
    val scores = readExamScores()

    // do aggregation first - 10% perf increase
    val maxScores: RDD[(Long, Double)] = scores.reduceByKey(Math.max)
    val finalScores = maxScores.join(candidates).filter(_._2._1 > 9.0)

    finalScores.count
  }

  def preFiltering() = {
    val candidates = readIds()
    val scores = readExamScores()

    // do filtering first before the join
    val maxScores = scores.reduceByKey(Math.max).filter(_._2 > 9.0)
    val finalScores = maxScores.join(candidates)

    finalScores.count
  }

  def coPartitioning() = {
    val candidates = readIds()
    val scores = readExamScores()

    val partitionerForScores = candidates.partitioner match {
      case None => new HashPartitioner(candidates.getNumPartitions)
      case Some(partitioner) => partitioner
    }

    val repartitionedScores = scores.partitionBy(partitionerForScores)
    val joined: RDD[(Long, (Double, String))] = repartitionedScores.join(candidates)
    val finalScores = joined
      .reduceByKey((pair1, pair2) => if(pair1._1 > pair2._1) pair1 else pair2)
      .filter(_._2._1 > 9.0)

    finalScores.count
  }

  def combined() = {
    val candidates = readIds()
    val scores = readExamScores()

    val partitionerForScores = candidates.partitioner match {
      case None => new HashPartitioner(candidates.getNumPartitions)
      case Some(partitioner) => partitioner
    }

    val repartitionedScores = scores.partitionBy(partitionerForScores)

    // do filtering first before the join
    val maxScores = repartitionedScores.reduceByKey(Math.max).filter(_._2 > 9.0)
    val finalScores = maxScores.join(candidates)

    finalScores.count
  }

  def main(args: Array[String]): Unit = {
    plainJoin()
    preAggregate()
    preFiltering()
    coPartitioning()
    combined()

    Thread.sleep(1000000)
  }
}
