package generator

import java.io.{File, FileWriter, PrintWriter}

import scala.annotation.tailrec
import scala.io.Source
import scala.util.Random

object DataGenerator {

  val random = new Random()

  /////////////////////////////////////////////////////////////////////////////////
  // General data generation
  /////////////////////////////////////////////////////////////////////////////////

  def randomDouble(limit: Double): Double = random.nextDouble() * limit

  def randomLong(limit: Long = Long.MaxValue): Long = Math.abs(random.nextLong()) % limit

  def randomInt(limit: Int = Int.MaxValue): Int = random.nextInt(limit)

  def randomIntBetween(low: Int, high: Int) = {
    assert(low <= high)
    random.nextInt(high - low) + low
  }

  def randomString(n: Int) =
    new String((0 to n).map(_ => ('a' + random.nextInt(26)).toChar).toArray)

  /////////////////////////////////////////////////////////////////////////////////
  // Laptop models generation - skewed data lectures
  /////////////////////////////////////////////////////////////////////////////////

  val laptopModelsSet: Seq[LatopModel] = Seq(
    LatopModel("Razer", "Blade"),
    LatopModel("Alienware", "Area-51"),
    LatopModel("HP", "Omen"),
    LatopModel("Acer", "Predator"),
    LatopModel("Asus", "ROG"),
    LatopModel("Lenovo", "Legion"),
    LatopModel("MSI", "Raider")
  )

  def randomLaptopModel(uniform: Boolean = false): LatopModel = {
    val makeModelIndex = if (!uniform && random.nextBoolean()) 0 else random.nextInt(laptopModelsSet.size) // 50% of the data is of the first kind
    laptopModelsSet(makeModelIndex)
  }

  def randomProcSpeed() = s"3.${random.nextInt(9)}".toDouble

  def randomRegistration(): String = s"${random.alphanumeric.take(7).mkString("")}"

  def randomPrice() = 500 + random.nextInt(1500)

  def randomLaptop(uniformDist: Boolean = false): Laptop = {
    val makeModel = randomLaptopModel()
    Laptop(randomRegistration(), makeModel.make, makeModel.model, randomProcSpeed())
  }

  def randomLaptopOffer(uniformDist: Boolean = false): LaptopOffer = {
    val makeModel = randomLaptopModel()
    LaptopOffer(makeModel.make, makeModel.model, randomProcSpeed(), randomPrice())
  }

  /////////////////////////////////////////////////////////////////////////////////
  // Misc data generation
  /////////////////////////////////////////////////////////////////////////////////

  /**
    * For the iterator-to-iterator transformations lecture.
    * Generates a number of metrics in the style of "metricName metricValue", where metricName is a string and metricValue is a double.
    *
    * @param destPath the path of the file the metrics will be written to.
    * @param nMetrics the number of metrics to generate
    * @param limit the maximum value any metric can take
    */
  def generateMetrics(destPath: String, nMetrics: Int, limit: Double = 1000000) = {
    val writer = new PrintWriter(new FileWriter(new File(destPath)))
    (1 to nMetrics).foreach(_ => writer.println(s"${randomString(16)} ${randomDouble(1000000)}"))
    writer.flush()
    writer.close()
  }

  /**
    * For the RDD joins & cogroup lectures. Generates 3 files:
    * 1) with student IDs and names
    * 2) with student IDs and emails
    * 3) with student IDs and exam attempt grade
    *
    * @param rootFolderPath the path where the 3 files will be written
    * @param nStudents the number of students
    * @param nAttempts the number of attempts of the exam, per each student
    */
  def generateExamData(rootFolderPath: String, nStudents: Int, nAttempts: Int): Unit = {
    val studentNames = (0 to nStudents).map(_ => randomString(16))
    val studentIds = studentNames.map(_ => randomLong())
    val idWriter = new PrintWriter(new FileWriter(new File(s"$rootFolderPath/examIds.txt")))
    val emailWriter = new PrintWriter(new FileWriter(new File(s"$rootFolderPath/examEmails.txt")))
    val scoreWriter = new PrintWriter(new FileWriter(new File(s"$rootFolderPath/examScores.txt")))

    studentNames
      .zip(studentIds)
      .foreach {
        case (name, id) =>
          idWriter.println(s"$id $name")
          emailWriter.println(s"$id $name@rockthejvm.com")
      }

    val scores = studentIds
      .flatMap(id => Seq.fill(5)(id))
      .map(id => (id, randomInt(10), randomInt(10)))
      .toSet

    scores.foreach {
      case (id, scoreMaj, scoreMin) => scoreWriter.println(s"$id $scoreMaj.$scoreMin")
    }

    idWriter.flush()
    idWriter.close()
    emailWriter.flush()
    emailWriter.close()
    scoreWriter.flush()
    scoreWriter.close()
  }

  /**
    * For the Secondary Sort lesson.
    * Generates random person encounters as key-value pairs in a CSV file.
    * The key is the person identifier and the value is the distance to the closest person, as measured by a hypothetical "approach device".
    *
    * @param path the file path to write the data to
    * @param nPeople the number of people involved in the data
    * @param nValuesPerPerson the number of people encounters
    * @param skew the percentage of the data that belongs to one person
    */
  def generatePeopleEncounters(path: String, nPeople: Int, nValuesPerPerson: Int, skew: Double = 0): Unit = {
    val writer = new PrintWriter(new FileWriter(new File(path)))
    val nEntries = nPeople * nValuesPerPerson

    writer.println("personId,approachValue")
    (1 to nEntries).foreach { _ =>
      val personIndex = if (random.nextDouble() < skew) 0 else 1 + random.nextInt(nPeople)
      val approachValue = 10000 * random.nextDouble()

      writer.println(s"person_$personIndex,$approachValue")
    }

    writer.flush()
    writer.close()
  }

  /**
    * A function which generates random text in lorem-ipsum fashion, in chunks, as normal "paragraphs".
    * Supports an optional senders argument to attach every paragraph to a sender/broker ID for key-value crunching.
    * If the number of senders is positive, then each line will have a prefix "(senderID) // ", where senderID is randomly picked between 1 and nSenders.
    *
    * @param dstPath the file path where you want to write the text
    * @param nWords the number of words
    * @param nParagraphs the number of lines
    * @param nSenders (default 0) the number of unique senders
    */
  def generateText(dstPath: String, nWords: Int, nParagraphs: Int, nSenders: Int = 0): Unit = {
    assert(nSenders >= 0)
    assert(nWords > 1)
    assert(nParagraphs > 0)

    val words = Source.fromFile("src/main/resources/data/lipsum/words.txt").getLines().toSeq
    val numWords = words.length

    def pickRandomWord(isLast: Boolean = false) =
      words(random.nextInt(numWords)) + (if (!isLast && random.nextInt() % 5 == 0) "," else "")

    val lowSentenceLimit = 2
    val highSentenceLimit = 14
    val avgParLength = nWords / nParagraphs
    val lowParLimit = avgParLength / 2
    val highParLimit = avgParLength * 3 / 2
    val writer = new PrintWriter(new FileWriter(new File(dstPath)))

    @tailrec
    def generateLipsumRec(nWords: Int, nParagraphs: Int, nWordsInParagraph: Int, attachSender: Boolean = false): Unit = {
      val sentenceLength =
        if (nWordsInParagraph < highSentenceLimit) nWordsInParagraph
        else randomIntBetween(lowSentenceLimit, highSentenceLimit)

      val ending = if (sentenceLength == nWordsInParagraph) "." else ". "
      val sentence = ((1 until sentenceLength).map(_ => pickRandomWord()) :+ pickRandomWord(true)).mkString(" ") + ending

      if (attachSender) {
        val sender = (randomInt(nSenders) + 1) + " // "
        writer.print(sender)
      }
      writer.print(sentence.capitalize)

      val nWordsInParagraphLeft = nWordsInParagraph - sentenceLength
      val nParagraphsLeft = nParagraphs - 1

      if (nWordsInParagraphLeft == 0) {
        if (nParagraphsLeft > 0) {
          val nWordsLeft = nWords - sentenceLength
          val nextParLength =
            if (nParagraphsLeft == 1) nWordsLeft
            else randomIntBetween(lowParLimit, highParLimit)

          writer.print("\n")
          generateLipsumRec(nWords - sentenceLength, nParagraphsLeft, nextParLength, nSenders > 0)
        }
      } else {
        generateLipsumRec(nWords - sentenceLength, nParagraphs, nWordsInParagraphLeft)
      }
    }

    val nWordsInFirstParagraph = if (nWords < highParLimit) nWords else randomIntBetween(lowParLimit, highParLimit)

    generateLipsumRec(nWords, nParagraphs, nWordsInFirstParagraph, nSenders > 0)
    writer.flush()
    writer.close()
  }

  def main(args: Array[String]): Unit = {
    generateExamData("src/main/resources/data/studentgen", 100000, 5)
  }
}