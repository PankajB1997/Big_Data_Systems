import org.apache.spark._
import org.apache.spark.rdd.RDD

object CommonWords {

  def populateStopwords(sc: SparkContext, stopwordsFile: String): Array[String] = {
    sc.textFile(stopwordsFile)
      .collect()
  }

  def wordCount(sc: SparkContext, stopwords: Array[String], inputFile: String): RDD[(String, Int)] = {
    // Load our input data.
    val input =  sc.textFile(inputFile)
    // Split up into words and remove stopwords
    val words = input
      .flatMap(line => line.split(" "))
      .map(word => word.replaceAll("[^a-zA-Z ]", "").toLowerCase().trim())
      .filter(word => !stopwords.contains(word) && word.trim().length() != 0)
    // Transform into word and count.
    words
      .map(word => (word, 1))
      .reduceByKey{case (x, y) => x + y}
  }

  def commonCount(context: SparkContext, counts1: RDD[(String, Int)], counts2: RDD[(String, Int)]): RDD[(Int, String)] = {
    counts1
      .join(counts2)
      .map(row => (row._1, (if (row._2._1 > row._2._2) row._2._2 else row._2._1)))
        .map(item => item.swap)
        .sortByKey(ascending = false)
  }

  def main(args: Array[String]) {
    val inputFile1 = args(0)
    val inputFile2 = args(1)
    val outputFile = args(2)
    val stopwordsFile = args(3)

    // Create a Scala Spark Context.
    val conf = new SparkConf()
    val sc = new SparkContext(conf)
    // val sc = SparkContext.getOrCreate()

    // Populate list of stopwords.
    val stopwords = populateStopwords(sc, stopwordsFile)
    // Run word count for each file
    val counts1 = wordCount(sc, stopwords, inputFile1)
    val counts2 = wordCount(sc, stopwords, inputFile2)
    // Run common words across the two sets of counts
    val results = commonCount(sc, counts1, counts2)
    // Save the common word count back out to a text file, causing evaluation
    results.map(x => x._1 + "\t" + x._2).saveAsTextFile(outputFile)
  }
}