import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.PairRDDFunctions
import annotation.tailrec
import scala.reflect.ClassTag
import scala.util.Random

/** A raw posting, either a question or an answer */
case class Posting(postingType: Int, id: Int, parentId: Option[Int], score: Int, tags: Option[String]) extends Serializable

/** The main class */
object Assignment2 extends Assignment2 {

  @transient lazy val conf: SparkConf = new SparkConf().setMaster("local").setAppName("Assignment2")
  @transient lazy val sc: SparkContext = new SparkContext(conf)
  //sc.setLogLevel("WARN")

  /** Main function */
  def main(args: Array[String]): Unit = {
    val lines   = sc.textFile("QA_data.csv")
    val raw     = rawPostings(lines)
    val grouped = groupedPostings(raw)
    val scored  = scoredPostings(grouped)
    val vectors = vectorPostings(scored)
    val results   = kmeans(vectors)
//    val results = clusterResults(means, vectors)
    printResults(results)
  }
}

/** The parsing and kmeans methods */
class Assignment2 extends Serializable {

  /** Languages */
  val Domains =
    List(
      "Machine-Learning", "Compute-Science", "Algorithm", "Big-Data", "Data-Analysis", "Security", "Silicon Valley", "Computer-Systems",
      "Deep-learning", "Internet-Service-Providers", "Programming-Language", "Cloud-services", "Software-Engineering", "Embedded-System", "Architecture")

  /** K-means parameter: How "far apart" languages should be for the kmeans algorithm? */
  def DomainSpread = 50000
  assert(DomainSpread > 0)

  /** K-means parameter: Number of clusters */
  def kmeansKernels = 45

  /** K-means parameter: Convergence criteria, if distance < kmeansEta, stop*/
  def kmeansEta: Double = 20.0D

  /** K-means parameter: Maximum iterations */
  def kmeansMaxIterations = 120

  //
  // Parsing utilities:
  //

  /** Load postings from the given file */
  def rawPostings(lines: RDD[String]): RDD[Posting] =
    lines.map(line => {
      val arr = line.split(",")
      Posting(postingType =    arr(0).toInt,
        id =             arr(1).toInt,
//        acceptedAnswer = if (arr(2) == "") None else Some(arr(2).toInt),
        parentId =       if (arr(2) == "") None else Some(arr(2).toInt),
        score =          arr(3).toInt,
        tags =           if (arr.length >= 5) Some(arr(4).intern()) else None)
    })

  /** Group the questions and answers together */
  /** please keep the function name but you can modify the parameters for this function */
  def groupedPostings(raw: RDD[Posting]): RDD[(Int, Iterable[Posting])]  = {
    // Filter the questions and answers separately
    // Prepare them for a join operation by extracting the QID value in the first element of a tuple
    raw.map(x => (x.parentId.getOrElse(x.id), x)).groupByKey()
  }

  /** Compute the maximum score for each posting */
  /** Return the question ID, highest score among answers, and the domain **/
  /** please keep the function name but you can modify the parameters for this function */
  def scoredPostings(grouped: RDD[(Int, Iterable[Posting])]): RDD[(Int, Int, String)] = {
    grouped.map(x => (x._1, x._2.filter(z => z.parentId.isDefined).maxBy(y => y.score).score,
      x._2.filter(z => z.parentId.isEmpty).head.tags.getOrElse("")))
  }

  /** Compute the vectors for the kmeans */
  /** please keep the function name but you can modify the parameters for this function */
  def vectorPostings(scored: RDD[(Int, Int, String)]): RDD[(Int, Int)] = {
    scored.map(x => (Domains.indexOf(x._3) * DomainSpread, x._2))
  }

  //
  //  Kmeans method:
  //

  /** Main kmeans computation */
  /** please keep the function name but you can modify the parameters for this function */
  @tailrec final def kmeans(vectors: RDD[(Int, Int)]): RDD[((Int, Int), Iterable[(Int, Int)])] = {
    var iter: Int = 0
    var distance: Double = Double.PositiveInfinity
    // Initialise kmeansKernels random points as centroids
    var centroids: Array[(Int, Int)] = Random.shuffle(vectors.collect().toList).take(kmeansKernels)
    var new_centroids: Array[(Int, Int)] = centroids.clone()
    // Initialise results RDD
    var results: RDD[((Int, Int), Iterable[(Int, Int)])]
    // Keep computing centroids and assigning points until convergence
    while (!converged(distance) && iter < kmeansMaxIterations) {
      iter += 1;
      // Initialise centroids to new_centroids
      centroids = new_centroids
      // Assign each data point to the closest centroid
      results = vectors.map(x => (findClosest(x, centroids), x)).groupByKey()
      // Recompute centroids using current cluster memberships
      new_centroids = ???
        // ??? new_centroids = results.aggregateByKey((0,0), lambda a,b:(a[0]+b,a[1]+1), lambda a,b:(a[0]+b[0],a[1]+b[1]))
      // Set convergence criterion parameter
      distance = euclideanDistance(centroids, new_centroids)
    }
    results
  }

  //
  //  Kmeans utilities (Just some cases, you can implement your own utilities.)
  //

  /** Decide whether the kmeans clustering converged */
  def converged(distance: Double) = distance < kmeansEta

  /** Return the euclidean distance between two points */
  def euclideanDistance(v1: (Int, Int), v2: (Int, Int)): Double = {
    val part1 = (v1._1 - v2._1).toDouble * (v1._1 - v2._1)
    val part2 = (v1._2 - v2._2).toDouble * (v1._2 - v2._2)
    part1 + part2
  }

  /** Return the sum of euclidean distances between two sets of points, each set having same number of points */
  def euclideanDistance(a1: Array[(Int, Int)], a2: Array[(Int, Int)]): Double = {
    assert(a1.length == a2.length)
    var sum = 0d
    var idx = 0
    while(idx < a1.length) {
      sum += euclideanDistance(a1(idx), a2(idx))
      idx += 1
    }
    sum
  }

  /** Return the closest point */
  def findClosest(p: (Int, Int), centers: Array[(Int, Int)]): Int = {
    var bestIndex = 0
    var closest = Double.PositiveInfinity
    for (i <- centers.indices) {
      val tempDist = euclideanDistance(p, centers(i))
      if (tempDist < closest) {
        closest = tempDist
        bestIndex = i
      }
    }
    bestIndex
  }

  /** Average the vectors */
  def averageVectors(ps: Iterable[(Int, Int)]): (Int, Int) = {
    val iter = ps.iterator
    var count = 0
    var comp1: Long = 0
    var comp2: Long = 0
    while (iter.hasNext) {
      val item = iter.next
      comp1 += item._1
      comp2 += item._2
      count += 1
    }
    ((comp1 / count).toInt, (comp2 / count).toInt)
  }

  def computeMedian(a: Iterable[(Int, Int)]) = {
    val s = a.map(x => x._2).toArray
    val length = s.length
    val (lower, upper) = s.sortWith(_<_).splitAt(length / 2)
    if (length % 2 == 0) (lower.last + upper.head) / 2 else upper.head
  }

  //  Displaying results
  def printResults(results: RDD[((Int, Int), Iterable[(Int, Int)])]): Unit = {
    //Todo
  }
}
