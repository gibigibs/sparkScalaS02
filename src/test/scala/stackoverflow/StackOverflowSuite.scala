package stackoverflow

import org.apache.spark.rdd.RDD
import org.junit.runner.RunWith
import org.scalatest.junit.JUnitRunner
import org.scalatest.{BeforeAndAfterAll, FunSuite}
import org.scalatest.Matchers._

@RunWith(classOf[JUnitRunner])
class StackOverflowSuite extends FunSuite with BeforeAndAfterAll {


  lazy val testObject = new StackOverflow {
    override val langs =
      List(
        "JavaScript", "Java", "PHP", "Python", "C#", "C++", "Ruby", "CSS",
        "Objective-C", "Perl", "Scala", "Haskell", "MATLAB", "Clojure", "Groovy")

    override def langSpread = 50000

    override def kmeansKernels = 45

    override def kmeansEta: Double = 20.0D

    override def kmeansMaxIterations = 120
  }

  test("testObject can be instantiated") {
    val instantiatable = try {
      testObject
      true
    } catch {
      case _: Throwable => false
    }
    assert(instantiatable, "Can't instantiate a StackOverflow object")
  }

  test("'kmeans' should move the means") {

    var means: Array[(Int, Int)] = Array((10, 10), (100, 100), (200, 200))

    val vectors: RDD[(Int, Int)] = StackOverflow.sc.parallelize(List(

      (10, 10), (100, 100), (200, 200),

      (110, 110), (120, 120),

      (240, 240), (280, 280)))

    var newMeans = StackOverflow.kmeans(means, vectors, debug = true)

    assert(newMeans(1)._1 == 110, "Mean '1' should move")

    assert(newMeans(2)._1 == 240, "Mean '2' should move")

  }

  test("median scores for an odd number of element should be the middle one for the sorted set") {
    val scores = List(21, 6854, 1, 874, 1064, 2, 10, 22, 10, 2, 10, 10, 22)
    val median = StackOverflow.medianScores(scores)
    println(s"${scores.sorted}")
    median shouldEqual 10

  }

  test("median scores for an even number of element should be the avg between the two middle ones of the sorted set") {
    val scores = List(21, 6854, 1, 874, 1064, 2, 10, 22, 10, 2, 10, 10)
    val median = StackOverflow.medianScores(scores)
    println(s"${scores.sorted}")
    median shouldEqual 10

  }

}
