package software.uncharted.graphing.salt


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SharedSparkContext
import org.scalatest.FunSuite



/**
  * Created by nkronenfeld on 2/18/2016.
  */
class GraphTilingOperationsTestSuite extends FunSuite with SharedSparkContext {
  Logger.getRootLogger.setLevel(Level.WARN)
  import GraphTilingOperations._

  test("filter operation") {
    val data = sc.parallelize(1 to 20)
    assert(List(3, 8, 13, 18) === filter[Int](n => (3 == (n % 5)))(data).collect.toList)
  }

  test("regex filter operation") {
    val data = sc.parallelize(Seq("abc def ghi", "abc d e f ghi", "the def quick", "def ghi", "abc def"))
    assert(List("abc def ghi", "the def quick", "def ghi") === regexFilter(".*def.+")(data).collect.toList)
    assert(List("abc d e f ghi", "def ghi") === regexFilter(".+def.*", true)(data).collect.toList)
  }
}
