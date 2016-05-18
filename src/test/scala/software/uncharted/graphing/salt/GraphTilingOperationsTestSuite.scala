package software.uncharted.graphing.salt


import org.apache.log4j.{Level, Logger}
import org.apache.spark.SharedSparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types._
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

  test("optional operation") {
    val transform1: Option[Int => Int] = Some((n: Int) => n*n)
    assert(16 === optional(transform1)(4))

    val transform2: Option[Int => Int] = None
    assert(4 === optional(transform2)(4))
  }

  test("map as an operation") {
    val data = sc.parallelize(1 to 4)
    assert(List(1, 4, 9, 16) === map((n: Int) => n*n)(data).collect.toList)
  }

  test("toDataFrame from case class") {
    val data = sc.parallelize(Seq(TestRow(1, 1.0, "one"), TestRow(2, 2.0, "two"), TestRow(3, 3.0, "three"), TestRow(4, 4.0, "four")))
    val converted = toDataFrame(sqlc)(data)
    assert(List(1, 2, 3, 4) === converted.select("a").rdd.map(_(0).asInstanceOf[Int]).collect.toList)
    assert(List(1.0, 2.0, 3.0, 4.0) === converted.select("b").rdd.map(_(0).asInstanceOf[Double]).collect.toList)
    assert(List("one", "two", "three", "four") === converted.select("c").rdd.map(_(0).asInstanceOf[String]).collect.toList)
  }

  test("toDataFrame from CSV with auto-schema") {
    val data = sc.parallelize(Seq("1,1.0,one", "2,2.0,two", "3,3.0,three", "4,4.0,four"))
    val converted = toDataFrame(sqlc, Map("inferSchema" -> "true"), None)(data)
    assert(List(1, 2, 3, 4) === converted.select("C0").rdd.map(_(0).asInstanceOf[Int]).collect.toList)
    assert(List(1.0, 2.0, 3.0, 4.0) === converted.select("C1").rdd.map(_(0).asInstanceOf[Double]).collect.toList)
    assert(List("one", "two", "three", "four") === converted.select("C2").rdd.map(_(0).asInstanceOf[String]).collect.toList)
  }

  test("toDataFrame from CSV with explicit schema") {
    val data = sc.parallelize(Seq("1,1.0,one", "2,2.0,two", "3,3.0,three", "4,4.0,four"))
    val schema = StructType(Seq(StructField("a", IntegerType), StructField("b", DoubleType), StructField("c", StringType)))
    val converted = toDataFrame(sqlc, Map[String, String](), Some(schema))(data)
    assert(List(1, 2, 3, 4) === converted.select("a").rdd.map(_(0).asInstanceOf[Int]).collect.toList)
    assert(List(1.0, 2.0, 3.0, 4.0) === converted.select("b").rdd.map(_(0).asInstanceOf[Double]).collect.toList)
    assert(List("one", "two", "three", "four") === converted.select("c").rdd.map(_(0).asInstanceOf[String]).collect.toList)
  }

  test("getBounds") {
    val data = toDataFrame(sqlc)(sc.parallelize(Seq(
      Coordinates(0.0, 0.0, 0.0, 0.0),
      Coordinates(1.0, 4.0, 3.0, 5.0),
      Coordinates(2.0, 2.0, 1.0, 0.0),
      Coordinates(-1.0, -2.0, -3.0, -4.0)
    )))

    assert(List((-1.0, 2.0), (-2.0, 4.0)) === getBounds("w", "x")(data).toList)
    assert(List((-3.0, 3.0), (-4.0, 5.0)) === getBounds("y", "z")(data).toList)
    assert(List((-2.0, 4.0), (-3.0, 3.0)) === getBounds("x", "y")(data).toList)
  }

  test("cartesian tiling without autobounds") {
    val data = toDataFrame(sqlc)(sc.parallelize(Seq(
      Coordinates(0.0,-1.0, 0.0, 0.0),
      Coordinates(0.0, 0.0, -1.0, 0.0),
      Coordinates(0.0, 0.0, 0.0, 0.0),
      Coordinates(0.0, 0.5, 0.5, 0.0),
      Coordinates(0.0, 1.5, 3.5, 0.0),
      Coordinates(0.0, 2.5, 2.5, 0.0),
      Coordinates(0.0, 3.5, 1.5, 0.0),
      Coordinates(0.0, 4.0, 0.0, 0.0),
      Coordinates(0.0, 0.0, 4.0, 0.0)
    )))
    val tiles = cartesianTiling("x", "y", "count", Seq(0), Some((0.0, 0.0, 4.0, 4.0)), 4)(addOnesColumn("count")(data)).collect

    assert(List(0.0, 1.0, 0.0, 0.0,  0.0, 0.0, 1.0, 0.0,  0.0, 0.0, 0.0, 1.0,  2.0, 0.0, 0.0, 0.0) === tiles(0).bins.seq.toList)
  }

  test("cartesian tiling with autobounds") {
    val data = toDataFrame(sqlc)(sc.parallelize(Seq(
      Coordinates(0.0, 0.0, 0.0, 0.0),
      Coordinates(0.0, 0.5, 0.5, 0.0),
      Coordinates(0.0, 1.5, 3.5, 0.0),
      Coordinates(0.0, 2.5, 2.5, 0.0),
      Coordinates(0.0, 3.5, 1.5, 0.0),
      Coordinates(0.0, 4.0, 4.0, 0.0)
    )))
    val tiles = cartesianTiling("x", "y", "count", Seq(0), None, 4)(addOnesColumn("count")(data)).collect

    assert(List(0.0, 1.0, 0.0, 1.0,  0.0, 0.0, 1.0, 0.0,  0.0, 0.0, 0.0, 1.0,  2.0, 0.0, 0.0, 0.0) === tiles(0).bins.seq.toList)
  }
}

case class TestRow (a: Int, b: Double, c: String)
case class Coordinates (w: Double, x: Double, y: Double, z: Double)
