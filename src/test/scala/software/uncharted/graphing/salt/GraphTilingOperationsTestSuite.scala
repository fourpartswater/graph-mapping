/**
  * Copyright (c) 2014-2016 Uncharted Software Inc. All rights reserved.
  *
  * Property of Uncharted(tm), formerly Oculus Info Inc.
  * http://uncharted.software/
  *
  * This software is the confidential and proprietary information of
  * Uncharted Software Inc. ("Confidential Information"). You shall not
  * disclose such Confidential Information and shall use it only in
  * accordance with the terms of the license agreement you entered into
  * with Uncharted Software Inc.
  */
package software.uncharted.graphing.salt


import java.nio.{ByteOrder, DoubleBuffer, ByteBuffer}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SharedSparkContext
import org.apache.spark.sql.types._
import org.scalatest.{Tag, FunSuite}
import software.uncharted.graphing.utilities.S3Client
import software.uncharted.salt.core.util.SparseArray
import software.uncharted.xdata.ops.{numeric => XDataNum}


class GraphTilingOperationsTestSuite extends FunSuite with SharedSparkContext {
  Logger.getRootLogger.setLevel(Level.WARN)
  import GraphTilingOperations._
  import software.uncharted.xdata.ops.{io => XDataIO}

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
    val tiles = cartesianTiling("x", "y", "count", Seq(0), Some((0.0, 0.0, 4.0, 4.0)), 4)(XDataNum.addConstantColumn("count", 1)(data)).collect

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
    val tiles = cartesianTiling("x", "y", "count", Seq(0), None, 4)(XDataNum.addConstantColumn("count", 1)(data)).collect

    assert(List(0.0, 1.0, 0.0, 1.0,  0.0, 0.0, 1.0, 0.0,  0.0, 0.0, 0.0, 1.0,  2.0, 0.0, 0.0, 0.0) === tiles(0).bins.seq.toList)
  }

  object S3Test extends Tag("s3.test")

  test("save s3 sparse data", S3Test) {
    val data = toDataFrame(sqlc)(sc.parallelize(Seq(
      Coordinates(0.0, 0.0, 0.0, 0.0),
      Coordinates(0.0, 0.5, 0.5, 0.0),
      Coordinates(0.0, 1.5, 3.5, 0.0),
      Coordinates(0.0, 2.5, 2.5, 0.0),
      Coordinates(0.0, 3.5, 1.5, 0.0),
      Coordinates(0.0, 4.0, 4.0, 0.0)
    )))

    val tiles = cartesianTiling("x", "y", "count", Seq(0), None, 4)(XDataNum.addConstantColumn("count", 1)(data))
    val encodedTiles = serializeTilesSparse(tiles)
    val awsCredentials = (sys.env("AWS_ACCESS_KEY"), sys.env("AWS_SECRET_KEY"))
    XDataIO.writeToS3(awsCredentials._1, awsCredentials._2, "uncharted-s3-client-test", "sparse_test_layer")(encodedTiles)
    val client = S3Client(awsCredentials._1, awsCredentials._2)

    client.download("uncharted-s3-client-test", "sparse_test_layer/0/0/0.bin").map { bytes =>
      val byteBuffer = ByteBuffer.wrap(bytes.toArray).order(ByteOrder.LITTLE_ENDIAN)
      assertResult(5)(byteBuffer.getInt)
      assertResult(0.0)(byteBuffer.getDouble)
      val tileData = for (i <- 0 until (byteBuffer.remaining / (8 + 4))) yield (byteBuffer.getInt, byteBuffer.getDouble)
      val sparse = tileData.foldLeft(new SparseArray(16, 0d)) { (curr, elem) => curr.update(elem._1, elem._2); curr }
      assert(List(0.0, 1.0, 0.0, 1.0,  0.0, 0.0, 1.0, 0.0,  0.0, 0.0, 0.0, 1.0,  2.0, 0.0, 0.0, 0.0) === sparse.seq)
      client.delete("uncharted-s3-client-test", "sparse_test_layer/0/0/0.bin")
    }.getOrElse {
      fail("Failed to fetch tile from S3 bucket")
    }
  }

  test("save s3 dense data", S3Test) {
    val data = toDataFrame(sqlc)(sc.parallelize(Seq(
      Coordinates(0.0, 0.0, 0.0, 0.0),
      Coordinates(0.0, 0.5, 0.5, 0.0),
      Coordinates(0.0, 1.5, 3.5, 0.0),
      Coordinates(0.0, 2.5, 2.5, 0.0),
      Coordinates(0.0, 3.5, 1.5, 0.0),
      Coordinates(0.0, 4.0, 4.0, 0.0)
    )))

    val tiles = cartesianTiling("x", "y", "count", Seq(0), None, 4)(XDataNum.addConstantColumn("count", 1)(data))
    val encodedTiles = serializeTilesSparse(tiles)
    val awsCredentials = (sys.env("AWS_ACCESS_KEY"), sys.env("AWS_SECRET_KEY"))
    XDataIO.writeToS3(awsCredentials._1, awsCredentials._2, "uncharted-s3-client-test", "dense_test_layer")(encodedTiles)
    val client = S3Client(awsCredentials._1, awsCredentials._2)

    client.download("uncharted-s3-client-test", "dense_test_layer/0/0/0.bin").map { bytes =>
      val doubleBuffer = DoubleBuffer.allocate(bytes.length / 8)
      doubleBuffer.put(ByteBuffer.wrap(bytes.toArray).order(ByteOrder.LITTLE_ENDIAN).asDoubleBuffer())
      val data = doubleBuffer.array
      assert(List(0.0, 1.0, 0.0, 1.0,  0.0, 0.0, 1.0, 0.0,  0.0, 0.0, 0.0, 1.0,  2.0, 0.0, 0.0, 0.0) === data)
      client.delete("uncharted-s3-client-test", "dense_test_layer/0/0/0.bin")
    }.getOrElse {
      fail("Failed to fetch tile from S3 bucket")
    }
  }
}

case class TestRow (a: Int, b: Double, c: String)
case class Coordinates (w: Double, x: Double, y: Double, z: Double)
