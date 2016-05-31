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
package software.uncharted.graphing.clustering.unithread



import java.io._

import org.scalatest.FunSuite
import software.uncharted.graphing.analytics.{WrappingTileAggregator, WrappingClusterAggregator, CustomGraphAnalytic}
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.analytic.numeric.{MeanAggregator, MaxAggregator, SumAggregator}

import scala.language.implicitConversions



class CommunityModificationsTestSuite extends FunSuite {
  test("Test node degree modification") {
    // 3 3-node stars, with a central node (node 0) connected to everything
    val g: Graph = new Graph(
      Array(2, 4, 6, 10, 12, 14, 16, 20, 22, 24, 26, 30, 42),
      Array(
        3, 12,
        3, 12,
        3, 12,
        0, 1, 2, 12,
        7, 12,
        7, 12,
        7, 12,
        4, 5, 6, 12,
        11, 12,
        11, 12,
        11, 12,
        8, 9, 10, 12,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
      ),
      (0 to 12).map(n => new NodeInfo(n.toLong, 1, None, Array(), Array())).toArray
    )
    val c = new Community(g, -1, 0.15, new NodeDegreeAlgorithm(5))
    c.one_level(false)
    assert(3 === c.n2c(0))
    assert(3 === c.n2c(1))
    assert(3 === c.n2c(2))
    assert(3 === c.n2c(3))
    assert(7 === c.n2c(4))
    assert(7 === c.n2c(5))
    assert(7 === c.n2c(6))
    assert(7 === c.n2c(7))
    assert(11 === c.n2c(8))
    assert(11 === c.n2c(9))
    assert(11 === c.n2c(10))
    assert(11 === c.n2c(11))
    assert(12 === c.n2c(12))
  }

  test("Test retention of analytic data") {
    // Helper functions to help manage streams
    def openWriteStream = {
      val baos = new ByteArrayOutputStream()
      val dos = new DataOutputStream(baos)
      (baos, dos)
    }
    def closeWriteStream (streamPair: (ByteArrayOutputStream, DataOutputStream)): Array[Byte] = {
      streamPair._2.flush()
      streamPair._2.close()
      streamPair._1.flush()
      streamPair._1.close()
      streamPair._1.toByteArray
    }
    def openReadStream (data: Array[Byte]) = {
      val bais = new ByteArrayInputStream(data)
      val dis = new DataInputStream(bais)
      (bais, dis)
    }

    // Some analytics to use
    val analytics = Array[CustomGraphAnalytic[_]](
      new TestGraphAnalytic("abc", 2, SumAggregator),
      new TestGraphAnalytic("def", 3, MaxAggregator),
      new TestGraphAnalytic("ghi", 4, MeanAggregator)
    )

    // Create some data
    // Contains some edge analytic data for now; may remove later.
    val ge: GraphEdges = new GraphEdges(Array(
      Seq(
        (1, 1.0f, Seq("1.5", "1.6", "1.7")),
        (2, 1.0f, Seq("2.5", "2.6", "2.7"))
      ),
      Seq(
        (0, 1.0f, Seq("1.5", "1.6", "1.7")),
        (2, 1.0f, Seq("3.5", "3.6", "3.7"))
      ),
      Seq(
        (0, 1.0f, Seq("2.5", "2.6", "2.7")),
        (1, 1.0f, Seq("3.5", "3.6", "3.7"))
      )
    ))
    val metadata =
      """
        |0 zero 0.5 0.6 0.7
        |1 one 1.5 1.6 1.7
        |2 two 2.5 2.6 2.7
      """.stripMargin
    ge.readMetadata(
      new BufferedReader(new InputStreamReader(new ByteArrayInputStream(metadata.getBytes))),
      None, " ", 0, 1, analytics
    )

    // Write out our gGraphEdges
    val edgeOStreamPair = openWriteStream
    val weightOStreamPair = openWriteStream
    val metadataOStreamPair = openWriteStream
    ge.display_binary(edgeOStreamPair._2, Some(weightOStreamPair._2), Some(metadataOStreamPair._2))

    val edgeIStreamPair = openReadStream(closeWriteStream(edgeOStreamPair))
    val weightIStreamPair = openReadStream(closeWriteStream(weightOStreamPair))
    val metadataIStreamPair = openReadStream(closeWriteStream(metadataOStreamPair))

    // Read it back in as a full graph
    val g = Graph(edgeIStreamPair._2, Some(weightIStreamPair._2), Some(metadataIStreamPair._2), analytics)

    // Make sure metadata is correct
    assert(3 === g.nb_nodes)
    assert(0.5 == g.nodeInfo(0).analyticData(0))
    assert(0.6 == g.nodeInfo(0).analyticData(1))
    assert((1, 0.7) == g.nodeInfo(0).analyticData(2))
    assert(1.5 == g.nodeInfo(1).analyticData(0))
    assert(1.6 == g.nodeInfo(1).analyticData(1))
    assert((1, 1.7) == g.nodeInfo(1).analyticData(2))
    assert(2.5 == g.nodeInfo(2).analyticData(0))
    assert(2.6 == g.nodeInfo(2).analyticData(1))
    assert((1, 2.7) == g.nodeInfo(2).analyticData(2))

    // Cluster that graph
    val c= new Community(g, -1, 0.15)
    c.one_level()
    val g1 = c.partition2graph_binary

    // Since the graph was fully connected, it should reduce to a single node.
    // Check that it did so, and that analytics aggregated correctly.
    assert(1 === g1.nb_nodes)
    assert(4.5 === g1.nodeInfo(0).analyticData(0))
    assert(2.6 === g1.nodeInfo(0).analyticData(1))
    assert((3, 5.1) === g1.nodeInfo(0).analyticData(2))


    // Check community clustering output for analytics
    def getNodes (c: Community): Seq[String] = {
      val baos = new ByteArrayOutputStream()
      val outStream = new PrintStream(baos)
      c.display_partition(0, outStream, None)
      outStream.flush()
      outStream.close()
      baos.flush()
      baos.close()
      baos.toString.split("\n").filter(_.startsWith("node"))
    }
    // raw level
    val clusterOutput0 = getNodes(c)
    assert(3 === clusterOutput0.length)
    def checkOutput (expected: Seq[Either[String, Seq[String]]], actual: String, delimiter: String = "\t"): Unit = {
      println(s"Full actual string: $actual")
      actual.split(delimiter).zipAll(expected, "", Left("")).foreach { case (a, es) =>
        println(s"expected: $es, actual: $a")
        es match {
          case Left(s) => assert(s === a)
          case Right(ss) => assert(ss.contains(a))
        }
      }
    }
    implicit def stringToLeftString        (s: String):      Either[String, Seq[String]] = Left(s)
    implicit def stringSeqToRightStringSeq (s: Seq[String]): Either[String, Seq[String]] = Right(s)
    checkOutput(Seq("node", "0", Seq("0", "1", "2"), "1", "2", "zero", "0.5", "0.6", "0.7"), clusterOutput0(0))
    checkOutput(Seq("node", "1", Seq("0", "1", "2"), "1", "2", "one", "1.5", "1.6", "1.7"), clusterOutput0(1))
    checkOutput(Seq("node", "2", Seq("0", "1", "2"), "1", "2", "two", "2.5", "2.6", "2.7"), clusterOutput0(2))

    // next level
    val c1 = new Community(g1, -1, 0.15)
    c1.one_level()
    val clusterOutput1 = getNodes(c1)
    assert(1 === clusterOutput1.length)
    checkOutput(
      Seq("node", Seq("0", "1", "2"), Seq("0", "1", "2"), "3", "6", Seq("zero", "one", "two"), "4.5", "2.6", "1.7"),
      clusterOutput1(0)
    )
  }
}

class TestGraphAnalytic[T] (_name: String, _column: Int, baseAggregator: Aggregator[Double, T, Double]) extends CustomGraphAnalytic[T] {
  override val name: String = _name
  override val column: Int = _column
  override val aggregator: Aggregator[String, T, String] =
    new  WrappingClusterAggregator(baseAggregator, (s: String) => s.toDouble, (d: Double) => d.toString)
  override def min(left: String, right: String): String = (left.toDouble min right.toDouble).toString
  override def max(left: String, right: String): String = (left.toDouble max right.toDouble).toString
}
