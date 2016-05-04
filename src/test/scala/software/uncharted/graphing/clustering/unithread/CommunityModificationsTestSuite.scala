package software.uncharted.graphing.clustering.unithread

import java.io._

import org.scalatest.FunSuite
import software.uncharted.graphing.analytics.CustomGraphAnalytic
import software.uncharted.graphing.salt.{WrappingTileAggregator, WrappingClusterAggregator}
import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.analytic.numeric.{MeanAggregator, MaxAggregator, SumAggregator, SumAggregatorSpec}

import scala.util.parsing.json.JSONObject

/**
  * Created by nkronenfeld on 2016-02-01.
  */
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
      (0 to 12).map(n => new NodeInfo(n.toLong, 1, None, Array(), Seq())).toArray
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
    val analytics = Seq[CustomGraphAnalytic[_, _]](
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
    assert(0.5 == g.nodeInfo(0).analyticData(0))
    assert(0.6 == g.nodeInfo(0).analyticData(1))
    assert((1, 0.7) == g.nodeInfo(0).analyticData(2))
    assert(1.5 == g.nodeInfo(1).analyticData(0))
    assert(1.6 == g.nodeInfo(1).analyticData(1))
    assert((1, 1.7) == g.nodeInfo(1).analyticData(2))
    assert(2.5 == g.nodeInfo(2).analyticData(0))
    assert(2.6 == g.nodeInfo(2).analyticData(1))
    assert((1, 2.7) == g.nodeInfo(2).analyticData(2))
  }
}

class TestGraphAnalytic[T] (_name: String, _column: Int, aggregator: Aggregator[Double, T, Double]) extends CustomGraphAnalytic[T, T] {
  override val name: String = _name
  override val column: Int = _column
  override val clusterAggregator: Aggregator[String, T, String] =
    new  WrappingClusterAggregator(aggregator, (s: String) => s.toDouble, (d: Double) => d.toString)
  override val tileAggregator: Aggregator[String, T, JSONObject] =
    new WrappingTileAggregator(aggregator, (s: String) => s.toDouble, (value: Double) => new JSONObject(Map("value" -> value)))
}