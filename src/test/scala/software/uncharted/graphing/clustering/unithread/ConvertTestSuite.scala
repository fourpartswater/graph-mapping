package software.uncharted.graphing.clustering.unithread

import java.io.{ByteArrayInputStream, InputStreamReader, BufferedReader}

import org.scalatest.FunSuite

/**
  * Created by nkronenfeld on 2016-01-07.
  */
class ConvertTestSuite extends FunSuite {
  test("Test reading of edge files (no weights)") {
    val rawData =
      """edge primary 0 1
        |edge secondary 0 2
        |edge primary 1 3
        |edge secondary 1 4
        |edge primary 2 3
        |edge secondary 2 4""".stripMargin
    val reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(rawData.getBytes)))
    val edges = GraphEdges(reader, Some("edge"), "[ \t]+", 2, 3, None)
    assert(5 === edges.links.length)
    assert(edges.links(0).toList === List((1, 1.0f), (2, 1.0f)))
    assert(edges.links(1).toList === List((0, 1.0f), (3, 1.0f), (4, 1.0f)))
    assert(edges.links(2).toList === List((0, 1.0f), (3, 1.0f), (4, 1.0f)))
    assert(edges.links(3).toList === List((1, 1.0f), (2, 1.0f)))
    assert(edges.links(4).toList === List((1, 1.0f), (2, 1.0f)))
  }

  test("Test reading of edge files (with weights)") {
    val rawData =
      """edge primary   0 1 1 0.7
        |edge secondary 0 2 1 0.2
        |edge primary   1 3 1 0.6
        |edge secondary 1 4 1 0.3
        |edge primary   2 3 1 0.5
        |edge secondary 2 4 1 0.4""".stripMargin
    val reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(rawData.getBytes)))
    val edges = GraphEdges(reader, Some("edge"), "[ \t]+", 2, 3, Some(5))
    assert(5 === edges.links.length)
    assert(edges.links(0).toList === List((1, 0.7f), (2, 0.2f)))
    assert(edges.links(1).toList === List((0, 0.7f), (3, 0.6f), (4, 0.3f)))
    assert(edges.links(2).toList === List((0, 0.2f), (3, 0.5f), (4, 0.4f)))
    assert(edges.links(3).toList === List((1, 0.6f), (2, 0.5f)))
    assert(edges.links(4).toList === List((1, 0.3f), (2, 0.4f)))
  }

  test("Test adding metadata to edges") {
    val rawData =
      """edge primary 0 1
        |edge secondary 0 2
        |edge primary 1 3
        |edge secondary 1 4
        |edge primary 2 3
        |edge secondary 2 4
        |node 0 2 zero
        |node 1 3 one
        |node 2 3 two
        |node 3 4 three
        |node 4 2 four""".stripMargin
    val edgeReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(rawData.getBytes)))
    val edges = GraphEdges(edgeReader, Some("edge"), "[ \t]+", 2, 3, None)
    val nodeReader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(rawData.getBytes)))
    edges.readMetadata(nodeReader, Some("node"), "[ \t]+", 1, 3)
    assert(edges.metaData.get.length === 5)
    assert("zero" === edges.metaData.get.apply(0))
    assert("one" === edges.metaData.get.apply(1))
    assert("two" === edges.metaData.get.apply(2))
    assert("three" === edges.metaData.get.apply(3))
    assert("four" === edges.metaData.get.apply(4))
  }
}
