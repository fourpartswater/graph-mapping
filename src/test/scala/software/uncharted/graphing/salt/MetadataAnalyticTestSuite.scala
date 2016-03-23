package software.uncharted.graphing.salt



import scala.collection.mutable.{Buffer => MutableBuffer}
import org.scalatest.FunSuite



class MetadataAnalyticTestSuite extends FunSuite {
  test("Standard metadata analytic toString") {
    val expected =
      """{
        |  "numCommunities": 4,
        |  "communities": [{
        |  "heirLevel": 3,
        |  "id": 2,
        |  "coords": [2.1, 1.3],
        |  "radius": 4.4,
        |  "degree": 2,
        |  "numNodes": 3,
        |  "metadata": "\"abc\\def\"",
        |  "isPrimaryNode": true,
        |  "parentID": 2,
        |  "parentCoords": [2.1, 1.4],
        |  "parentRadius": 4.5,
        |  "statsList": [1.1,1.2,1.3,1.4],
        |  "interEdges": [{"dstId": 3, "dstCoords": [4.1, 4.2], "weight": 4},{"dstId": 4, "dstCoords": [5.1, 5.3], "weight": 6}],
        |  "intraEdges": []
        |},{
        |  "heirLevel": 4,
        |  "id": 3,
        |  "coords": [1.2, 3.1],
        |  "radius": 4.5,
        |  "degree": 1,
        |  "numNodes": 2,
        |  "metadata": "aa \"abc\\def\" ff",
        |  "isPrimaryNode": false,
        |  "parentID": 2,
        |  "parentCoords": [2.2, 1.5],
        |  "parentRadius": 4.6,
        |  "statsList": [],
        |  "interEdges": [],
        |  "intraEdges": [{"dstId": 2, "dstCoords": [2.1, 1.3], "weight": 5}]
        |}]
        |}""".stripMargin
    val c1 = new GraphCommunity(
      3, 2, (2.1, 1.3), 4.4, 2, 3, "\"abc\\def\"", true, 2, (2.1, 1.4), 4.5,
      Some(MutableBuffer(1.1, 1.2, 1.3, 1.4)),
      Some(MutableBuffer(new GraphEdge(3, (4.1, 4.2), 4), new GraphEdge(4, (5.1, 5.3), 6))),
      None
    )
    val c2 = new GraphCommunity(
      4, 3, (1.2, 3.1), 4.5, 1, 2, "aa \"abc\\def\" ff", false, 2, (2.2, 1.5), 4.6,
      None,
      Some(MutableBuffer[GraphEdge]()),
      Some(MutableBuffer(GraphEdge(2, (2.1, 1.3), 5)))
    )
    val r = new GraphRecord(Some(MutableBuffer(c1, c2)), 4)
    val actual = r.toString

    assert(expected === actual)
  }

  test("Standard metadata analytic fromString") {
    val text =
      """{
        |  "numCommunities": 4,
        |  "communities": [{
        |    "heirLevel": 3,
        |    "id": 2,
        |    "coords": [2.1, 1.3],
        |    "radius": 4.4,
        |    "degree": 2,
        |    "numNodes": 3,
        |    "metadata": "\"abc\\def\"",
        |    "isPrimaryNode": true,
        |    "parentID": 2,
        |    "parentCoords": [2.1, 1.4],
        |    "parentRadius": 4.5,
        |    "statsList": [1.1,1.2,1.3,1.4],
        |    "interEdges": [
        |      {"dstId": 3, "dstCoords": [4.1, 4.2], "weight": 4},
        |      {
        |        "dstId": 4,
        |        "dstCoords": [5.1, 5.3],
        |        "weight": 6
        |      }
        |    ],
        |    "intraEdges": []
        |  },{
        |    "heirLevel": 4,
        |    "id": 3,
        |    "coords": [1.2, 3.1],
        |    "radius": 4.5,
        |    "degree": 1,
        |    "numNodes": 2,
        |    "metadata": "aa \"abc\\def\" ff",
        |    "isPrimaryNode": false,
        |    "parentID": 2,
        |    "parentCoords": [2.2, 1.5],
        |    "parentRadius": 4.6,
        |    "statsList": [],
        |    "interEdges": [],
        |    "intraEdges": [{"dstId": 2, "dstCoords": [2.1, 1.3], "weight": 5}]
        |  }]
        |}""".stripMargin
    val r = GraphRecord.fromString(text)

    val c1 = new GraphCommunity(
      3, 2, (2.1, 1.3), 4.4, 2, 3, "\"abc\\def\"", true, 2, (2.1, 1.4), 4.5,
      Some(MutableBuffer(1.1, 1.2, 1.3, 1.4)),
      Some(MutableBuffer(new GraphEdge(3, (4.1, 4.2), 4), new GraphEdge(4, (5.1, 5.3), 6))),
      Some(MutableBuffer[GraphEdge]())
    )

    val c2 = new GraphCommunity(
      4, 3, (1.2, 3.1), 4.5, 1, 2, "aa \"abc\\def\" ff", false, 2, (2.2, 1.5), 4.6,
      Some(MutableBuffer[Double]()),
      Some(MutableBuffer[GraphEdge]()),
      Some(MutableBuffer(GraphEdge(2, (2.1, 1.3), 5)))
    )
    assert(4 === r.numCommunities)
    assert(c1 === r.communities.get.apply(0))
    assert(c2 === r.communities.get.apply(1))
  }
}
