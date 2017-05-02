/**
  * Copyright (c) 2014-2017 Uncharted Software Inc. All rights reserved.
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



import scala.collection.mutable.{Buffer => MutableBuffer}
import org.scalatest.FunSuite



class MetadataAnalyticTestSuite extends FunSuite {
  test("Standard metadata analytic toString") {
    val expected =
      """{
        |  "numCommunities": 4,
        |  "communities": [{
        |  "hierLevel": 3,
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
        |  "analyticValues": [],
        |  "interEdges": [{"dstId": 3, "dstCoords": [4.1, 4.2], "weight": 4},{"dstId": 4, "dstCoords": [5.1, 5.3], "weight": 6}],
        |  "intraEdges": []
        |},{
        |  "hierLevel": 4,
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
        |  "analyticValues": [],
        |  "interEdges": [],
        |  "intraEdges": [{"dstId": 2, "dstCoords": [2.1, 1.3], "weight": 5}]
        |}]
        |}""".stripMargin
    val c1 = new GraphCommunity(
      3, 2, (2.1, 1.3), 4.4, 2, 3, "\"abc\\def\"", true, 2, (2.1, 1.4), 4.5,
      Array[String](),
      Some(MutableBuffer(new GraphEdge(3, (4.1, 4.2), 4), new GraphEdge(4, (5.1, 5.3), 6))),
      None
    )
    val c2 = new GraphCommunity(
      4, 3, (1.2, 3.1), 4.5, 1, 2, "aa \"abc\\def\" ff", false, 2, (2.2, 1.5), 4.6,
      Array[String](),
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
        |    "hierLevel": 3,
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
        |    "hierLevel": 4,
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
        |    "interEdges": [],
        |    "intraEdges": [{"dstId": 2, "dstCoords": [2.1, 1.3], "weight": 5}]
        |  }]
        |}""".stripMargin
    val r = GraphRecord.fromString(text)

    val c1 = new GraphCommunity(
      3, 2, (2.1, 1.3), 4.4, 2, 3, "\"abc\\def\"", true, 2, (2.1, 1.4), 4.5,
      Array[String](),
      Some(MutableBuffer(new GraphEdge(3, (4.1, 4.2), 4), new GraphEdge(4, (5.1, 5.3), 6))),
      Some(MutableBuffer[GraphEdge]())
    )

    val c2 = new GraphCommunity(
      4, 3, (1.2, 3.1), 4.5, 1, 2, "aa \"abc\\def\" ff", false, 2, (2.2, 1.5), 4.6,
      Array[String](),
      Some(MutableBuffer[GraphEdge]()),
      Some(MutableBuffer(GraphEdge(2, (2.1, 1.3), 5)))
    )
    assert(4 === r.numCommunities)

    val beginning = 0
    val r1 = r.communities.get.apply(beginning + 0)
    val foo = c1.equals(r1)
    assert(c1 === r.communities.get.apply(beginning + 0))
    assert(c2 === r.communities.get.apply(beginning + 1))
  }



  test("Adding records, hierarchy level 0") {
    def mkCommunity(degree: Int) =
      new GraphCommunity(0, 1L, (0.0, 0.0), 0.0, degree, 0L, "", false, 1L, (0.0, 0.0), 0.0, Array[String]())

    val list = Seq(7, 6, 5, 3, 2, 1).map(n => mkCommunity(n))
    assert(GraphRecord.addCommunity(list, mkCommunity(8)).map(_.degree) === List(8, 7, 6, 5, 3, 2, 1))
    assert(GraphRecord.addCommunity(list, mkCommunity(4)).map(_.degree) === List(7, 6, 5, 4, 3, 2, 1))
    assert(GraphRecord.addCommunity(list, mkCommunity(0)).map(_.degree) === List(7, 6, 5, 3, 2, 1, 0))

    val oldMax = GraphRecord.maxCommunities
    GraphRecord.maxCommunities = 10
    try {
      val longList = (10 to 1 by -1).map(n => mkCommunity(n))
      assert(GraphRecord.addCommunity(longList, mkCommunity(11)).map(_.degree) === (11 to 2 by -1).toList)
      assert(GraphRecord.addCommunity(longList, mkCommunity(5)).map(_.degree) ===
        List(10, 9, 8, 7, 6, 5, 5, 4, 3, 2))
      assert(GraphRecord.addCommunity(longList, mkCommunity(0)).map(_.degree) === (10 to 1 by -1).toList)
    } catch {
      case t: Throwable =>
        GraphRecord.maxCommunities = oldMax
        throw t
    }
  }

  test("Adding records, hierarchy level > 0") {
    def mkCommunity(numNodes: Int) =
      new GraphCommunity(1, 1L, (0.0, 0.0), 0.0, 0, numNodes.toLong, "", false, 1L, (0.0, 0.0), 0.0, Array[String]())

    val list = Seq(7, 6, 5, 3, 2, 1).map(n => mkCommunity(n))
    assert(GraphRecord.addCommunity(list, mkCommunity(4)).map(_.numNodes.toInt) === List(7, 6, 5, 4, 3, 2, 1))
    assert(GraphRecord.addCommunity(list, mkCommunity(0)).map(_.numNodes.toInt) === List(7, 6, 5, 3, 2, 1, 0))
    assert(GraphRecord.addCommunity(list, mkCommunity(8)).map(_.numNodes.toInt) === List(8, 7, 6, 5, 3, 2, 1))

    val oldMax = GraphRecord.maxCommunities
    GraphRecord.maxCommunities = 10
    try {
      val longList = (10 to 1 by -1).map(n => mkCommunity(n))
      assert(GraphRecord.addCommunity(longList, mkCommunity(11)).map(_.numNodes.toInt) === (11 to 2 by -1).toList)
      assert(GraphRecord.addCommunity(longList, mkCommunity(5)).map(_.numNodes.toInt) ===
        List(10, 9, 8, 7, 6, 5, 5, 4, 3, 2))
      assert(GraphRecord.addCommunity(longList, mkCommunity(0)).map(_.numNodes.toInt) === (10 to 1 by -1).toList)
    } catch {
      case t: Throwable =>
        GraphRecord.maxCommunities = oldMax
        throw t
    }
  }

  test("Merge communities, hierarch level 0") {
    def mkCommunity(degree: Int) =
      new GraphCommunity(0, 1L, (0.0, 0.0), 0.0, degree, 0L, "", false, 1L, (0.0, 0.0), 0.0, Array[String]())

    assert(
      GraphRecord.mergeCommunities(
        List(7, 5, 3, 1).map(n => mkCommunity(n)),
        List(8, 6, 4, 2).map(n => mkCommunity(n))
      ).map(_.degree).toList === List(8, 7, 6, 5, 4, 3, 2, 1))
    assert(
      GraphRecord.mergeCommunities(
        List(8, 6, 4, 2).map(n => mkCommunity(n)),
        List(7, 5, 3, 1).map(n => mkCommunity(n))
      ).map(_.degree).toList === List(8, 7, 6, 5, 4, 3, 2, 1))
    assert(
      GraphRecord.mergeCommunities(
        List(7, 5, 3, 1).map(n => mkCommunity(n)),
        List[GraphCommunity]()
      ).map(_.degree).toList === List(7, 5, 3, 1))
    assert(
      GraphRecord.mergeCommunities(
        List[GraphCommunity](),
        List(7, 5, 3, 1).map(n => mkCommunity(n))
      ).map(_.degree).toList === List(7, 5, 3, 1))

    val oldMax = GraphRecord.maxCommunities
    GraphRecord.maxCommunities = 10
    try {
      assert(
        GraphRecord.mergeCommunities(
          List(11, 9, 7, 5, 3, 1).map(n => mkCommunity(n)),
          List(10, 8, 6, 4, 2).map(n => mkCommunity(n))
        ).map(_.degree).toList === (11 to 2 by -1).toList)
    } catch {
      case t: Throwable =>
        GraphRecord.maxCommunities = oldMax
        throw t
    }
  }
}
