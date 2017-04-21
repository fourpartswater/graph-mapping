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
package software.uncharted.graphing.clustering.usc



import scala.collection.mutable.Buffer

import org.apache.spark.graphx._
import org.scalatest.FunSuite
import software.uncharted.graphing.clustering.ClusteringStatistics

import software.uncharted.graphing.clustering.unithread.reference.{Graph => BGLLGraph}
import software.uncharted.graphing.clustering.unithread.reference.{Community => BGLLCommunity}

import software.uncharted.graphing.clustering.usc.reference.{Graph => USCGraph, Community => USCCommunity, LouvainSpark, GraphMessage, RemoteMap}



/**
 * Created by nkronenfeld on 11/20/2015.
 */
class SubGraphCommunityTestSuite extends FunSuite {
  test("Test that a subgraph community detection yields identical results as an identical graph") {
    val degrees = Array(4, 7, 12, 14, 18, 22, 25, 29, 34, 37, 43, 48, 50, 52, 55, 56)
    val links = Array(
      /*  0 */ 2, 3, 4, 5,
      /*  1 */ 2, 4, 7,
      /*  2 */ 0, 1, 4, 5, 6,
      /*  3 */ 0, 7,
      /*  4 */ 0, 1, 2, 10,
      /*  5 */ 0, 2, 7, 11,
      /*  6 */ 2, 7, 11,
      /*  7 */ 1, 3, 5, 6,
      /*  8 */ 9, 10, 11, 14, 15,
      /*  9 */ 8, 12, 14,
      /* 10 */ 4, 8, 11, 12, 13, 14,
      /* 11 */ 5, 6, 8, 10, 13,
      /* 12 */ 9, 10,
      /* 13 */ 10, 11,
      /* 14 */ 8, 9, 10,
      /* 15 */ 8
    )
    val refGraph = new BGLLGraph(degrees, links)
    val refClusterer = new BGLLCommunity(refGraph, 1, 0.15)
    refClusterer.one_level(false)
    val refResult = refClusterer.partition2graph_binary

    val subGraphLinks: Array[Array[(Int, Float)]] = (0 to 15).map{n =>
      val start = if (0 == n) 0 else degrees(n - 1)
      val end = degrees(n)
      links.drop(start).take(end-start).map(link => (link, 1.0f)).toArray
    }.toArray
    val subGraph = new SubGraph[Int](
      (0 to 15).map(n => (n.toLong, n)).toArray,
      subGraphLinks,
      (0 to 15).map(n => Array[(VertexId, Float)]()).toArray
    )
    val subClusterer = new SubGraphCommunity[Int](subGraph, 1, 0.15)
    subClusterer.oneLevel(false)
    val subResult = subClusterer.getReducedSubgraphWithVertexMap(false)._1

    assert(refResult.nb_nodes === subResult.numNodes)
    assert(refResult.nb_links === subResult.numInternalLinks)
    for (n <- 0 until refResult.nb_nodes) {
      val refNeighbors = refResult.neighbors(n).toList
      val subNeighbors = subResult.internalNeighbors(n).toList
      assert(refResult.neighbors(n).toList === subResult.internalNeighbors(n).toList)
    }
  }


  test("Test multi-part community reduction and recombination") {
    def unOptionB[A, B] (input: (A, Option[B])): (A, B) = (input._1, input._2.get)

    val part1 = new SubGraph[Int](
      (0 to 5).map(n => (n.toLong, n)).toArray,
      Array(
        Array((1, 1.0f), (2, 1.0f)), Array((0, 1.0f), (2, 1.0f)), Array((0, 1.0f), (1, 1.0f)),
        Array((4, 1.0f), (5, 1.0f)), Array((3, 1.0f), (5, 1.0f)), Array((3, 1.0f), (4, 1.0f))
      ),
      (0 to 5).map(n => Array(((n+6).toLong, 1.0f))).toArray
    )
    val c1 = new SubGraphCommunity(part1, -1, 0.15)
    c1.oneLevel(false)
    val result1 = unOptionB(c1.getReducedSubgraphWithVertexMap(true))

    val part2 = new SubGraph[Int](
      (6 to 11).map(n => (n.toLong, n)).toArray,
      Array(
        Array((1, 1.0f), (2, 1.0f)), Array((0, 1.0f), (2, 1.0f)), Array((0, 1.0f), (1, 1.0f)),
        Array((4, 1.0f), (5, 1.0f)), Array((3, 1.0f), (5, 1.0f)), Array((3, 1.0f), (4, 1.0f))
      ),
      (0 to 5).map(n => Array((n.toLong, 1.0f))).toArray
    )
    val c2 = new SubGraphCommunity(part2, -1, 0.15)
    c2.oneLevel(false)
    val result2 = unOptionB(c2.getReducedSubgraphWithVertexMap(true))

    val combined = GraphConsolidator(result1._1.numNodes + result2._1.numNodes)(Iterator(result1, result2))

    assert(4 === combined.numNodes)
    for (i <- 0 to 3) {
      assert(6.0f === combined.weightedSelfLoopDegree(i))
      assert(9.0f === combined.weightedInternalDegree(i))
      val neighbors = combined.internalNeighbors(i).toList.sortBy(_._1)
      assert(2 === neighbors.size)
      assert(neighbors.contains((i, 6.0f)))
      val expectedNeighbor = (i + 2) % 4
      assert(neighbors.contains((expectedNeighbor, 3.0f)))
    }
  }
  test("Test multi-part community reduction and recombination (baseline)") {
    val part1 = new USCGraph(
      Seq(2, 4, 6, 8, 10, 12),
      Seq(
        (1, 1.0f), (2, 1.0f),
        (0, 1.0f), (2, 1.0f),
        (0, 1.0f), (1, 1.0f),
        (4, 1.0f), (5, 1.0f),
        (3, 1.0f), (5, 1.0f),
        (3, 1.0f), (4, 1.0f)
        ),
      Some(Seq(
        RemoteMap(0, 0, 1), RemoteMap(1, 1, 1), RemoteMap(2, 2, 1),
        RemoteMap(3, 3, 1), RemoteMap(4, 4, 1), RemoteMap(5, 5, 1)
      ))
    )
    val c1 = new USCCommunity(part1, -1, 0.15)
    c1.one_level(false)
    val result1 = new GraphMessage(0, c1.partition2graph_binary, c1)

    val part2 = new USCGraph(
      Seq(2, 4, 6, 8, 10, 12),
      Seq(
        (1, 1.0f), (2, 1.0f),
        (0, 1.0f), (2, 1.0f),
        (0, 1.0f), (1, 1.0f),
        (4, 1.0f), (5, 1.0f),
        (3, 1.0f), (5, 1.0f),
        (3, 1.0f), (4, 1.0f)
      ),
      Some(Seq(
        RemoteMap(0, 0, 0), RemoteMap(1, 1, 0), RemoteMap(2, 2, 0),
        RemoteMap(3, 3, 0), RemoteMap(4, 4, 0), RemoteMap(5, 5, 0)
      ))
    )
    val c2 = new USCCommunity(part2, -1, 0.15)
    c2.one_level(false)
    val result2 = new GraphMessage(1, c2.partition2graph_binary, c2)

    val combined = LouvainSpark.reconstructGraph(
      Iterator[(GraphMessage, Iterable[ClusteringStatistics])](
        (result1, List[ClusteringStatistics]()),
        (result2, List[ClusteringStatistics]())
      ),
      Buffer[ClusteringStatistics]()
    )

    assert(4 === combined.nb_nodes)
    for (i <- 0 to 3) {
      // Check local links
      assert(6.0f === combined.weighted_degree(i))
      assert(6.0f === combined.nb_selfloops(i))
      val ln = combined.neighbors(i).toList
      assert(1 === ln.size)
      val ll = ln.head
      assert(i === ll._1)
      assert(6.0f === ll._2)

      // Check formerly remote links
      assert(3.0f === combined.weighted_degree_remote(i))
      val rn = combined.remote_neighbors(i).get.toList
      assert(1 === rn.size)
      val rl = rn.head
      assert(((i + 2) % 4) === rl._1)
      assert(3.0f === rl._2)
    }
  }

  def outputGraph (g: USCGraph, name: String): Unit = {
    println
    println
    println("Number of nodes for graph "+name+": "+g.nb_nodes)
    for (i <- 0 until g.nb_nodes) {
      println
      println("\t"+i+": Neighbors        : "+g.nb_neighbors(i)+g.neighbors(i).toList.mkString(" ([", ", ", "])"))
      println("\t"+i+": Remote neighbors : "+g.nb_remote_neighbors(i)+g.remote_neighbors(i).map(_.toList.mkString(" ([", ", ", "])")).getOrElse(" ([])"))
      println("\t"+i+": Weights (s, t, r): "+g.nb_selfloops(i)+", "+g.weighted_degree(i)+", "+g.weighted_degree_remote(i))
    }
  }
}
