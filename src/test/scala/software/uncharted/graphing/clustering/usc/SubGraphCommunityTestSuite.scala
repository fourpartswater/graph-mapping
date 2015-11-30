package software.uncharted.graphing.clustering.usc

import org.apache.spark.SharedSparkContext
import org.apache.spark.graphx._
import org.scalatest.FunSuite
import software.uncharted.graphing.clustering.reference.{Graph => BGLLGraph}
import software.uncharted.graphing.clustering.reference.Community

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
    val refClusterer = new Community(refGraph, 1, 0.15)
    refClusterer.one_level(false)
    val refResult = refClusterer.partition2graph_binary

    val subGraph = new SubGraph[Int](
      (0 to 15).map(n => (n.toLong, n)).toArray,
      degrees.map(d => (d, 0)),
      (links, Array[VertexId]())
    )
    val subClusterer = new SubGraphCommunity[Int](subGraph, 1, 0.15)
    subClusterer.one_level(false)
    val subResult = subClusterer.getReducedSubgraph()

    assert(refResult.nb_nodes === subResult.numNodes)
    assert(refResult.nb_links === subResult.numLinks._1)
    for (n <- 0 until refResult.nb_nodes) {
      val refNeighbors = refResult.neighbors(n).toList
      val subNeighbors = subResult.internalNeighbors(n).toList
      assert(refResult.neighbors(n).toList === subResult.internalNeighbors(n).toList)
    }

    subClusterer.getVertexMapping.foreach(println)
  }
}