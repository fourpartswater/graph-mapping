/**
  * This code is copied and translated from https://sites.google.com/site/findcommunities, then modified futher to
  * support analytics and metadata.
  *
  * This means most of it is probably (c) 2008 V. Blondel, J.-L. Guillaume, R. Lambiotte, E. Lefebvre, and that
  * we can't distribute it without permission - though as a translation, with some optimization for readability in
  * scala, it may be a gray area.
  *
  * TThe rest is:
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

import org.scalatest.FunSuite
import software.uncharted.graphing.analytics.CustomGraphAnalytic

class CommunityClusteringTestSuite extends FunSuite {
  private def mkNode (n: Int): NodeInfo = {
    // Internal degree is one plus the maximum power of 3 (up to 2) by which N is divisible.
    // This should make it predictable which node is the primary in all communities at all levels
    val internalDegree = {
      if (0 == (n % 9)) {
        3
      } else if (0 == (n % 3)) {
        2
      } else {
        1
      }
    }
    NodeInfo(n.toLong, internalDegree, Some(s"Node $n"), Array[Any](), Array[CustomGraphAnalytic[_]]())
  }

  test("Inline community clustering") {
    // Three completely connected communities
    val graph = new Graph(
      // Node:
      //    0        1     2     3     4        5     6     7     8
      Array(3,       5,    7,    9,   12,      14,   16,   18,   21),                                     // cummulative degrees
      Array(1, 2, 3, 0, 2, 0, 1, 4, 5, 3, 5, 7, 3, 4, 7, 8, 6, 8, 6, 7, 2), // links
      (0 until 9).map(mkNode).toArray,                                      // nodes
      Some(Array[Float](                                                    // weights
        5, 5, 1, 5, 5, 5, 5, 5, 5, 5, 5, 1, 5, 5, 5, 5, 5, 5, 5, 5, 1
      ))
    )
    val community = new Community(graph, -1, 0.0)
    val clusterer = new CommunityClusterer(community, false, false, 0.0, n => new BaselineAlgorithm)

    val results = clusterer.doClustering[Map[String, String]](
      (level, startGraph, startMod, community) => {
        community.comm_nodes.zipWithIndex.filter { case (components, index) =>
          components.length > 0
        }.flatMap { case (components, index) =>
          components.map(c => (c, index))
        }.map { case (from, to) =>
          (community.g.nodeInfo(from).metaData.get, community.g.nodeInfo(to).metaData.get)
        }.toMap
      }
    )

    assert(9 === results(0).size)
    assert(3 === results(0).values.toSet.size)

     // 3 sensible groups, one with nodes 0, 1, and 2
    assert(results(0)("Node 0") === results(0)("Node 1"))
    assert(results(0)("Node 0") === results(0)("Node 2"))

    // 1 with nodes 3, 4, and 5
    assert(results(0)("Node 3") === results(0)("Node 4"))
    assert(results(0)("Node 3") === results(0)("Node 5"))

    // and one with nodes 6, 7, and 8
    assert(results(0)("Node 6") === results(0)("Node 7"))
    assert(results(0)("Node 6") === results(0)("Node 8"))

    // And make sure the groups don't overlap
    assert(results(0)("Node 0") !== results(0)("Node 3"))
    assert(results(0)("Node 0") !== results(0)("Node 6"))
  }
}
