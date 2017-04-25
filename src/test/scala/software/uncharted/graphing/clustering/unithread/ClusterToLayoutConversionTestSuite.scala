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

import org.apache.spark.SharedSparkContext
import org.scalatest.FunSuite
import software.uncharted.graphing.analytics.CustomGraphAnalytic

class ClusterToLayoutConversionTestSuite extends FunSuite with SharedSparkContext {
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

  test("Conversion of clustering output to layout input") {
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

    val results = clusterer.doClustering(ClusterToLayoutConverter.withLevel(sc))

    assert(2 === results.length)
    assert(9 === results(0).vertices.count())
    assert(3 === results(1).vertices.count())
    val level1 = results(0).vertices.map(r => (r._2.metadata, r._2.parentId)).collect.toMap
    val level2 = results(1).vertices.map(r => (r._2.id, r._2.metadata)).collect.toMap

    // 3 sensible groups, one with nodes 0, 1, and 2
    assert(level2(level1("Node 0")) === level2(level1("Node 1")))
    assert(level2(level1("Node 0")) === level2(level1("Node 2")))

    // 1 with nodes 3, 4, and 5
    assert(level2(level1("Node 3")) === level2(level1("Node 4")))
    assert(level2(level1("Node 3")) === level2(level1("Node 5")))

    // and one with nodes 6, 7, and 8
    assert(level2(level1("Node 6")) === level2(level1("Node 7")))
    assert(level2(level1("Node 6")) === level2(level1("Node 8")))

    // And make sure the groups don't overlap
    assert(level2(level1("Node 0")) !== level2(level1("Node 3")))
    assert(level2(level1("Node 0")) !== level2(level1("Node 6")))
  }
}
