/**
 * Copyright © 2014-2015 Uncharted Software Inc. All rights reserved.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 * http://uncharted.software/
 *
 * This software is the confidential and proprietary information of
 * Uncharted Software Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Uncharted Software Inc.
 */
package software.uncharted.graphing.clustering.usc



import org.scalatest.FunSuite

import org.apache.spark.SharedSparkContext
import org.apache.spark.graphx.Edge
import org.apache.spark.graphx.Graph

import software.uncharted.graphing.clustering.utilities.GraphOperations



class SubGraphTestSuite extends FunSuite with SharedSparkContext {
  import GraphOperations._

  test("Test sub-graph construction") {
    val nodes = sc.parallelize(0 until 12).map(n => (n.toLong, n))
    val edges = sc.parallelize(List(
      new Edge(0, 0, 1.0), new Edge(0, 1, 1.0), new Edge(0, 4, 1.0),
      new Edge(1, 2, 1.0), new Edge(1, 9, 1.0),
      new Edge(2, 3, 1.0), new Edge(2, 6, 1.0), new Edge(2, 10, 1.0),
      new Edge(4, 6, 1.0), new Edge(4, 7, 1.0), new Edge(4, 8, 1.0), new Edge(4, 11, 1.0),
      new Edge(5, 7, 1.0), new Edge(5, 10, 1.0),
      new Edge(6, 9, 1.0),
      new Edge(7, 8, 1.0),
      new Edge(8, 11, 1.0),
      new Edge(9, 10, 1.0), new Edge(9, 11, 1.0),
      new Edge(10, 11, 1.0)
    ))
    val base = Graph(nodes, edges)

    val bidirectional = base.explicitlyBidirectional(d => 2.0*d)
    val inSubGraphs = SubGraph.graphToSubGraphs(bidirectional, Some((d: Double) => d.toFloat), 3)
    val subGraphs = inSubGraphs.mapPartitionsWithIndex{case (p, i) => Iterator((p, i.toList))}.collect.toMap

    assert(3 === subGraphs.size)
    assert(1 === subGraphs(0).size)
    assert(1 === subGraphs(1).size)
    assert(1 === subGraphs(2).size)

    val subGraph0 = subGraphs(0)(0)
    assert(4 === subGraph0.numNodes)
    assert((7, 4, 11) === subGraph0.numLinks)
    assert((8.0, 4.0, 12.0) === subGraph0.totalWeight)

    val subGraph1 = subGraphs(1)(0)
    assert(4 === subGraph1.numNodes)
    assert((6, 7, 13) === subGraph1.numLinks)
    assert((6.0, 7.0, 13.0) === subGraph1.totalWeight)

    val subGraph2 = subGraphs(2)(0)
    assert(4 === subGraph2.numNodes)
    assert((8, 7, 15) === subGraph2.numLinks)
    assert((8.0, 7.0, 15.0) === subGraph2.totalWeight)
  }
}
