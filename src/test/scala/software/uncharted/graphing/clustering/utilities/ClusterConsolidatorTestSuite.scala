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
package software.uncharted.graphing.clustering.utilities

import org.apache.spark.SharedSparkContext
import org.apache.spark.graphx._
import org.scalatest.FunSuite


/**
 * Created by nkronenfeld on 10/29/2015.
 */
class ClusterConsolidatorTestSuite extends FunSuite with SharedSparkContext {
  test("Test node consolidation without data consolidation or edge consolidation") {
    val nodes = sc.parallelize(1 to 4).map(n => (n.toLong, (n-(n-1)%2).toLong))
    val edges = sc.parallelize(List(
      (1, 1, 0.5), (1, 2, 1.0), (1, 3, 1.5), (1, 4, 2.0),
      (2, 4, 2.0),
      (3, 1, 0.75), (3, 2, 1.0),
      (4, 1, 0.25), (4, 2, 0.75), (4, 3, 1.25), (4, 4, 1.75)
    )).map{case (start, end, weight) => new Edge(start, end, weight)}
    val graph = Graph(nodes, edges)

    val newNodeFcn: (VertexId, Long) => VertexId = (id, target) => target
    val consolidated = ClusterConsolidator.consolidate(graph, newNodeFcn)
    val newNodes = consolidated.vertices.collect.sorted.toList
    val newEdges = consolidated.edges.map(edge =>
      (edge.srcId, edge.dstId, edge.attr)
    ).collect.sorted.toList
    assert(List((1L, 1L), (3L, 3L)) === newNodes)
    assert(List(
      (1L, 1L, 0.5), (1L, 1L, 1.0),
      (1L, 3L, 1.5), (1L, 3L, 2.0), (1L, 3L, 2.0),
      (3L, 1L, 0.25), (3L, 1L, 0.75), (3L, 1L, 0.75), (3L, 1L, 1.0),
      (3L, 3L, 1.25), (3L, 3L, 1.75)
    ) === newEdges)
  }

  test("Test node consolidation with data consolidation but no edge consolidation") {
    val nodes = sc.parallelize(1 to 4).map(n => (n.toLong, ((n-(n-1)%2).toLong, n)))
    val edges = sc.parallelize(List(
      (1, 1, 0.5), (1, 2, 1.0), (1, 3, 1.5), (1, 4, 2.0),
      (2, 4, 2.0),
      (3, 1, 0.75), (3, 2, 1.0),
      (4, 1, 0.25), (4, 2, 0.75), (4, 3, 1.25), (4, 4, 1.75)
    )).map{case (start, end, weight) => new Edge(start, end, weight)}
    val graph = Graph(nodes, edges)

    val newNodeFcn: (VertexId, (Long, Int)) => VertexId = (id, value) => value._1
    val mergeNodeFcn: ((Long, Int), (Long, Int)) => (Long, Int) = (a, b) => (a._1, a._2 + b._2)
    val consolidated = ClusterConsolidator.consolidate(graph, newNodeFcn, Some(mergeNodeFcn))
    val newNodes = consolidated.vertices.collect.sorted.toList
    val newEdges = consolidated.edges.map(edge =>
      (edge.srcId, edge.dstId, edge.attr)
    ).collect.sorted.toList
    assert(List((1L, (1L, 3)), (3L, (3L, 7))) === newNodes)
    assert(List(
      (1L, 1L, 0.5), (1L, 1L, 1.0),
      (1L, 3L, 1.5), (1L, 3L, 2.0), (1L, 3L, 2.0),
      (3L, 1L, 0.25), (3L, 1L, 0.75), (3L, 1L, 0.75), (3L, 1L, 1.0),
      (3L, 3L, 1.25), (3L, 3L, 1.75)
    ) === newEdges)
  }

  test("Test node consolidation with data consolidation and edge consolidation") {
    val nodes = sc.parallelize(1 to 4).map(n => (n.toLong, ((n-(n-1)%2).toLong, n)))
    val edges = sc.parallelize(List(
      (1, 1, 0.5), (1, 2, 1.0), (1, 3, 1.5), (1, 4, 2.0),
      (2, 4, 2.0),
      (3, 1, 0.75), (3, 2, 1.0),
      (4, 1, 0.25), (4, 2, 0.75), (4, 3, 1.25), (4, 4, 1.75)
    )).map{case (start, end, weight) => new Edge(start, end, weight)}
    val graph = Graph(nodes, edges)

    val newNodeFcn: (VertexId, (Long, Int)) => VertexId = (id, value) => value._1
    val mergeNodeFcn: ((Long, Int), (Long, Int)) => (Long, Int) = (a, b) => (a._1, a._2 + b._2)
    val consolidated =
      ClusterConsolidator
        .consolidate(graph, newNodeFcn, Some(mergeNodeFcn))
        .groupEdges(_ + _)
    val newNodes = consolidated.vertices.collect.sorted.toList
    val newEdges = consolidated.edges.map(edge =>
      (edge.srcId, edge.dstId, edge.attr)
    ).collect.sorted.toList
    assert(List((1L, (1L, 3)), (3L, (3L, 7))) === newNodes)
    assert(List(
      (1L, 1L, 1.5),
      (1L, 3L, 5.5),
      (3L, 1L, 2.75),
      (3L, 3L, 3.0)
    ) === newEdges)
  }
}
