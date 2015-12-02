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
package software.uncharted.graphing.utilities

import org.apache.log4j.{Level, Logger}
import org.apache.spark.SharedSparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.scalatest.Matchers._
import org.scalatest.{BeforeAndAfter, FunSuite}

import software.uncharted.graphing.clustering.reference.{Community, Graph => ReferenceGraph}
import software.uncharted.spark.ExtendedRDDOperationsTestSuite


/**
 * Created by nkronenfeld on 11/11/15.
 */
class GraphOperationsTestSuite extends FunSuite with SharedSparkContext with BeforeAndAfter {
  import GraphOperations._
  import TestUtilities._
  val epsilon = 1E-6


  before(turnOffLogSpew)

  test("Test individual modularity calculation with cannonical graph") {
    val canonicalGraph = standardBGLLGraph(sc, d => d)
    val canonicalModularity: Double = canonicalGraph.calculateIndividualModularity(d => d)
    canonicalModularity should be ((-1.0 / 14.0) +- epsilon)
  }

  test("Test individual modularity calculation with a graph with self-links") {
    val nodes = sc.parallelize(0L to 3L).map(n => (n, n))
    val edges = sc.parallelize(List[Edge[Double]](
        new Edge(0L, 0L, 1.0),
        new Edge(2L, 2L, 1.0),
        new Edge(1L, 3L, 1.0)
    ))
    val graph = Graph(nodes, edges)
    val modularity = graph.calculateIndividualModularity(d => d)
    modularity should be ((7.0 / 18.0) +- epsilon)
  }

  test("Test modularity calculation with canonical graph and 1-node communities") {
    val canonicalGraph = standardBGLLGraph(sc, d => d)

    // Each vertex in its own community (the default)
    val individualModularity: Double = canonicalGraph.calculateModularity(d => d)
    individualModularity should be((-1.0 / 14.0) +- epsilon)
  }

  test("Test modularity calculation with canonical graph and 2-node communities") {
    val canonicalGraph = standardBGLLGraph(sc, d => d)

    // Vertices grouped pairwise (0 and 1, 2 and 3, etc)
    val pairModularity = canonicalGraph.calculateModularity(d => d, (vid, data) => vid / 2)
    val targetPairModularity = -1.0 / 14.0 + 2.0 * (3.0 - (12.0 + 10.0 + 16.0 + 12.0 + 15.0 + 30.0 + 4.0 + 3.0) / 56.0) / 56.0
    pairModularity should be(targetPairModularity +- epsilon)
  }

  test("Test modularity calculation with canonical graph and nodes 1 and 2 conjoined") {
    val canonicalGraph = standardBGLLGraph(sc, d => d)

    // Vertices all in their own community, except vertex 2, which is in community 1
    val modularity = canonicalGraph.calculateModularity(d => d, (vid, data) => if (2L == vid) 1L else vid)
    val targetModularity = -1.0 / 14.0 + 2.0 * (1.0 - 15 / 56.0) / 56.0
    modularity should be (targetModularity +- epsilon)
  }

  test("Test individual modularity on canonical graph with nodes 1 and 2 conjoined") {
    // Here we take the test graph from https://sites.google.com/site/findcommunities/, in which the original Louvain
    // algorithm is published, turn node 0 into node 1, and combine node 1 and 2 into 2
    //
    // This should be the equivalent of the last test ("Test modularity calculation with canonical graph and nodes 1
    // and 2 conjoined"), but instead of saying nodes 1 and 2 are in the same group, we actually combine them into a
    // single node.
    val nodes = sc.parallelize(1L to 15L).map(n => (n, n))
    val edges = sc.parallelize(List[Edge[Double]](
      new Edge(1L, 2L, 1.0), new Edge(1L, 3L, 1.0), new Edge(1L, 4L, 1.0), new Edge(1L, 5L, 1.0),
      new Edge(2L, 2L, 1.0), new Edge(2L, 4L, 1.0), new Edge(2L, 7L, 1.0),
      new Edge(2L, 4L, 1.0), new Edge(2L, 5L, 1.0), new Edge(2L, 6L, 1.0),
      new Edge(3L, 7L, 1.0),
      new Edge(4L, 10L, 1.0),
      new Edge(5L, 7L, 1.0), new Edge(5L, 11L, 1.0),
      new Edge(6L, 7L, 1.0), new Edge(6L, 11L, 1.0),
      new Edge(8L, 9L, 1.0), new Edge(8L, 10L, 1.0), new Edge(8L, 11L, 1.0), new Edge(8L, 14L, 1.0), new Edge(8L, 15L, 1.0),
      new Edge(9L, 12L, 1.0), new Edge(9L, 14L, 1.0),
      new Edge(10L, 11L, 1.0), new Edge(10L, 12L, 1.0), new Edge(10L, 13L, 1.0), new Edge(10L, 14L, 1.0),
      new Edge(11L, 13L, 1.0)
    ))
    val canonicalGraph = Graph(nodes, edges)

    // Vertices all in their own community, except vertex 2, which is in community 1
    val modularity = canonicalGraph.calculateIndividualModularity(d => d)
    val targetModularity = -1.0 / 14.0 + 2.0 * (1.0 - 15 / 56.0) / 56.0
    modularity should be (targetModularity +- epsilon)
  }

  test("Test modularity vs. baseline calculation") {
    val canonicalGraph = standardBGLLGraph(sc, d => d)

    // Convert to reference form
    val refGraph = ReferenceGraph(canonicalGraph)
    val refCom = new Community(refGraph, -1, 0.0001)
    val referenceModularity = refCom.modularity

    val modularity = canonicalGraph.calculateIndividualModularity(d => 1.0)
    assert(referenceModularity === modularity)
  }

  test("Test explicit bidirectionality") {
    val graph = Graph[Int, Double](
      sc.parallelize(0 until 7).map(n => (n.toLong, n)),
      sc.parallelize(List[Edge[Double]](
        new Edge(0, 1, 1.5), new Edge(0, 2, 0.75), new Edge(1, 1, 0.4), new Edge(1, 2, 1.01),
        new Edge(2, 3, 4.1), new Edge(2, 4, 4.2), new Edge(2, 5, 4.3), new Edge(5, 5, 0.1)
      ))
    )
    val bidirectional = graph.explicitlyBidirectional(d => d*2.0)
    val edgesBySource = bidirectional.edges
      .map{edge => (edge.srcId, List(edge))}
      .reduceByKey(_ ++ _)
      .collect.map{case (source, edges) => (source, edges.sortBy(_.dstId).map(edge => (edge.dstId, edge.attr)))}
      .toMap

    assert(List((1, 1.5), (2, 0.75)) === edgesBySource(0))
    assert(List((0, 1.5), (1, 0.8), (2, 1.01)) === edgesBySource(1))
    assert(List((0, 0.75), (1, 1.01), (3, 4.1), (4, 4.2), (5, 4.3)) === edgesBySource(2))
    assert(List((2, 4.1)) === edgesBySource(3))
    assert(List((2, 4.2)) == edgesBySource(4))
    assert(List((2, 4.3), (5, 0.2)) === edgesBySource(5))
    // Make sure that's all of them.
    assert(14 === edgesBySource.map(_._2.size).reduce(_ + _))
  }

  private def getEdges[ED: Ordering] (input: Graph[_, ED]): Map[(Int, Int), List[ED]] = {
    input.edges.map(edge =>
      if (edge.srcId < edge.dstId) ((edge.srcId.toInt, edge.dstId.toInt) -> List(edge.attr))
      else ((edge.dstId.toInt, edge.srcId.toInt) -> List(edge.attr))
    ).reduceByKey(_ ++ _).collect.map{case (key, value) => (key, value.sorted)}.toMap
  }

  test("Test implicit bidirectionality (simple round-trip)") {
    val base = Graph[Int, Double](
      sc.parallelize(0 until 7).map(n => (n.toLong, n)),
      sc.parallelize(List[Edge[Double]](
        new Edge(0, 1, 1.5), new Edge(0, 2, 0.75), new Edge(1, 1, 0.4), new Edge(1, 2, 1.01),
        new Edge(2, 3, 4.1), new Edge(2, 4, 4.2), new Edge(2, 5, 4.3), new Edge(5, 5, 0.1)
      ))
    )
    val explicitVersion = base.explicitlyBidirectional(d => d*2.0)
    val implicitVersion = explicitVersion.implicitlyBidirectional(d => d*0.5)

    val edges = getEdges(implicitVersion)

    assert(List(1.5) === edges((0, 1)))
    assert(List(0.75) === edges((0, 2)))
    assert(List(0.4) === edges((1, 1)))
    assert(List(1.01) === edges((1, 2)))
    assert(List(4.1) === edges((2, 3)))
    assert(List(4.2) === edges((2, 4)))
    assert(List(4.3) === edges((2, 5)))
    assert(List(0.1) === edges((5, 5)))
    assert(8 === edges.size)
  }

  test("Test implicit bidirectionality with complex partition breakup") {
    val base = constructGraph(sc,
      Map(0 -> Seq((0L, 0L), (1L, 1L), (2L, 2L), (3L, 3L))),
      Map(
        0 -> Seq(new Edge(0, 1, 0.4), new Edge(0, 2, 0.5), new Edge(0, 3, 0.6)),
        1 -> Seq(new Edge(1, 0, 0.4), new Edge(2, 0, 0.5), new Edge(3, 0, 0.6))
      )
    )
    val implicitVersion = base.implicitlyBidirectional(d => d * 0.5)

    val edges = getEdges(implicitVersion)

    assert(List(0.4) === edges((0, 1)))
    assert(List(0.5) === edges((0, 2)))
    assert(List(0.6) === edges((0, 3)))
    assert(3 === edges.size)
  }

  test("Test implicit and explicit bidirectionality with multiple self-links") {
    val base = constructGraph(sc, 4, Map(0 -> Seq(new Edge(0, 0, 1.0), new Edge(0, 0, 1.0), new Edge(0, 0, 2.0))))

    val explicitVersion = base.explicitlyBidirectional(d => d*2.0)
    val implicitVersion = explicitVersion.implicitlyBidirectional(d => d*0.5)

    val explicitEdges = getEdges(explicitVersion)
    assert(List(2.0, 2.0, 4.0) === explicitEdges((0, 0)))
    assert(1 === explicitEdges.size)

    val implicitEdges = getEdges(implicitVersion)
    assert(List(1.0, 1.0, 2.0) === implicitEdges((0, 0)))
    assert(1 === implicitEdges.size)
  }

  test("Test implicit bidirectionality with multiple links in the same direction") {
    val base = constructGraph(sc, 4, Map(0 -> Seq(
      new Edge(0, 1, 1.0), new Edge(0, 1, 1.0), new Edge(1, 0, 1.0), new Edge(1, 0, 1.0),
      new Edge(2, 3, 1.0), new Edge(3, 2, 1.0), new Edge(3, 2, 1.0), new Edge(3, 2, 1.0),
      new Edge(4, 5, 1.0), new Edge(4, 5, 1.0), new Edge(4, 5, 1.0), new Edge(5, 4, 1.0), new Edge(5, 4, 1.0),
      new Edge(6, 7, 1.0), new Edge(7, 6, 1.0), new Edge(7, 6, 1.0)
    )))

    val implicitVersion = base.implicitlyBidirectional(d => d*0.5)

    val implicitEdges = getEdges(implicitVersion)
    assert(List(1.0, 1.0) === implicitEdges((0, 1)))
    assert(List(1.0, 1.0, 1.0) === implicitEdges((2, 3)))
    assert(List(1.0, 1.0, 1.0) === implicitEdges((4, 5)))
    assert(List(1.0, 1.0) === implicitEdges((6, 7)))
  }
}
