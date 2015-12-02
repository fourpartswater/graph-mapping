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



import org.apache.log4j.{Level, Logger}

import scala.collection.mutable.{Map => MutableMap}

import org.scalatest.FunSuite
import org.scalatest.Matchers._

import org.apache.spark.SharedSparkContext
import org.apache.spark.graphx.{Edge, Graph}

import software.uncharted.graphing.utilities.GraphOperations
import software.uncharted.graphing.utilities.TestUtilities



class SubGraphTestSuite extends FunSuite with  SharedSparkContext {
  import GraphOperations._
  import TestUtilities._
  val epsilon = 1E-6

  override def beforeAll(): Unit = {
    turnOffLogSpew
    super.beforeAll()
  }

  private def constructSampleSubgraphs (n: Int, weightFcn: (Int, Int) => Float = (a, b) => 1.0f) = {
    val nodes = sc.parallelize(0 until 12).map(n => (n.toLong, n))
    def mkEdge(src: Int, dst: Int) = new Edge(src.toLong, dst.toLong, weightFcn(src, dst))
    val edges = sc.parallelize(List(
      mkEdge(0, 0), mkEdge(0, 1), mkEdge(0, 4),
      mkEdge(1, 2), mkEdge(1, 9),
      mkEdge(2, 3), mkEdge(2, 6), mkEdge(2, 10),
      mkEdge(4, 6), mkEdge(4, 7), mkEdge(4, 8), mkEdge(4, 11),
      mkEdge(5, 7), mkEdge(5, 10),
      mkEdge(6, 9),
      mkEdge(7, 8),
      mkEdge(8, 11),
      mkEdge(9, 10), mkEdge(9, 11),
      mkEdge(10, 11)
    ))
    val base = Graph(nodes, edges)

    val bidirectional = base.explicitlyBidirectional(d => 2.0f*d)
    SubGraph.graphToSubGraphs(bidirectional, Some((d: Float) => d.toFloat), n)
  }

  test("Test sub-graph construction") {
    val inSubGraphs = constructSampleSubgraphs(3)
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

  test("Test full graph reconstitution") {
    val subGraphs = constructSampleSubgraphs(3, (s, d) => 1.0f + (s+d)/22.0f)
    val graph = Graph.fromEdges[Int, Float](subGraphs.flatMap(_.toFullGraphLinks), -1)

    // Check vertices
    val vertices = graph.vertices.collect.toList.sorted
    assert(12 === vertices.size)
    (0 until 12).foreach(n => assert((n.toLong, -1) === vertices(n)))

    // Check edges
    val edges = MutableMap[(Long, Long), Float]()
    graph.edges.map(edge => ((edge.srcId, edge.dstId), edge.attr)).collect.foreach { case (endPoints, weight) =>
      edges(endPoints) = weight
    }
    def checkEdge (src: Int, dst: Int): Unit = {
      if (src == dst) {
        assert(edges.remove((src, dst)).get === 2.0f + (src + dst)/ 11.0f)
      } else {
        assert(edges.remove((src, dst)).get === 1.0f + (src + dst)/ 22.0f)
        assert(edges.remove((dst, src)).get === 1.0f + (src + dst)/ 22.0f)
      }
    }
    checkEdge(0, 0)
    checkEdge(0, 1)
    checkEdge(0, 4)
    checkEdge(1, 2)
    checkEdge(1, 9)
    checkEdge(2, 3)
    checkEdge(2, 6)
    checkEdge(2, 10)
    checkEdge(4, 6)
    checkEdge(4, 7)
    checkEdge(4, 8)
    checkEdge(4, 11)
    checkEdge(5, 7)
    checkEdge(5, 10)
    checkEdge(6, 9)
    checkEdge(7, 8)
    checkEdge(8, 11)
    checkEdge(9, 10)
    checkEdge(9, 11)
    checkEdge(10, 11)
    assert(edges.isEmpty)
  }

  test("Test degree calculation") {
    val subGraphsRDD = constructSampleSubgraphs(3, (s, d) => 1.0f + (s+d)/20.0f)
    val subGraphs = subGraphsRDD.mapPartitionsWithIndex{case (p, i) => Iterator((p, i.toList.head))}.collect.toMap


    // Node 0
    subGraphs(0).weightedInternalDegree(0) should be (3.05 +- epsilon)
    subGraphs(0).weightedExternalDegree(0) should be (1.20 +- epsilon)
    subGraphs(0).weightedDegree(0) should be (4.25 +- epsilon)

    // Node 1
    subGraphs(0).weightedInternalDegree(1) should be (2.20 +- epsilon)
    subGraphs(0).weightedExternalDegree(1) should be (1.50 +- epsilon)
    subGraphs(0).weightedDegree(1) should be (3.70 +- epsilon)

    // Node 2
    subGraphs(0).weightedInternalDegree(2) should be (2.40 +- epsilon)
    subGraphs(0).weightedExternalDegree(2) should be (3.00 +- epsilon)
    subGraphs(0).weightedDegree(2) should be (5.40 +- epsilon)

    // Node 3
    subGraphs(0).weightedInternalDegree(3) should be (1.25 +- epsilon)
    subGraphs(0).weightedExternalDegree(3) should be (0.00 +- epsilon)
    subGraphs(0).weightedDegree(3) should be (1.25 +- epsilon)

    // Node 4
    subGraphs(1).weightedInternalDegree(0) should be (3.05 +- epsilon)
    subGraphs(1).weightedExternalDegree(0) should be (4.55 +- epsilon)
    subGraphs(1).weightedDegree(0) should be (7.60 +- epsilon)

    // Node 5
    subGraphs(1).weightedInternalDegree(1) should be (1.60 +- epsilon)
    subGraphs(1).weightedExternalDegree(1) should be (1.75 +- epsilon)
    subGraphs(1).weightedDegree(1) should be (3.35 +- epsilon)

    // Node 6
    subGraphs(1).weightedInternalDegree(2) should be (1.50 +- epsilon)
    subGraphs(1).weightedExternalDegree(2) should be (3.15 +- epsilon)
    subGraphs(1).weightedDegree(2) should be (4.65 +- epsilon)

    // Node 7
    subGraphs(1).weightedInternalDegree(3) should be (3.15 +- epsilon)
    subGraphs(1).weightedExternalDegree(3) should be (1.75 +- epsilon)
    subGraphs(1).weightedDegree(3) should be (4.90 +- epsilon)

    // Node 8
    subGraphs(2).weightedInternalDegree(0) should be (1.95 +- epsilon)
    subGraphs(2).weightedExternalDegree(0) should be (3.35 +- epsilon)
    subGraphs(2).weightedDegree(0) should be (5.30 +- epsilon)

    // Node 9
    subGraphs(2).weightedInternalDegree(1) should be (3.95 +- epsilon)
    subGraphs(2).weightedExternalDegree(1) should be (3.25 +- epsilon)
    subGraphs(2).weightedDegree(1) should be (7.20 +- epsilon)

    // Node 10
    subGraphs(2).weightedInternalDegree(2) should be (4.00 +- epsilon)
    subGraphs(2).weightedExternalDegree(2) should be (3.35 +- epsilon)
    subGraphs(2).weightedDegree(2) should be (7.35 +- epsilon)

    // Node 11
    subGraphs(2).weightedInternalDegree(3) should be (6.00 +- epsilon)
    subGraphs(2).weightedExternalDegree(3) should be (1.75 +- epsilon)
    subGraphs(2).weightedDegree(3) should be (7.75 +- epsilon)
  }

  test("Test self-loop calculation") {
    val subGraphsRDD = constructSampleSubgraphs(3, (s, d) => 1.0f + (s+d)/20.0f)
    val subGraphs = subGraphsRDD.mapPartitionsWithIndex{case (p, i) => Iterator((p, i.toList.head))}.collect.toMap

    subGraphs(0).weightedSelfLoopDegree(0) should be (2.0 +- epsilon)
    subGraphs(0).weightedSelfLoopDegree(1) should be (0.0 +- epsilon)
    subGraphs(0).weightedSelfLoopDegree(2) should be (0.0 +- epsilon)
    subGraphs(0).weightedSelfLoopDegree(3) should be (0.0 +- epsilon)

    subGraphs(1).weightedSelfLoopDegree(0) should be (0.0 +- epsilon)
    subGraphs(1).weightedSelfLoopDegree(1) should be (0.0 +- epsilon)
    subGraphs(1).weightedSelfLoopDegree(2) should be (0.0 +- epsilon)
    subGraphs(1).weightedSelfLoopDegree(3) should be (0.0 +- epsilon)

    subGraphs(2).weightedSelfLoopDegree(0) should be (0.0 +- epsilon)
    subGraphs(2).weightedSelfLoopDegree(1) should be (0.0 +- epsilon)
    subGraphs(2).weightedSelfLoopDegree(2) should be (0.0 +- epsilon)
    subGraphs(2).weightedSelfLoopDegree(3) should be (0.0 +- epsilon)
  }

  test("Test conversion to reference implementation") {
    val inSubGraphs = constructSampleSubgraphs(3)
    val subGraphs = inSubGraphs.mapPartitionsWithIndex { case (p, i) => Iterator((p, i.toList)) }.collect.toMap

    val subGraph0 = subGraphs(0)(0)
    val ref0 = subGraph0.toReferenceImplementation
    assert(4 === ref0.nb_nodes)
    assert(7 === ref0.nb_links)
    assert(8.0 === ref0.total_weight)

    val subGraph1 = subGraphs(1)(0)
    val ref1 = subGraph1.toReferenceImplementation
    assert(4 === ref1.nb_nodes)
    assert(6 === ref1.nb_links)
    assert(6.0 === ref1.total_weight)

    val subGraph2 = subGraphs(2)(0)
    val ref2 = subGraph2.toReferenceImplementation
    assert(4 === ref2.nb_nodes)
    assert(8 === ref2.nb_links)
    assert(8.0 === ref2.total_weight)
  }
}
