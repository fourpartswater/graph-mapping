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
package software.uncharted.graphing.clustering


import org.apache.log4j.{Level, Logger}

import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.SharedSparkContext
import org.apache.spark.graphx.{VertexId, Graph}

import software.uncharted.graphing.clustering.experiments.{PathClustering, LouvainClustering4}
import software.uncharted.graphing.clustering.utilities.ClusterConsolidator
import software.uncharted.graphing.utilities.TestUtilities._


/**
 * For cannonical implementations, see:
 *
 * https://perso.uclouvain.be/vincent.blondel/research/louvain.html
 *
 * Created by nkronenfeld on 10/30/2015.
 */
class LouvainClusteringTestSuite extends FunSuite with SharedSparkContext with BeforeAndAfter {
  before(turnOffLogSpew)

  ignore("Test new Louvain clustering") {
    val testGraph = standardBGLLGraph(sc, d => d)
    val lc = new LouvainClustering4
    val (gr1, cr1) = lc.runLouvainClustering(testGraph, 1)
    val n1 = gr1.vertices.collect()
    val e1 = gr1.edges.collect()
    val c1 = cr1.collect()

    println("\nNodes:")
    n1.foreach(println)
    println("\nEdges:")
    e1.foreach(println)
    println("\nClusters:")
    c1.foreach(println)
  }

  def showGraph[VD, ED] (name: String, graph: Graph[VD, ED])(implicit vOrd: Ordering[(VertexId, VD)]): Unit = {
    val nodes = graph.vertices.collect.sorted
    val edges = graph.edges.collect
    println
    println("Graph "+name)
    println("\tNodes:")
    nodes.foreach(node => println("\t\t"+node))
    println("\tEdges:")
    edges.foreach(edge => println("\t\t"+edge))
  }

  ignore("Test path clustering") {
    val testGraph = standardBGLLGraph(sc, d => d)
    // Raise log level to ignore info
    Logger.getRootLogger.setLevel(Level.WARN)

    val groupFcn: (VertexId, (Long, VertexId)) => VertexId = (s, d) => d._2
    val PC = new PathClustering

    showGraph("Input graph", testGraph)
    var rn: Graph[(Long, VertexId), Double] = null
    var rc: Graph[Long, Double] = testGraph
    (1 to 5).foreach{n =>
      rn = PC.checkClusters(rc)
      showGraph("Neighbor determination "+n, rn)
      rc = ClusterConsolidator.consolidate(rn, groupFcn).mapVertices(groupFcn).groupEdges(_ + _)
      showGraph("Consolidation "+n, rc)
    }
  }
}
