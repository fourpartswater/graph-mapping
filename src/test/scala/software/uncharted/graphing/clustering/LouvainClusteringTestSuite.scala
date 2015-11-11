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

import org.apache.spark.{SparkContext, SharedSparkContext}
import org.apache.spark.graphx.{VertexId, Edge, Graph}
import software.uncharted.graphing.clustering.experiments.{PathClustering, LouvainClustering4}
import software.uncharted.graphing.clustering.sotera.{VertexState, LouvainHarness2}
import software.uncharted.graphing.clustering.utilities.ClusterConsolidator


/**
 * For cannonical implementations, see:
 *
 * https://perso.uclouvain.be/vincent.blondel/research/louvain.html
 *
 * Created by nkronenfeld on 10/30/2015.
 */
class LouvainClusteringTestSuite extends FunSuite with BeforeAndAfter with SharedSparkContext {
  var testGraph: Graph[Long, Double] = null
  before {
    // Take test graph from https://sites.google.com/site/findcommunities/, in which the original Louvain algorithm is
    // published.
    val nodes = sc.parallelize(0L to 15L).map(n => (n, n))
    val edges = sc.parallelize(List[Edge[Double]](
      new Edge(0L, 2L, 1.0), new Edge(0L, 3L, 1.0), new Edge(0L, 4L, 1.0), new Edge(0L, 5L, 1.0),
      new Edge(1L, 2L, 1.0), new Edge(1L, 4L, 1.0), new Edge(1L, 7L, 1.0),
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
    testGraph = Graph(nodes, edges)
  }
  after {
    testGraph = null
  }

  ignore("Test new Louvain clustering") {
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

  ignore("Test old louvain clustering") {
    var rlevel: Int = -1
    var rq: Double = 0.0
    var result: Graph[VertexState, Long] = null

    val LC = new LouvainHarness2(0.15, 1) {
      override def finalSave(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,Long]) = {
        rlevel = level
        rq = q
        result = graph
      }
    }
    LC.run(sc, testGraph.mapEdges(edge => edge.attr.toLong))

    val n1 = result.vertices.collect()
    val e1 = result.edges.collect()

    println("\nNodes:")
    n1.foreach(println)
    println("\nEdges:")
    e1.foreach(println)
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
