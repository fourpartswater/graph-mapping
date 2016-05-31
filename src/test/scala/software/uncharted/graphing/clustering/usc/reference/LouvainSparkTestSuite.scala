/**
  * This code is copied and translated to Scala from https://github.com/usc-cloud/hadoop-louvain-community
  *
  * It is therefor Copyright 2013 University of California, licensed under the Apache License, version 2.0,
  * which can be obtained at http://www.apache.org/licenses/LICENSE-2.0
  *
  * There are some minor fixes, which I have attempted to resubmit back to the baseline version.
  */
package software.uncharted.graphing.clustering.usc.reference



import org.apache.log4j.{Level, Logger}
import org.apache.spark.SharedSparkContext
import org.apache.spark.graphx.{Graph => SparkGraph, Edge}

import org.scalatest.{BeforeAndAfter, FunSuite}
import software.uncharted.graphing.utilities.GraphOperations

import GraphOperations._



class LouvainSparkTestSuite extends FunSuite with SharedSparkContext with BeforeAndAfter {
  before{
    Logger.getRootLogger.setLevel(Level.WARN)
  }

  test("Conversion from spark graph to USC-style graph") {
    val nodes = sc.parallelize(0 until 12).map(n => (n.toLong, n))
    def mkEdge(src: Int, dst: Int) = new Edge(src.toLong, dst.toLong, 1.0f)
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
    val sparkVersion = SparkGraph(nodes, edges)
    val uscVersion = LouvainSpark.sparkGraphToUSCGraphs(sparkVersion.explicitlyBidirectional(f => f), None, 2)
    val uscGraphs = uscVersion.collect
    assert(2 === uscGraphs.size)

    assert(6 === uscGraphs(0).nb_nodes)
    assert(9 === uscGraphs(0).nb_links)
    assert(9 === uscGraphs(0).remoteLinks.get.size)

    assert(6 === uscGraphs(1).nb_nodes)
    assert(12 === uscGraphs(1).nb_links)
    assert(9 === uscGraphs(1).remoteLinks.get.size)
  }

  test("Clustering by USC algorithm") {
    val nodes = sc.parallelize(0 until 12).map(n => (n.toLong, n))
    def mkEdge(src: Int, dst: Int) = new Edge(src.toLong, dst.toLong, 1.0f)
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
    val sparkVersion = SparkGraph(nodes, edges)
    val uscVersion = LouvainSpark.sparkGraphToUSCGraphs(sparkVersion.explicitlyBidirectional(f => f), None, 2)

    val (result, stats) = LouvainSpark.doClustering(1, 0.15, false)(uscVersion)
    stats.foreach(println)
  }

  test("Test that consolidating communities into a single node does not change the modularity of the whole graph") {
    val graph = new Graph(
      Array(4, 7, 12, 14, 18, 22, 25, 29, 34, 37, 43, 48, 50, 52, 55, 56),
      Array(
        /*  0 */ (2, 1.0f), (3, 1.0f), (4, 1.0f), (5, 1.0f),
        /*  1 */ (2, 1.0f), (4, 1.0f), (7, 1.0f),
        /*  2 */ (0, 1.0f), (1, 1.0f), (4, 1.0f), (5, 1.0f), (6, 1.0f),
        /*  3 */ (0, 1.0f), (7, 1.0f),
        /*  4 */ (0, 1.0f), (1, 1.0f), (2, 1.0f), (10, 1.0f),
        /*  5 */ (0, 1.0f), (2, 1.0f), (7, 1.0f), (11, 1.0f),
        /*  6 */ (2, 1.0f), (7, 1.0f), (11, 1.0f),
        /*  7 */ (1, 1.0f), (3, 1.0f), (5, 1.0f), (6, 1.0f),
        /*  8 */ (9, 1.0f), (10, 1.0f), (11, 1.0f), (14, 1.0f), (15, 1.0f),
        /*  9 */ (8, 1.0f), (12, 1.0f), (14, 1.0f),
        /* 10 */ (4, 1.0f), (8, 1.0f), (11, 1.0f), (12, 1.0f), (13, 1.0f), (14, 1.0f),
        /* 11 */ (5, 1.0f), (6, 1.0f), (8, 1.0f), (10, 1.0f), (13, 1.0f),
        /* 12 */ (9, 1.0f), (10, 1.0f),
        /* 13 */ (10, 1.0f), (11, 1.0f),
        /* 14 */ (8, 1.0f), (9, 1.0f), (10, 1.0f),
        /* 15 */ (8, 1.0f)
      ),
      None
    )

    var c1 = new Community(graph, -1, 0.15)
    val improvement = c1.one_level(false)
    val mod1 = c1.modularity

    val g2 = c1.partition2graph_binary
    val c2 = new Community(g2, -1, 0.15)
    val mod2 = c2.modularity

    assert(mod1 === mod2)
  }
}
