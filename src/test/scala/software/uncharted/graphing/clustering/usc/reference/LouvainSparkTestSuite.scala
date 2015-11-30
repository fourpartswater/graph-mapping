package software.uncharted.graphing.clustering.usc.reference



import org.apache.log4j.{Level, Logger}
import org.apache.spark.SharedSparkContext
import org.apache.spark.graphx.{Graph => SparkGraph, Edge}

import org.scalatest.{BeforeAndAfter, FunSuite}

import software.uncharted.graphing.clustering.utilities.GraphOperations._


/**
 * Created by nkronenfeld on 11/26/2015.
 */
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
}
