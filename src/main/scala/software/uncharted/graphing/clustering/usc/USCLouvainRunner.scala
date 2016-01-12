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



import java.util.Date

import scala.collection.generic.Growable
import scala.collection.mutable.ListBuffer
import scala.reflect.ClassTag
import scala.util.Try

import org.apache.log4j.{Level, Logger}
import org.apache.spark.graphx.{Graph => SparkGraph, Edge}
import org.apache.spark.{Accumulable, SparkConf, SparkContext}
import org.apache.spark.rdd.RDD

import com.oculusinfo.tilegen.util.ArgumentParser

import software.uncharted.graphing.clustering.ClusteringStatistics
import software.uncharted.graphing.utilities.GraphOperations
import GraphOperations._



/**
 * A class to run my version of the USC Louvain Clustering algorithm on a spark graph
 */
object USCLouvainRunner {
  def usage: Unit = {
    println("Usage: LouvainSpark <node file> <node prefix> <edge file> <edge prefix> <partitions>")
  }

  def parseLine[TA] (separator: String, columnA: Int, conversionA: String => TA): String => TA = line => {
    val fields = line.split(separator)
    conversionA(fields(columnA))
  }

  def parseLine[TA, TB] (separator: String,
                         columnA: Int, conversionA: String => TA,
                         columnB: Int, conversionB: String => TB): String => (TA, TB) = line => {
    val fields = line.split(separator)
    (conversionA(fields(columnA)), conversionB(fields(columnB)))
  }

  def parseLine[TA, TB, TC] (separator: String,
                             columnA: Int, conversionA: String => TA,
                             columnB: Int, conversionB: String => TB,
                             columnC: Int, conversionC: String => TC): String => (TA, TB, TC) = line => {
    val fields = line.split(separator)
    (conversionA(fields(columnA)), conversionB(fields(columnB)), conversionC(fields(columnC)))
  }

  def getData[T: ClassTag] (sc: SparkContext, source: String, prefixOpt: Option[String], parser: String => T): RDD[T] = {
    val rawSource = sc.textFile(source)
    val filteredSource = prefixOpt.map(prefix => rawSource.filter(_.startsWith(prefix))).getOrElse(rawSource)
    filteredSource.map{line => Try(parser(line))}.filter(_.isSuccess).map(_.get)
  }

  def main (args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val argParser = new ArgumentParser(args)

    val (nodeFile, nodePrefix, nodeSeparator, nodeIdCol, edgeFile, edgePrefix, edgeSeparator, edgeSrcCol, edgeDstCol, weightColOpt, partitions):
    (Option[String], Option[String], String, Option[Int], String, Option[String], String, Int, Int, Option[Int], Int) =
      try {
        val nodeFile = argParser.getStringOption("nodeFile", "The data file from which to get nodes")
        val nodePrefix = argParser.getStringOption("nodePrefix", "A prefix required on every line of the node data file for a line to count as a node.")
        val nodeSeparator = argParser.getString("nodeSeparator", "A separator string for breaking the node data file into columns", Some("\t"))
        val nodeIdCol = argParser.getIntOption("nodeIdCol", "The column number of the column of node lines containing the node ID (which must be parsable into a long)")

        val edgeFile = argParser.getString("edgeFile", "The data file from which to get edges")
        val edgePrefix = argParser.getStringOption("edgePrefix", "A prefix required on every line of the edge data file for a line to count as a edge.")
        val edgeSeparator = argParser.getString("edgeSeparator", "A separator string for breaking the edge data file into columns", Some("\t"))
        val edgeSrcCol = argParser.getInt("edgeSrcCol", "The column number of the column of edge lines containing the node ID of the source node")
        val edgeDstCol = argParser.getInt("edgeDstCol", "The column number of the column of edge lines containing the node ID of the destination node")
        val weightColOpt = argParser.getIntOption("edgeWeightCol", "The column number of the column of edge lines containing the weight of the edge")

        val partitions = argParser.getInt("partitions", "The number of partitions into which to break the graph for first-round processing")
        (nodeFile, nodePrefix, nodeSeparator, nodeIdCol, edgeFile, edgePrefix, edgeSeparator, edgeSrcCol, edgeDstCol, weightColOpt, partitions)
      } catch {
        case e: Exception => {
          argParser.usage
          return
        }
      }

    val sc = new SparkContext((new SparkConf).setAppName("USC Louvain Clustering"))

    val edges: RDD[Edge[Float]] = weightColOpt.map { weightCol =>
      getData(
        sc, edgeFile, edgePrefix,
        parseLine(edgeSeparator, edgeSrcCol, _.toLong, edgeDstCol, _.toLong, weightCol, _.toFloat)
      ).map { case (src, dst, weight) => new Edge(src, dst, weight) }
    }.getOrElse {
      getData(
        sc, edgeFile, edgePrefix,
        parseLine(edgeSeparator, edgeSrcCol, _.toLong, edgeDstCol, _.toLong)
      ).map { case (src, dst) => new Edge(src, dst, 1.0f) }
    }

    val sparkGraph: SparkGraph[Long, Float] =
      if (nodeFile.isDefined && nodeIdCol.isDefined) {
        val nodes: RDD[(Long, Long)] = getData(
          sc, nodeFile.get, nodePrefix,
          parseLine(nodeSeparator, nodeIdCol.get, _.toLong)
        ).map(n => (n, n))

        SparkGraph(nodes, edges).explicitlyBidirectional(f => f).renumber()
      } else {
        SparkGraph.fromEdges(edges, -1).mapVertices{case (id, data) => id}
      }


    // Convert to a set of sub-graphs
    val subGraphs = SubGraph.graphToSubGraphs(sparkGraph, (f: Float) => f, partitions)

    // Set up a clustering statistics accumulator
    val stats = sc.accumulableCollection(ListBuffer[ClusteringStatistics]())

    subGraphs.mapPartitionsWithIndex{case (partition, graphs) =>
      graphs.map(graph => (partition, graph))
    }.foreach { case (partition, graph) =>
      println("Partition " + partition + " constructed with: ")
      println("\tnodes: " + graph.numNodes)
      println("\tinternal links: " + graph.numInternalLinks)
      println("\texternal links: " + graph.numExternalLinks)
      println("\tTimestamp: "+new Date())
    }
    val resultGraph = doClustering(-1, 0.15, false)(subGraphs, stats)

    println("Resultant graph:")
    println("\tnodes: " + resultGraph.numNodes)
    println("\tinternal links: " + resultGraph.numInternalLinks)
    println("\texternal links: " + resultGraph.numExternalLinks)
    println("\tTimestamp: "+new Date())

    stats.value.foreach(println)
  }

  /**
   * Run the complete USC version of Louvain clustering
   * @param input An RDD of graph objects, one per partition
   */
  def doClustering (numPasses: Int, minModularityIncrease: Double, randomize: Boolean)
                   (input: RDD[SubGraph[Long]],
                    stats: Accumulable[_ <: Growable[ClusteringStatistics], ClusteringStatistics]) = {
    println("Starting first pass on "+input.partitions.size+" partitions")
    val firstPass = input.mapPartitionsWithIndex { case (partition, index) =>
      def logStat (stat: String, value: String) =
        println("\tpartition "+partition+": "+stat+": "+value+"\t\t"+new Date())

      logStat("first pass", "start")
      val g1 = index.next()
      logStat("first pass", "graph")
      val c1 = new SubGraphCommunity(g1, numPasses, minModularityIncrease)
      logStat("nodes 1", g1.numNodes.toString)
      logStat("internal links 1", g1.numInternalLinks.toString)
      logStat("external links 1", g1.numExternalLinks.toString)
      logStat("modularity 1", c1.modularity.toString)
      c1.one_level(randomize)
      val result = c1.getReducedSubgraphWithVertexMap(true)
      c1.clusteringStatistics.foreach(cs =>
        stats += cs.addLevelAndPartition(1, partition)
      )
      logStat("first pass", "complete")

      Iterator((result._1, result._2.get))
    }

    // Get the total nodes
    firstPass.cache()
    val totalNodes = firstPass.map(_._1.numNodes).reduce(_ + _)

    val graph = firstPass.repartition(1).mapPartitions{i =>
      def logStat (stat: String, value: String, level: Int) =
        println("\tlevel "+level+": "+stat+": "+value+"\t\t"+new Date())

      val precision = 0.000001
      logStat("consolidation", "start", 0)
      val g = GraphConsolidator(totalNodes)(i)
      logStat("consolidation", "reconstruction", 0)

      var g2 = g
      var c  = new SubGraphCommunity(g, -1, precision)
      logStat("nodes", g.numNodes.toString, 0)
      logStat("internal links", g.numInternalLinks.toString, 0)
      logStat("external links", g.numExternalLinks.toString, 0)
      logStat("modularity", c.modularity.toString, 0)

      // First pass is done; do the rest of the clustering
      var modularity = c.modularity
      var new_modularity = modularity
      var improvement = true
      var level = 2

      do {
        logStat("consolidation", "start", level)
        improvement = c.one_level()
        new_modularity = c.modularity
        level = level + 1
        logStat("consolidation", "clustered", level)
        g2 = c.getReducedSubgraphWithVertexMap(false)._1
        logStat("consolidation", "consolidated", level)
        c.clusteringStatistics.map { cs =>
          val levelStats = cs.addLevelAndPartition(level, -1)
          println("\tLevel stats: "+levelStats)
          stats += levelStats
        }

        logStat("consolidation", "done", level)
        c = new SubGraphCommunity(g2, -1, precision)
        logStat("nodes", g2.numNodes.toString, level)
        logStat("internal links", g2.numInternalLinks.toString, level)
        logStat("internal links", g2.numExternalLinks.toString, level)
        logStat("modularity", c.modularity.toString, level)
        modularity = new_modularity
      } while (improvement || level < 4)

      logStat("consolidation", "all complete", 0)
      Iterator(g2)
    }.collect.head

    graph
  }
}
