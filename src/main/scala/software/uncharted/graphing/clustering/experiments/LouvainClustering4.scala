/**
  * Copyright (c) 2014-2017 Uncharted Software Inc. All rights reserved.
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
package software.uncharted.graphing.clustering.experiments



import org.apache.spark.graphx._ //scalastyle:ignore
import org.apache.spark.rdd.RDD
import software.uncharted.graphing.clustering.utilities.ClusterConsolidator
import software.uncharted.graphing.utilities.EdgeCalculation

import scala.reflect.ClassTag



class LouvainClustering4 {

  def runLouvainClustering[VD: ClassTag](graph: Graph[VD, Double],
                                         iterations: Int):
  (Graph[VD, Double], RDD[(VertexId, VertexId)]) = {
    val m = totalWeight(graph)

    var clustersOpt: Option[RDD[(VertexId, VertexId)]] = None
    var presentGraph = graph
    for (n <- 1 to iterations) {
      val (newGraph, newClusters) = iterate(graph, m)
      presentGraph = newGraph
      clustersOpt = Some(
        clustersOpt.map { oldClusters =>
          // TODO: Combine old and new movements into one map
          // If old = (5 -> 4, 6 -> 4, 8 -> 10, 9 -> 1), and new = (4 -> 7, 10 -> 12, 11 -> 12)
          // we want combined = (4 -> 7, 5 -> 7, 6 -> 7, 8 -> 12, 9 -> 1, 10 -> 12, 11 -> 12)
          //
          // To get there, we need:
          //
          //  old     new       result
          //  4 <- 5  4 -> 7    5 -> 7
          //  4 <- 6  4 -> 7    6 -> 7
          //  10 <- 8 10 -> 12  8 -> 12
          //  1 <- 9            1 -> 9
          //          11 -> 12
          //
          // And take result, and union it with new
          oldClusters.map { case (start, end) =>
            (end, start)
          }.leftOuterJoin(newClusters).map { case (oldEnd, (start, newEndOption)) =>
              (start, newEndOption.getOrElse(oldEnd))
          }.union(newClusters)
        }.getOrElse(newClusters)
      )
    }

    (presentGraph, clustersOpt.getOrElse{
       val sc = graph.vertices.context
       sc.parallelize(List[(VertexId, VertexId)]())
     })
  }

  def totalWeight(graph: Graph[_, Double]): Double =
    graph.edges.map(_.attr).reduce(_ + _)

  /**
   * Take a graph and reduce it, returning the new graph, and an RDD of the mapping from old to new node IDs
   *
   * The input graph must have been prepared for Louvain clustering using initializeGraph
   */
  def iterate[VD: ClassTag](graph: Graph[VD, Double],
                            totalWeight: Double): (Graph[VD, Double], RDD[(VertexId, VertexId)]) = {
    // calculate weights, both interior and exterior
    val graphWithWeights = addWeights(graph)
    // For the moment, in all the below, we assume all links to be bi-directional
    // We start by this iteration assuming each node is in its own community
    val movingNodes = getNewCommunityForVertices(graphWithWeights, totalWeight * 2.0)

    val joinNodeDataFcn: (VertexId, VD, Option[VertexId]) => (VD, VertexId) = (vid, data, moveOption) =>
    (data, moveOption.getOrElse(vid))

    val graphWithMoveinfo: Graph[(VD, VertexId), Double] = graph.outerJoinVertices(movingNodes)(joinNodeDataFcn)

    val getNewNodeFcn: (VertexId, (VD, VertexId)) => VertexId = (vid, data) =>
      data._2
    val getDataFcn: (VertexId, (VD, VertexId)) => VD = (vid, data) =>
      data._1
    val gmiv = graphWithMoveinfo.vertices.collect
    val gmie = graphWithMoveinfo.edges.collect
    println("Consolidating:")
    println("Nodes:")
    gmiv.foreach(println)
    println
    println("Edges:")
    gmie.foreach(println)

    // Consolidate nodes brought into the same cluster into one node
    val reducedGraph: Graph[(VD, VertexId), Double] =
      ClusterConsolidator.consolidate[(VD, VertexId), Double](graphWithMoveinfo, getNewNodeFcn)

    // And group edges
    val consolidatedEdges = reducedGraph.groupEdges(_ + _)
    // And remove extra weight information and target group information
    val reduced: Graph[VD, Double] = consolidatedEdges.mapVertices { case (oldVid, (data, newVid)) =>
      data
    }
    (
      // ... and   group edges,      and remove extraneous weight and target group information
      reducedGraph.groupEdges(_ + _).mapVertices[VD](getDataFcn),
      graphWithMoveinfo.vertices.map { case (oldVid, (data, newVid)) => (oldVid, newVid) }
    )
  }

  def addWeights[VD: ClassTag] (graph: Graph[VD, Double]): Graph[(VD, DegreesAndWeights), Double] = {
    LouvainInitializer.calculateAugmentedVertexInfo((d: Double) => d)(graph)
  }

  private def getNewCommunityForVertices[VD] (graph: Graph[(VD, DegreesAndWeights), Double], m2: Double): VertexRDD[VertexId] = {
//    val vertices: RDD[(VertexId, VertexId, Double)] = graph.triplets.filter(triplet =>
    val a = graph.triplets.filter(triplet => triplet.srcId != triplet.dstId )
    val b = a.flatMap { triplet =>
      Seq(((triplet.srcId, triplet.srcAttr, triplet.dstId), (triplet.attr, 1L)),
          ((triplet.dstId, triplet.dstAttr, triplet.srcId), (triplet.attr, 1L)))
    }
    val c = b.reduceByKey { (a, b) => (a._1 + b._1, a._2 + b._2) }
    val d = c.flatMap { case ((srcId, srcV, destC), (k_i_in, k_c_i)) =>
        val sigma_in = srcV._2.wIn
        val sigma_tot = srcV._2.wTot
        val k_i = srcV._2.dIn
        // A simple reduction of the formula from the wiki page on Louvain Clustering
        val dQ = k_i_in - sigma_tot * k_i / m2
        if (dQ > 0.0) Some(((srcId, srcV), (destC, dQ, k_i_in, k_c_i))) else None
    }
    val e = d.reduceByKey { (a, b) => if (a._2 > b._2) a else b }
    val f = e.map { case ((srcId, srcV), (destC, dQ, totalLinkWeight, totalLinks)) => (srcId, destC, dQ) }

    val ic = graph.triplets.collect.map(_.toString).sorted
    val ac = a.collect.map(_.toString).sorted
    val bc = b.collect.map(_.toString).sorted
    val cc = c.collect.map(_.toString).sorted
    val dc = d.collect.map(_.toString).sorted
    val ec = e.collect.map(_.toString).sorted
    val fc = f.collect.map(_.toString).sorted

    println("Input:")
    ic.foreach(icm => println("\t" + icm))
    println("Stage A:")
    ac.foreach(acm => println("\t" + acm))
    println("Stage B:")
    bc.foreach(bcm => println("\t" + bcm))
    println("Stage C:")
    cc.foreach(ccm => println("\t" + ccm))
    println("Stage D:")
    dc.foreach(dcm => println("\t" + dcm))
    println("Stage E:")
    ec.foreach(ecm => println("\t" + ecm))
    println("Stage F:")
    fc.foreach(fcm => println("\t" + fcm))

    val vertices = f.map{case (s, d, w) => (s, d)}
    VertexRDD(vertices, graph.edges, -1L)
  }
}

case class DegreesAndWeights (dIn: Int, dTot: Int, wIn: Double, wTot: Double) {
  //scalastyle:off method.name
  def + (that: DegreesAndWeights): DegreesAndWeights =
    DegreesAndWeights(this.dIn + that.dIn, this.dTot + that.dTot, this.wIn + that.wIn, this.wTot + that.wTot)
  //scalastyle:on method.name
}
object DegreesAndWeights {
  final val default = DegreesAndWeights(0, 0, 0.0, 0.0)
}


/**
 * Initialize a graph with degree and weight information by node, both interior and total (where interior is defined
 * by self-links)
 */
object LouvainInitializer extends EdgeCalculation[Double] {
  type Data = DegreesAndWeights
  val dct= implicitly[ClassTag[Data]]
  val defaultData = DegreesAndWeights.default

  def getEdgeInfo(context: EdgeContext[Long, Double, Data]): Option[(Option[DegreesAndWeights], Option[DegreesAndWeights])] = {
    if (context.srcId == context.dstId) {
      Some(Some(DegreesAndWeights(2, 2, 2.0*context.attr, 2.0*context.attr)), None)
    } else {
      Some(Some(DegreesAndWeights(0, 1, 0.0, context.attr)), Some(DegreesAndWeights(0, 1, 0.0, context.attr)))
    }
  }

  def mergeEdgeInfo (a: Data, b: Data): Data = a + b
}

