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


import software.uncharted.graphing.clustering.experiments.partitioning.labelpropagation.RoughGraphPartitioner

import scala.collection.mutable.{Buffer, Map => MutableMap, Set => MutableSet}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import software.uncharted.spark.ExtendedRDDOpertations._
import software.uncharted.graphing.clustering.unithread.reference.{Graph => BGLLGraph}



/**
 * This is based on the reference BGLL Graph_Binary implementation, but modified to allow to:
 *    * Allow control of node processing order
 *    * Keeping node and edge payload information along with the data
 *    * Reasonable handling of saving combinations
 *
 * As such, a subgraph will consist of all the nodes in a single partition, the links within nodes on that partition,
 * and the links to nodes outside that partition
 *
 * Unlike the reference version by USC, we are assuming weighted graphs in all cases.  The weights will be there
 * for all iterations but the first anyway.
 *
 * @param nodes The ID and original data in the complete graph of each node in our subgraph
 * @param links The internal links from each node in our subgraph, with one array of links per node
 * @param remoteLinks The external from each node in our subgraph, with one array of links per node
 */
class SubGraph[VD] (nodes: Array[(VertexId, VD)],
                    links: Array[Array[(Int, Float)]],
                    remoteLinks: Array[Array[(VertexId, Float)]])
 extends Serializable {
  assert(nodes.size == links.size)
  assert(nodes.size == remoteLinks.size)

  // Number of nodes in our subgraph
  val numNodes = nodes.size

  /** Get the original data for the given node */
  def nodeData (node: Int): (VertexId, VD) = nodes(node)

  // A quick function to mutate to our reference implementation for testing
  private[usc] def toReferenceImplementation: BGLLGraph = {
    val degrees: Array[Int] = links.map(_.size)
    for (i <- 1 until degrees.size) degrees(i) = degrees(i) + degrees(i - 1)
    new BGLLGraph(degrees, links.flatMap(_.map(_._1)), Some(links.flatMap(_.map(_._2))))
  }


  val numInternalLinks = links.foldLeft(0)(_ + _.size)

  def numInternalNeighbors (node: Int): Int = links(node).size

  def internalNeighbors (node: Int): Iterator[(Int, Float)] = links(node).toIterator

  def weightedInternalDegree (node: Int): Double =
    internalNeighbors(node).foldLeft(0.0)(_ + _._2.toDouble)

  lazy val totalInternalWeight = calculateTotalInternalWeight
  private def calculateTotalInternalWeight =
    links.foldLeft(0.0)(_ + _.foldLeft(0.0)(_ + _._2.toDouble))



  val numExternalLinks = remoteLinks.foldLeft(0)(_ + _.size)

  def numExternalNeighbors (node: Int): Int = remoteLinks(node).size

  def externalNeighbors (node: Int): Iterator[(Long, Float)] = remoteLinks(node).toIterator

  def weightedExternalDegree (node: Int): Double = {
    externalNeighbors(node).foldLeft(0.0)(_ + _._2.toDouble)
  }
  lazy val totalExternalWeight = calculateTotalExternalWeight
  private def calculateTotalExternalWeight =
    remoteLinks.foldLeft(0.0)(_ + _.foldLeft(0.0)(_ + _._2.toDouble))



  def weightedSelfLoopDegree (node: Int): Double =
    internalNeighbors(node).foldLeft(0.0) { case (sum, addend) =>
      if (node == addend._1) sum + addend._2.toDouble
      else sum
    }



  /** Get the links out of this SubGraph, for use in reconstructing the full graph */
  def toFullGraphNodes: Iterator[(VertexId, VD)] = nodes.iterator

  /** Get the edges out of this SubGraph, for use in reconstruction the full graph */
  def toFullGraphLinks: Iterator[Edge[Float]] = {
    (0 until nodes.size).toIterator.flatMap{src =>
      val srcId: VertexId = nodes(src)._1

      internalNeighbors(src).map{case (dst, weight) =>
        val dstId = nodes(dst)._1
        new Edge[Float](srcId, dstId, weight)
      } ++
      externalNeighbors(src).map{case (dst, weight) =>
        new Edge[Float](srcId, dst, weight)
      }
    }
  }





//  private class InternalNeighborIterator (node: Int) extends Iterator[(Int, Float)] {
//    var index: Int = if (0 == node) 0 else degrees(node-1)
//    val end: Int = degrees(node)
//
//    override def hasNext: Boolean = index < end
//
//    override def next(): (Int, Float) = {
//      val next = links(index)
//      index = index + 1
//      next
//    }
//  }
//  private class ExternalNeighborIterator (node: Int) extends Iterator[(VertexId, Float)] {
//    var index: Int = if (0 == node) 0 else remoteDegrees(node-1)
//    var end: Int = remoteDegrees(node)
//
//    override def hasNext: Boolean = index < end
//
//    override def next(): (VertexId, Float) = {
//      val next = remoteLinks(index)
//      index = index + 1
//      next
//    }
//  }
}



object SubGraph {
  def partitionGraphToSubgraphs[VD, ED] (graph: Graph[VD, ED],
                                         getEdgeWeight: ED => Float,
                                         partitions: Int,
                                         bidirectional: Boolean = true,
                                         randomness: Double = 0.5): RDD[SubGraph[VD]] = {
    def nodesAndEdgesToSubGraph (iV: Iterator[(VertexId, VD)],
                                 numNodes: Int,
                                 iE: Iterator[Edge[ED]]): Iterator[SubGraph[VD]] = {
      // Record our nodes
      val nodes = new Array[(VertexId, VD)](numNodes)
      val nodeIds = MutableMap[VertexId, Int]()
      val knownNodes = MutableSet[VertexId]()
      var i = 0
      iV.foreach{nodeDataPair =>
        nodes(i) = nodeDataPair
        nodeIds(nodeDataPair._1) = i
        knownNodes += nodeDataPair._1
        i = i + 1
      }

      // Record our links, separating out internal and external links, and combining edges to the same destination
      // while we are at it.
      val internalLinksMap = new Array[MutableMap[VertexId, Float]](numNodes)
      val externalLinksMap = new Array[MutableMap[VertexId, Float]](numNodes)
      for (i <- 0 until numNodes) {
        internalLinksMap(i) = MutableMap[VertexId, Float]()
        externalLinksMap(i) = MutableMap[VertexId, Float]()
      }

      iE.foreach { case (edge) =>
        val source = edge.srcId
        val sourceId = nodeIds(source)
        val destination = edge.dstId
        val weight = getEdgeWeight(edge.attr)
        if (knownNodes.contains(destination)) {
          internalLinksMap(sourceId)(destination) = internalLinksMap(sourceId).get(destination).getOrElse(0.0f) + weight
        } else {
          externalLinksMap(sourceId)(destination) = externalLinksMap(sourceId).get(destination).getOrElse(0.0f) + weight
        }
      }

      // Convert link buffers to arrays
      var numILinks = 0
      var numELinks = 0
      val internalLinks = new Array[Array[(Int, Float)]](numNodes)
      val externalLinks = new Array[Array[(VertexId, Float)]](numNodes)
      for (i <- 0 until numNodes) {
        val nodeILinks: MutableMap[VertexId, Float] = internalLinksMap(i)
        val nodeILinksArray = new Array[(Int, Float)](nodeILinks.size)
        var j = 0
        nodeILinks.foreach { case (destination, weight) =>
          nodeILinksArray(j) = (nodeIds(destination), weight)
          j = j + 1
          numILinks = numILinks + 1
        }
        internalLinks(i) = nodeILinksArray

        val nodeELinks = externalLinksMap(i)
        val nodeELinksArray = new Array[(VertexId, Float)](nodeELinks.size)
        j = 0
        nodeELinks.foreach { case (destination, weight) =>
          nodeELinksArray(j) = (destination, weight)
          j = j + 1
          numELinks = numELinks + 1
        }
        externalLinks(i) = nodeELinksArray
      }

      Iterator(new SubGraph(nodes, internalLinks, externalLinks))
    }
    RoughGraphPartitioner.iterateOverPartitions(graph, partitions, bidirectional, randomness)(nodesAndEdgesToSubGraph)
  }

  def graphToSubGraphs[VD, ED] (graph: Graph[VD, ED],
                                getEdgeWeight: ED => Float,
                                partitions: Int)
                               (implicit order: Ordering[(VertexId, VD)]): RDD[SubGraph[VD]] = {
    val sc = graph.vertices.context

    // Order our vertices
    val orderedVertices: RDD[(VertexId, VD)] = graph.vertices.sortBy(v => v)
    orderedVertices.cache

    // Repartition the vertices
    val repartitionedVertices = orderedVertices.repartitionEqually(partitions)

    // Get the vertex boundaries of each partition, so we can repartition the edges the same way
    val partitionBoundaries = repartitionedVertices.mapPartitionsWithIndex { case (partition, elements) =>
      val bounds = elements.map { case (vertexId, vertexData) =>
        (vertexId, vertexId)
      }.reduce((a, b) => (a._1 min b._1, a._2 max b._2))

      Iterator((partition, bounds))
    }.collect.toMap

    // Repartition the edges using these partition boundaries - collect all edges of node X into the partition of the
    // EdgeRDD corresponding to the partition of the VertexRDD containing node X
    val repartitionedEdges = graph.partitionBy(
      new SourcePartitioner(sc.broadcast(partitionBoundaries)),
      repartitionedVertices.partitions.size
    ).edges

    // Combine the two into an RDD of subgraphs, one per partition
    repartitionedVertices.zipPartitions(repartitionedEdges, true) { case (vi, ei) =>
      val nodes = vi.toArray
      val numNodes = nodes.size
      val newIdByOld = MutableMap[Long, Int]()

      // Renumber the nodes (so that the IDs are integers, since the BGLL algorithm doesn't work in scala with Long
      // node ids)
      for (i <- 0 until numNodes) {
        newIdByOld(nodes(i)._1) = i
      }

      // Separate edges into internal and external edges
      val internalLinks = new Array[Buffer[(Int, Float)]](numNodes)
      val externalLinks = new Array[Buffer[(VertexId, Float)]](numNodes)
      for (i <- 0 until numNodes) {
        internalLinks(i) = Buffer[(Int, Float)]()
        externalLinks(i) = Buffer[(VertexId, Float)]()
      }

      ei.foreach{edge =>
        val srcIdOric = edge.srcId
        val srcId = newIdByOld(srcIdOric)
        val dstIdOrig = edge.dstId
        if (newIdByOld.contains(dstIdOrig)) {
          internalLinks(srcId) += ((newIdByOld(dstIdOrig), getEdgeWeight(edge.attr)))
        } else {
          externalLinks(srcId) += ((dstIdOrig, getEdgeWeight(edge.attr)))
        }
      }

      Iterator(new SubGraph(nodes, internalLinks.map(_.toArray), externalLinks.map(_.toArray)))
    }
  }
}

class SourcePartitioner (boundaries: Broadcast[Map[Int, (Long, Long)]]) extends PartitionStrategy {
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    boundaries.value.filter { case (partition, (minVertex, maxVertex)) =>
      minVertex <= src && src <= maxVertex
    }.head._1
  }
}
