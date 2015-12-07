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



import scala.collection.mutable.{Buffer, Map => MutableMap}

import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD

import software.uncharted.spark.ExtendedRDDOpertations._
import software.uncharted.graphing.clustering.reference.{Graph => BGLLGraph}



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
 * @param degrees The cumulative internal degree of each node in our subgraph
 * @param links The internal links from each node in our subgraph
 * @param remoteDegrees The cumulative internal degree of each node in our subgraph
 * @param remoteLinks The external from each node in our subgraph
 */
class SubGraph[VD] (nodes: Array[(VertexId, VD)],
                    degrees: Array[Int],
                    links: Array[(Int, Float)],
                    remoteDegrees: Array[Int],
                    remoteLinks: Array[(VertexId, Float)])
 extends Serializable {
  assert(nodes.size == degrees.size)
  assert(nodes.size == remoteDegrees.size)
  assert(links.size == degrees(degrees.length - 1))
  assert(remoteLinks.size == remoteDegrees(remoteDegrees.length - 1))

  // Number of nodes in our subgraph
  val numNodes = degrees.size

  /** Get the original data for the given node */
  def nodeData (node: Int): (VertexId, VD) = nodes(node)

  // A quick function to mutate to our reference implementation for testing
  private[usc] def toReferenceImplementation: BGLLGraph = {
    new BGLLGraph(degrees, links.map(_._1), Some(links.map(_._2)))
  }


  val numInternalLinks = links.size

  def numInternalNeighbors (node: Int): Int =
    if (0 == node) {
      degrees(0)
    } else {
      degrees(node) - degrees(node - 1)
    }

  def internalNeighbors (node: Int): Iterator[(Int, Float)] =
    new InternalNeighborIterator(node)

  def weightedInternalDegree (node: Int): Double =
    internalNeighbors(node).foldLeft(0.0)(_ + _._2.toDouble)

  lazy val totalInternalWeight = calculateTotalInternalWeight
  private def calculateTotalInternalWeight =
    links.foldLeft(0.0)(_ + _._2.toDouble)



  val numExternalLinks = remoteLinks.size

  def numExternalNeighbors (node: Int): Int =
    if (0 == node) {
      remoteDegrees(0)
    } else {
      remoteDegrees(node) - remoteDegrees(node - 1)
    }

  def externalNeighbors (node: Int): Iterator[(Long, Float)] =
    new ExternalNeighborIterator(node)

  def weightedExternalDegree (node: Int): Double = {
    externalNeighbors(node).foldLeft(0.0)(_ + _._2.toDouble)
  }
  lazy val totalExternalWeight = calculateTotalExternalWeight
  private def calculateTotalExternalWeight =
    remoteLinks.foldLeft(0.0)(_ + _._2.toDouble)



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





  private class InternalNeighborIterator (node: Int) extends Iterator[(Int, Float)] {
    var index: Int = if (0 == node) 0 else degrees(node-1)
    val end: Int = degrees(node)

    override def hasNext: Boolean = index < end

    override def next(): (Int, Float) = {
      val next = links(index)
      index = index + 1
      next
    }
  }
  private class ExternalNeighborIterator (node: Int) extends Iterator[(VertexId, Float)] {
    var index: Int = if (0 == node) 0 else remoteDegrees(node-1)
    var end: Int = remoteDegrees(node)

    override def hasNext: Boolean = index < end

    override def next(): (VertexId, Float) = {
      val next = remoteLinks(index)
      index = index + 1
      next
    }
  }
}



object SubGraph {
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

      // Get our cumulative degree arrays
      val internalDegrees: Array[Int] = new Array[Int](numNodes)
      val externalDegrees: Array[Int] = new Array[Int](numNodes)
      var totalInternalDegree = 0
      var totalExternalDegree = 0
      for (i <- 0 until numNodes) {
        totalInternalDegree = totalInternalDegree + internalLinks(i).size
        totalExternalDegree = totalExternalDegree + externalLinks(i).size
        internalDegrees(i) = totalInternalDegree
        externalDegrees(i) = totalExternalDegree
      }
      val allInternalLinks: Array[(Int, Float)] = new Array[(Int, Float)](totalInternalDegree)
      var in = 0
      val allExternalLinks: Array[(VertexId, Float)] = new Array[(VertexId, Float)](totalExternalDegree)
      var en = 0
      for (i <- 0 until numNodes) {
        internalLinks(i).map { internalLink =>
          allInternalLinks(in) = internalLink
          in = in + 1
        }
        externalLinks(i).map { externalLink =>
          allExternalLinks(en) = externalLink
          en = en + 1
        }
      }

      Iterator(new SubGraph(nodes, internalDegrees, allInternalLinks, externalDegrees, allExternalLinks))
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
