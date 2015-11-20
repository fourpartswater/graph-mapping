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
import software.uncharted.graphing.clustering.reference



/**
 * This is based on the reference BGLL Graph_Binary implementation, but modified to allow to:
 *    * Allow control of node processing order
 *    * Keeping node and edge payload information along with the data
 *    * Reasonable handling of saving combinations
 *
 * As such, a subgraph will consist of all the nodes in a single partition, the links within nodes on that partition,
 * and the links to nodes outside that partition
 *
 * @param nodes The ID and original data in the complete graph of each node in our subgraph
 * @param degrees The cumulative degree of each node in our subgraph (divided into internal and external degree)
 * @param links The internal and external links of each node in our subgraph.  Internal links reference the internal
 *              node ID of their destination(i.e., order within this subgraph).  External links use the original
 *              node ID of their destination in the full graph.
 * @param weightsOpt An optional pair of lists of the weights of all the internal and external links.  The lengths
 *                   of these lists must, of course, match the lengths of the lists that comprise the links
 *                   parameter.
 */
class SubGraph[VD] (nodes: Array[(VertexId, VD)],
                    degrees: Array[(Int, Int)],
                    links: (Array[Int], Array[VertexId]),
                    weightsOpt: Option[(Array[Float], Array[Float])] = None) extends Serializable {
  assert(nodes.size == degrees.size)
  assert(links._1.size == degrees(degrees.length-1)._1)
  assert(links._2.size == degrees(degrees.length-1)._2)
  weightsOpt.foreach { weights =>
    assert(links._1.size == weights._1.size)
    assert(links._2.size == weights._2.size)
  }

  // Number of nodes in our subgraph
  val numNodes = degrees.size
  // The number of links in our subgraph (Internal, external, and total)
  val numLinks = (links._1.size, links._2.size, links._1.size + links._2.size)
  // The total weight of our subgraph (Again, internal, external, and total)
  lazy val totalWeight = calculateTotalWeight

  /** Get the original data for the given node */
  def nodeData (node: Int): (VertexId, VD) = nodes(node)

  // A quick function to mutate to our reference implementation for testing
  private[usc] def toReferenceImplementation: reference.Graph = {
    new reference.Graph(degrees.map(_._1), links._1, weightsOpt.map(_._1))
  }

  def numInternalNeighbors (node: Int): Int =
    if (0 == node) {
      degrees(0)._1
    } else {
      degrees(node)._1 - degrees(node - 1)._1
    }



  def internalNeighbors (node: Int): Iterator[(Int, Float)] =
    new InternalNeighborIterator(node)

  def weightedInternalDegree (node: Int): Double =
    weightsOpt.map(weights =>
      internalNeighbors(node).map(_._2.toDouble).fold(0.0)(_ + _)
    ).getOrElse(numInternalNeighbors(node).toDouble)

  def numExternalNeighbors (node: Int): Int =
    if (0 == node) {
      degrees(0)._2
    } else {
      degrees(node)._2 - degrees(node - 1)._2
    }



  def externalNeighbors (node: Int): Iterator[(Long, Float)] =
    new ExternalNeighborIterator(node)

  def weightedExternalDegree (node: Int): Double =
    weightsOpt.map(weights =>
      externalNeighbors(node).map(_._2.toDouble).fold(0.0)(_ + _)
    ).getOrElse(numExternalNeighbors(node).toDouble)

  def weightedSelfLoopDegree (node: Int): Double =
    internalNeighbors(node).filter(_._1 == node).map(_._2).fold(0.0f)(_ + _)



  def weightedDegree (node: Int): Double =
    weightedInternalDegree(node) + weightedExternalDegree(node)



  private def calculateTotalWeight = {
    val (totalInternal, totalExternal) = weightsOpt.map { case (internal, external) =>
      def total(array: Array[Float]): Double = {
        // We do this with a loop rather than a fold because we want the result as a double, without having to copy the
        // whole array to doubles.
        var sum = 0.0
        for (i <- 0 until array.size) sum += array(i)
        sum
      }
      (total(internal), total(external))
    }.getOrElse {
      (links._1.size.toDouble, links._2.size.toDouble)
    }
    (totalInternal, totalExternal, totalInternal + totalExternal)
  }

  /** Get the links out of this SubGraph, for use in reconstructing the full graph */
  def toFullGraphNodes: Iterator[(VertexId, VD)] = nodes.iterator

  /** Get the edges out of this SubGraph, for use in reconstruction the full graph */
  def toFullGraphLinks: Iterator[Edge[Float]] = {
    val (internalLinks, externalLinks) = weightsOpt.map(weights =>
      (links._1 zip weights._1, links._2 zip weights._2)
    ).getOrElse(links._1.map(dst => (dst, 1.0f)), links._2.map(dst => (dst, 1.0f)))

    (0 until nodes.size).toIterator.flatMap{src =>
      val srcId: VertexId = nodes(src)._1
      val startLinks: (Int, Int) = if (0 == src) (0, 0) else degrees(src-1)
      val endLinks: (Int, Int) = degrees(src)
      (startLinks._1 until endLinks._1).map { i =>
        val dst = links._1(i)
        val dstId = nodes(dst)._1
        val weight = weightsOpt.map(_._1(i)).getOrElse(1.0f)
        new Edge[Float](srcId, dstId, weight)
      } union(startLinks._2 until endLinks._2).map { i =>
        val dstId = links._2(i)
        val weight = weightsOpt.map(_._2(i)).getOrElse(1.0f)
        new Edge[Float](srcId, dstId, weight)
      }
    }
  }





  private class InternalNeighborIterator (node: Int) extends Iterator[(Int, Float)] {
    var index: Int = if (0 == node) 0 else degrees(node-1)._1
    val end: Int = degrees(node)._1

    override def hasNext: Boolean = index < end

    override def next(): (Int, Float) = {
      val nextLink: Int = links._1(index)
      val nextWeight: Float = weightsOpt.map(_._1(index)).getOrElse(1.0f)
      index = index + 1
      (nextLink, nextWeight)
    }
  }
  private class ExternalNeighborIterator (node: Int) extends Iterator[(Long, Float)] {
    var index: Int = if (0 == node) 0 else degrees(node-1)._2
    var end: Int = degrees(node)._2

    override def hasNext: Boolean = index < end

    override def next(): (Long, Float) = {
      val nextLink: Long = links._2(index)
      val nextWeight: Float = weightsOpt.map(_._2(index)).getOrElse(1.0f)
      index = index + 1
      (nextLink, nextWeight)
    }
  }
}



object SubGraph {
  def graphToSubGraphs[VD, ED] (graph: Graph[VD, ED],
                                getEdgeWeight: Option[ED => Float],
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
      val internalLinks = new Array[Buffer[Int]](numNodes)
      val externalLinks = new Array[Buffer[VertexId]](numNodes)
      val weightsOpt = getEdgeWeight.map(fcn => (new Array[Buffer[Float]](numNodes), new Array[Buffer[Float]](numNodes)))
      for (i <- 0 until numNodes) {
        internalLinks(i) = Buffer[Int]()
        externalLinks(i) = Buffer[VertexId]()
        weightsOpt.foreach{case (internalWeights, externalWeights) =>
            internalWeights(i) = Buffer[Float]()
            externalWeights(i) = Buffer[Float]()
        }
      }

      ei.foreach{edge =>
        val srcIdOric = edge.srcId
        val srcId = newIdByOld(srcIdOric)
        val dstIdOrig = edge.dstId
        if (newIdByOld.contains(dstIdOrig)) {
          internalLinks(srcId) += newIdByOld(dstIdOrig)
          getEdgeWeight.map { fcn =>
            (weightsOpt.get._1)(srcId) += fcn(edge.attr)
          }
        } else {
          externalLinks(srcId) += dstIdOrig
          getEdgeWeight.map { fcn =>
            (weightsOpt.get._2)(srcId) += fcn(edge.attr)
          }
        }
      }

      // Get our cumulative degree arrays
      val degrees: Array[(Int, Int)] = new Array[(Int, Int)](numNodes)
      var totalInternalDegree = 0
      var totalExternalDegree = 0
      for (i <- 0 until numNodes) {
        totalInternalDegree = totalInternalDegree + internalLinks(i).size
        totalExternalDegree = totalExternalDegree + externalLinks(i).size
        degrees(i) = (totalInternalDegree, totalExternalDegree)
      }

      // Put our links into our needed array form
      val allInternalLinks: Array[Int] = internalLinks.flatMap(linksForNode => linksForNode).toArray
      val allExternalLinks: Array[VertexId] = externalLinks.flatMap(linksForNode => linksForNode).toArray

      // Put our weights into our needed array form
      val allWeights: Option[(Array[Float], Array[Float])] = weightsOpt.map { case (internalWeights, externalWeights) =>
        (
          internalWeights.flatMap(weightsForNode => weightsForNode).toArray,
          externalWeights.flatMap(weightsForNode => weightsForNode).toArray
          )
      }

      Iterator(new SubGraph(nodes, degrees, (allInternalLinks, allExternalLinks), allWeights))
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
