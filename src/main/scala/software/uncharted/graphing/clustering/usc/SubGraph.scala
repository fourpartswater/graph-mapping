package software.uncharted.graphing.clustering.usc


import scala.reflect.ClassTag
import scala.collection.mutable.{Buffer, Map => MutableMap}

import org.apache.spark.Partitioner
import org.apache.spark.broadcast.Broadcast
import org.apache.spark.graphx.{PartitionID, Graph, PartitionStrategy, VertexId}
import org.apache.spark.rdd.RDD


/**
 * This is based on the reference BGLL Graph_Binary implementation, but modified to allow to:
 *    * Allow control of node processing order
 *    * Keeping node and edge payload information along with the data
 *    * Reasonable handling of saving combinations
 *
 * As such, a subgraph will consist of all the nodes in a single partition, the links within nodes on that partition,
 * and the links to nodes outside that partition
 *
 * @param nodeIds The ID in the complete graph of each node in our subgraph
 * @param degrees The degree of each node in our subgraph
 */
class SubGraph (nodeIds: Array[VertexId],
                degrees: Array[(Int, Int)],
                links: (Array[Int], Array[VertexId]),
                weightsOpt: Option[(Array[Float], Array[Float])] = None) {
  assert(nodeIds.size == degrees.size)
  assert(links._1.size == links._2.size)
  weightsOpt.foreach { weights =>
    assert(weights._1.size == weights._2.size)
    assert(links._1.size == weights._1.size)
  }

  // Number of nodes in our subgraph
  val numNodes = degrees.size
  // The number of links in our subgraph (Internal, external, and total)
  val numLinks = (links._1.size, links._2.size, links._1.size + links._2.size)
  // The total weight of our subgraph (Again, internal, external, and total)
  lazy val totalWeight = calculateTotalWeight

  def internalNeighbors (node: Int): Iterator[(Int, Float)] =
    new InternalNeighborIterator(node)

  def numInternalNeighbors (node: Int): Int =
    if (0 == node) {
      degrees(0)._1
    } else {
      degrees(node)._1 - degrees(node - 1)._1
    }

  def weightedInternalDegree (node: Int): Double =
    weightsOpt.map { case (internal, external) =>
      internalNeighbors(node).map(_._2.toDouble).fold(0.0)(_ + _)
    }.getOrElse(numInternalNeighbors(node).toDouble)

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
}
object SubGraph {
  def graphToSubGraphs[VD, ED] (graph: Graph[VD, ED],
                                partitions: Int)
                               (implicit order: Ordering[(VertexId, VD)]): RDD[SubGraph] = {
    val sc = graph.vertices.context

    // Order our vertices
    val orderedVertices: RDD[(VertexId, VD)] = graph.vertices.sortBy(v => v)
    orderedVertices.cache

    // Repartition the vertices
    val repartitionedVertices = repartitionEqually(orderedVertices, partitions)

    // Get the vertex boundaries of each partition, so we can repartition the edges the same way
    val partitionBoundaries = repartitionedVertices.mapPartitionsWithIndex { case (partition, elements) =>
      val bounds = elements.map { case (vertexId, vertexData) =>
        (vertexId, vertexId)
      }.reduce((a, b) => (a._1 min b._1, a._2 max b._2))

      Iterator((partition, bounds))
    }.collect.toMap

    // Repartition the edges using these partition boundaries - collect all edges of node X into the partition of the
    // EdgeRDD corresponding to the partition of the VertexRDD containing node X
    val repartitionedEdges = graph.partitionBy(new SourcePartitioner(sc.broadcast(partitionBoundaries))).edges

    // Combine the two into an RDD of subgraphs, one per partition
    repartitionedVertices.zipPartitions(repartitionedEdges, true) { case (vi, ei) =>
      val vertices = vi.toArray
      val numNodes = vertices.size
      val nodeIds = new Array[VertexId](numNodes)
      val degrees = new Array[(Int, Int)](numNodes)
      val newIdByOld = MutableMap[Long, Int]()

      // Renumber the nodes (so that the IDs are integers, since the BGLL algorithm doesn't work in scala with Long
      // node ids)
      for (i <- 0 until numNodes) {
        nodeIds(i) = vertices(i)._1
        newIdByOld(vertices(i)._1) = i
      }

      // Copy and move over edges, separating them out as internal and external
      val internalLinks = Buffer[Int]()
      val externalLinks = Buffer[Long]()

      // Collect edges by link, in order, and insert into links and weights arrays.

      //      class SubGraph (nodeIds: Array[VertexId],
      //                      degrees: Array[(Int, Int)],
      //                      links: (Array[Int], Array[VertexId]),
      //                      weightsOpt: Option[(Array[Float], Array[Float])] = None) {

      null
    }

    null
  }

  def repartitionEqually[T: ClassTag] (rdd: RDD[T], partitions: Int): RDD[T] = {
    // Figure out the points at which to best cut our vertex list into the given number of partitions
    val partitionSizes = rdd.mapPartitionsWithIndex{case (partition, iter) =>
      Iterator((partition, iter.size))
    }.collect.toMap
    val totalSize = partitionSizes.values.fold(0)(_ + _)
    val cutIndices = (0 until partitions).map(n => (totalSize.toDouble*n.toDouble/partitions.toDouble).floor.toLong).toArray

    // Figure out, for each current partition, the partitions and indices within it at which to cut
    var soFar = 0L
    val cutPoints = (0 until partitionSizes.size).map { currentPartition =>
      val start: Long = (0 until currentPartition).map(p => partitionSizes(p).toLong).fold(0L)(_ + _)
      val end: Long = start + partitionSizes(currentPartition)

      val newPartitions = (0 until cutIndices.size).filter{n =>
        val nS: Long = cutIndices(n)
        val nE: Long = if (n < (cutIndices.size-1)) cutIndices(n+1) else Long.MaxValue
        // We want segments with an endpoint inside our range, or surrounding our range
        ((start <= nS && nS < end) ||
          (start <= nE && nE < end) ||
          (nS <= start && end <= nE))
      }.map { newPartition =>
        val targetPartitionStart = cutIndices(newPartition)
        val targetPartitionEnd = if (newPartition == partitions-1) totalSize else cutIndices(newPartition+1)
        (newPartition, (targetPartitionStart - start).toInt, (targetPartitionEnd - start).toInt)
      }
      (currentPartition, newPartitions)
    }.toMap
    val cutPointsB = rdd.context.broadcast(cutPoints)

    // Label our points by target partition
    val labelledRDD = rdd.mapPartitionsWithIndex { case (partition, pIter) =>
      val cutPointsForPartition = cutPointsB.value(partition)
      pIter.zipWithIndex.map { case (datum, index) =>
        val targetPartition = cutPointsForPartition.filter { case (p, start, end) =>
          start <= index && index < end
        }.head._1
        (targetPartition, datum)
      }
    }

    // Repartition using the stored value...
    val repartitionedRDD = labelledRDD.partitionBy(new KeyPartitioner(partitions))

    /// .. And, finally, remove the stored value
    repartitionedRDD.map{case (partition, datum) => datum}
  }

}

/**
 * A partitioner that assumes an RDD[(Key, Value)], where the key is the partition into which each record should be
 * placed.
 *
 * @param size The number of partitions expected.  If any key is not in the range [0, size), behavior of this
 *             partitioner is underfined.
 */
class KeyPartitioner(size: Int) extends Partitioner {
  override def numPartitions: Int = size

  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}


class SourcePartitioner (boundaries: Broadcast[Map[Int, (Long, Long)]]) extends PartitionStrategy {
  override def getPartition(src: VertexId, dst: VertexId, numParts: PartitionID): PartitionID = {
    boundaries.value.filter { case (partition, (minVertex, maxVertex)) =>
      minVertex <= src && src <= maxVertex
    }.head._1
  }
}
