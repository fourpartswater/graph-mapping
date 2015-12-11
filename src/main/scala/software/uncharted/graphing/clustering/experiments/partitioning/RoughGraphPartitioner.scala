package software.uncharted.graphing.clustering.experiments.partitioning


import org.apache.spark.broadcast.Broadcast

import scala.collection.mutable.Buffer
import scala.collection.mutable.{Map => MutableMap}

import org.apache.spark.graphx._
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD



/**
 * Created by nkronenfeld on 12/9/2015.
 */
object RoughGraphPartitioner {
  def annotateGraphWithDegree[VD, ED] (graph: Graph[VD, ED]): Graph[(VD, Long), ED] = {
    val setDegree: EdgeContext[VD, ED, Long] => Unit = context => {
      context.sendToSrc(1L)
      context.sendToDst(1L)
    }
    val mergeDegree: (Long, Long) => Long = (a, b) => a + b

    val annotate: (VertexId, VD, Option[Long]) => (VD, Long) = (node, data, degreeOpt) =>
      degreeOpt.map(degree => (data, degree)).getOrElse((data, 0L))

    graph.outerJoinVertices(graph.aggregateMessages(setDegree, mergeDegree, TripletFields.None))(annotate)
  }
  def getDegreeDistribution[VD, ED] (graph: Graph[VD, ED]): Seq[(Long, Int)] = {
    getDegreeDistributionInternal(annotateGraphWithDegree(graph).vertices)
  }
  private def getDegreeDistributionInternal[VD] (vertices: VertexRDD[(VD, Long)]): Seq[(Long, Int)] = {
    vertices.map{case (id, (data, degree)) => (degree, 1)}.reduceByKey(_ + _).collect.sortBy(-_._1)
  }

  // Given a graph, find those most connected nodes that can be used as partition indicators
  def getPartitionIndicators[VD, ED] (graph: Graph[VD, ED], partitions: Int): (Map[VertexId, (Int, Long)], Map[Int, Long]) = {
    val linkCount = graph.edges.count
    val graphWithDegree = annotateGraphWithDegree(graph)
    val vertices = graphWithDegree.vertices
    vertices.cache

    val nodeCount = vertices.count
    val proportion = 2.0

    val degreeDistribution: Seq[(Long, Int)] = getDegreeDistributionInternal(vertices)

    var totalLinks = 0L
    var minNeededDegree = Long.MaxValue
    var neededNodes = 0
    degreeDistribution.foreach{case(degree, count) =>
      if (totalLinks < nodeCount * proportion) {
        totalLinks = totalLinks + degree * count
        neededNodes = neededNodes + count
        if (totalLinks >= nodeCount * proportion)
          minNeededDegree = degree
      }
    }

    val keyVertices = vertices.filter(_._2._2 >= minNeededDegree).map{case (id, (data, degree)) => (id, degree)}.collect()

    vertices.unpersist(false)

    val target = nodeCount * proportion / (partitions-1)
    var soFar = 0L
    var partition = 0
    val partitionIndicators = MutableMap[VertexId, (Int, Long)]()
    val partitionSizes = MutableMap[Int, Long]()
    keyVertices.foreach { case (id, degree) =>
      partitionIndicators(id) = (partition, degree)
      partitionSizes(partition) = partitionSizes.get(partition).getOrElse(0L) + degree
      soFar = soFar + degree

      if (soFar > target) {
        soFar = 0L
        partition = partition + 1
      }
    }
    println("Total partitions: "+partition)

    (partitionIndicators.toMap, partitionSizes.toMap)
  }

  /**
   * Annotate the nodes of a graph with the partition into which they should fall
   * @param graph The graph to partition
   * @param partitions A rough number of partitions into which to divide the graph.  This is an approximate input;
   *                   the actual number of partitions may vary slightly.
   * @param weightFcn A function to get the weight of a link.  Default is to assume every link has weight 1.0.  The
   *                  weight is used when determining the best partition in which to place a node - the weight of
   *                  links to progenitor nodes for that partition is taken into account.
   * @return A graph with the partition for each noded added to the node data, and the number of partitions
   */
  def annotateGraphWithPartition[VD, ED] (graph: Graph[VD, ED], partitions: Int, weightFcn: ED => Float = (edgeAttr: ED) => 1.0f): Graph[(VD, Int), ED] = {
    val (partitionIndicators, partitionSizes) = getPartitionIndicators(graph, partitions)
    val maxPartition = partitionIndicators.map(_._2._1).reduce(_ max _)
    // A partition for nodes that aren't linked to any indicated node
    val otherPartition = maxPartition + 1

    println("Partition sizes (in total degree of indicators):")
    partitionSizes.toList.sortBy(_._1).foreach(x => println("\t"+x))
    println("Other partition: "+otherPartition)

    val checkPartitionIndicators: EdgeContext[VD, ED, MutableMap[Int, Float]] => Unit = context => {
      partitionIndicators.get(context.srcId).foreach { case (partition, degree) =>
        context.sendToDst(MutableMap(partition -> degree * weightFcn(context.attr)))
      }
      partitionIndicators.get(context.dstId).foreach { case (partition, degree) =>
        context.sendToSrc(MutableMap(partition -> degree * weightFcn(context.attr)))
      }
    }
    val mergePartitionIndicators: (MutableMap[Int, Float], MutableMap[Int, Float]) => MutableMap[Int, Float] = (a, b) => {
      val mergedMap = a
      b.foreach{case (k, v) => mergedMap(k) = mergedMap.get(k).getOrElse(0.0f) + v}
      mergedMap
    }

    val annotate: (VertexId, VD, Option[MutableMap[Int, Float]]) => (VD, Int) = (node, data, partitionsOpt) => {
      partitionsOpt.map { partitions =>
        val proportionalPartitions = partitions.map { case (partition, count) => (partition, count.toDouble / partitionSizes(partition)) }
        val (bestPartition, bestProportionalPartition) = proportionalPartitions.reduce((a, b) =>
          if (a._2 > b._2) a else b
        )
        (data, bestPartition)
      }.getOrElse(data, otherPartition)
    }

    (
      graph.outerJoinVertices(graph.aggregateMessages(checkPartitionIndicators, mergePartitionIndicators, TripletFields.None))(annotate),
      otherPartition + 1
      )
  }

  def annotateGraphWithRandomPartition[VD, ED] (graph: Graph[VD, ED], partitions: Int): Graph[(VD, Int), ED] = {
    graph.mapVertices{case (id, data) =>
      val partition: Int = (math.random * partitions).floor.toInt
      (data, partition)
    }
  }

  def countLinks[VD, ED] (graph: Graph[(VD, Int), ED]): Int = {
    val partitionEdgeStats = graph.triplets.map { triplet =>
      val srcPartition = triplet.srcAttr._2
      val dstPartition = triplet.dstAttr._2
      if (srcPartition == dstPartition) MutableMap(srcPartition -> (2, 0))
      else MutableMap(srcPartition -> (0, 1), dstPartition -> (0, 1))
    }.reduce { (a, b) =>
      b.foreach { case (k, bv) =>
        val (bint, bext) = bv
        val (aint, aext) = a.get(k).getOrElse((0, 0))
        a(k) = (aint + bint, aext + bext
          )
      }
      a
    }.toList.sortBy(_._1)

    var totInt = 0
    var totExt = 0
    var n = 0
    partitionEdgeStats.foreach { case (partition, (internal, external)) =>
      println("Partition %d: Internal links: %d, external links: %d".format(partition, internal, external))
      totInt = totInt + internal
      totExt = totExt + external
      n = n + 1
    }
    println("Averages: Internal: "+(totInt.toDouble / n)+", External: "+(totExt.toDouble / n))
    println("Totals: Internal: "+totInt+", External: "+totExt+", overall: "+(totInt + totExt))

    partitionEdgeStats.map(_._1).reduce(_ max _)
  }

  /**
   * Take a graph, divide it up into partitions in a specified way, and allow the user to do something with those
   * partitions
   *
   * @param graph The graph to divide
   * @param partitions The rough number of sub-graphs
   * @param bidirectional True if links in the graph should be considered as bi-directional, false if they should
   *                      be considered unidirectional
   * @param fcn What to do with partitions.  This function takes an iterator over the nodes in a partition, and the
   *            links coming from those nodes.
   * @tparam T The output type
   * @return The output of fcn on each partition of the graph
   */
  def iterateOverPartitions[VD, ED, T] (graph: Graph[VD, ED], partitions: Int, bidirectional: Boolean)
                                       (fcn: (Iterator[(VertexId, VD)], Iterator[Edge[ED]]) => Iterator[T]): RDD[T] = {
    val (annotatedGraph, partitions) = annotateGraphWithPartition(graph, partitions)
    val partitioner = new KeyPartitioner(partitions)

    val partitionedNodes = annotatedGraph.vertices.map { case (vertexId, (data, partition)) =>
      (partition, (vertexId, data))
    }.partitionBy(partitioner)

    val partitionedLinks = annotatedGraph.triplets.flatMap { triplet: EdgeTriplet[(VD, Int), ED] =>
      if (bidirectional) {
        List[(Int, Edge[ED])](
          (triplet.srcAttr._2, new Edge[ED](triplet.srcId, triplet.dstId, triplet.attr)),
          (triplet.dstAttr._2, new Edge[ED](triplet.dstId, triplet.srcId, triplet.attr))
        )
      } else {
        List[(Int, Edge[ED])]((triplet.srcAttr._2, triplet))
      }
    }.partitionBy(partitioner)

    partitionedNodes.zipPartitions(partitionedLinks, true) { case (inRaw, ieRaw) =>
      fcn(inRaw.map(_._2), ieRaw.map(_._2))
    }
  }
}

class KeyPartitioner (partitions: Int) extends Partitioner {
  override def numPartitions: PartitionID = partitions

  override def getPartition(key: Any): PartitionID = key.asInstanceOf[Int]
}
