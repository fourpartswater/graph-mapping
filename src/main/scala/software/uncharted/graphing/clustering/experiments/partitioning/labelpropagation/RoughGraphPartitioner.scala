/**
  * Copyright (c) 2014-2016 Uncharted Software Inc. All rights reserved.
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
package software.uncharted.graphing.clustering.experiments.partitioning.labelpropagation



import scala.collection.mutable.{Map => MutableMap}

import org.apache.spark.graphx._ //scalastyle:ignore
import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag



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

  /**
   * Given a graph, find those most connected nodes that can be used as partition indicators
   * @param randomness the expected randomness in the graph. A completely random graph should get a 1.0, a totally
   *                   designed graph should get a -1.0.
   */
  def getPartitionIndicators[VD, ED] (graph: Graph[VD, ED], partitions: Int, randomness: Double): (Map[VertexId, (Int, Long)], Map[Int, Long]) = {
    val linkCount = graph.edges.count
    val graphWithDegree = annotateGraphWithDegree(graph)
    val vertices = graphWithDegree.vertices
    vertices.cache

    val nodeCount = vertices.count

    val degreeDistribution: Seq[(Long, Int)] = getDegreeDistributionInternal(vertices)
    val totalDegree = degreeDistribution.map{case (degree, count) => degree * count}.reduce(_ + _)
    val degreePerNode = totalDegree.toDouble / nodeCount
    val requiredTotalDegree = (totalDegree / (2.0 - randomness))  * ((partitions - 1.0) / partitions)

    var totalUsedDegree = 0L
    var minNeededDegree = Long.MaxValue
    var neededNodes = 0
    degreeDistribution.foreach{case(degree, count) =>
      if (totalUsedDegree < requiredTotalDegree) {
        totalUsedDegree = totalUsedDegree + degree * count
        neededNodes = neededNodes + count
        if (totalUsedDegree >= requiredTotalDegree) {
          minNeededDegree = degree
        }
      }
    }

    println("Pulling " + neededNodes + " of " + nodeCount + " nodes")
    println("  Total degree used: " + totalUsedDegree + " of " + totalDegree)
    println("  Minimum needed degree: " + minNeededDegree)
    val keyVertices = vertices.filter(_._2._2 >= minNeededDegree).map{case (id, (data, degree)) => (id, degree)}.collect()

    vertices.unpersist(false)

    val target = requiredTotalDegree / (partitions-1)
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
    println("Total partitions: " + partition)

    (partitionIndicators.toMap, partitionSizes.toMap)
  }

  /**
   * Annotate the nodes of a graph with the partition into which they should fall
   * @param graph The graph to partition
   * @param partitions A rough number of partitions into which to divide the graph.  This is an approximate input;
   *                   the actual number of partitions may vary slightly.
   * @param randomness The expected randomness in the graph.  A totally random graph should pass in 1.0, a totally
   *                   non-random graph, -1.0.
   * @param weightFcn A function to get the weight of a link.  Default is to assume every link has weight 1.0.  The
   *                  weight is used when determining the best partition in which to place a node - the weight of
   *                  links to progenitor nodes for that partition is taken into account.
   * @return A graph with the partition for each noded added to the node data, and the number of partitions
   */
  def annotateGraphWithPartition[VD, ED] (graph: Graph[VD, ED], partitions: Int,
                                          randomness: Double, weightFcn: ED => Float = (edgeAttr: ED) => 1.0f): (Graph[(VD, Int), ED], Int) = {
    val (partitionIndicators, partitionSizes) = getPartitionIndicators(graph, partitions, randomness)
    val maxPartition = partitionIndicators.map(_._2._1).reduce(_ max _)
    // A partition for nodes that aren't linked to any indicated node
    val otherPartition = maxPartition + 1

    println("Partition sizes (in total degree of indicators):")
    partitionSizes.toList.sortBy(_._1).foreach(x => println("\t" + x))
    println("Other partition: " + otherPartition)

    // Get the max weight in the graph
    val maxWeight = graph.edges.map(edge => weightFcn(edge.attr)).reduce(_ max _)

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
      b.foreach { case (k, v) => mergedMap(k) = mergedMap.get(k).getOrElse(0.0f) + v }
      mergedMap
    }

    val annotate: (VertexId, VD, Option[MutableMap[Int, Float]]) => (VD, Int) = (node, data, partitionsOpt) => {
      val partitions = partitionsOpt.getOrElse(MutableMap[Int, Float]())

      val proportionalPartitions = partitions.map { case (partition, count) => (partition, count.toDouble / partitionSizes(partition)) }
      var (bestPartition, bestProportionalPartition) =
        proportionalPartitions.fold((otherPartition, 0.0))((a, b) =>
          if (a._2 > b._2) a else b
        )

      if (partitionIndicators.contains(node)) {
        // See if it should be in its own partition
        val (selfPartition, selfDegree) = partitionIndicators(node)
        val selfPartitionDegree = partitionSizes(selfPartition)
        val partitionCount: Long = partitionSizes(partitionIndicators(node)._1)
        val selfPartitionScore = selfDegree * maxWeight / selfPartitionDegree

        if (selfPartitionScore > bestProportionalPartition) {
          bestPartition = selfPartition
          bestProportionalPartition = selfPartitionScore
        }
      }

      (data, bestPartition)
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
      if (srcPartition == dstPartition) {
        MutableMap(srcPartition -> (2, 0))
      } else {
        MutableMap(srcPartition -> (0, 1), dstPartition -> (0, 1))
      }
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
    println("Averages: Internal: " + (totInt.toDouble / n) + ", External: " + (totExt.toDouble / n))
    println("Totals: Internal: " + totInt + ", External: " + totExt + ", overall: " + (totInt + totExt))

    partitionEdgeStats.map(_._1).reduce(_ max _)
  }

  /**
   * Take a graph, divide it up into partitions in a specified way, and allow the user to do something with those
   * partitions
   *
   * @param graph The graph to divide
   * @param requestedPartitions The desired number of sub-graphs; this will be roughly fulfilled, but perhaps not
   *                            exactly.
   * @param bidirectional True if links in the graph should be considered as bi-directional, false if they should
   *                      be considered unidirectional
   * @param randomness The expected randomness in the graph.  A totally random graph should pass in 1.0, a totally
   *                   non-random graph, -1.0.
   * @param fcn What to do with partitions.  This function takes:
    *            <ol>
    *              <li> an iterator over the nodes in a partition</li>
    *              <li> The number of nodes in this partition </li>
    *              <li> An iterator over all links with these nodes as their source </li>
    *            </ol>
   * @tparam T The output type
   * @return The output of fcn on each partition of the graph
   */
  def iterateOverPartitions[VD, ED, T: ClassTag] (graph: Graph[VD, ED], requestedPartitions: Int,
                                                  bidirectional: Boolean, randomness: Double)
                                                 (fcn: (Iterator[(VertexId, VD)], Int, Iterator[Edge[ED]]) => Iterator[T]): RDD[T] = {
    val (annotatedGraph, actualPartitions) = annotateGraphWithPartition[VD, ED](graph, requestedPartitions, randomness)
    val partitioner = new KeyPartitioner(actualPartitions)

    // Partition our nodes according to annotation
    val partitionedNodes = annotatedGraph.vertices.map { case (vertexId, (data, partition)) =>
      (partition, (vertexId, data))
    }.partitionBy(partitioner)

    // Partition our links according to annotation
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

    // Get the number of nodes per partition
    val numNodes = partitionedNodes.mapPartitionsWithIndex { case (partition, iterator) =>
      Iterator((partition, iterator.size))
    }.partitionBy(partitioner)

    partitionedNodes.zipPartitions(partitionedLinks, numNodes, true) { case (inRaw, ieRaw, countsRaw) =>
      fcn(inRaw.map(_._2), countsRaw.next()._2, ieRaw.map(_._2))
    }
  }
}

class KeyPartitioner (partitions: Int) extends Partitioner {
  override def numPartitions: PartitionID = partitions

  override def getPartition(key: Any): PartitionID = key.asInstanceOf[Int]
}
