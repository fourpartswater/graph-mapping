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
package software.uncharted.graphing.utilities



import java.io.{ObjectOutputStream, ByteArrayOutputStream}

import org.apache.log4j.{Level, Logger}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark._
import org.apache.spark.graphx._
import org.apache.spark.graphx.impl.GraphImpl
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag



object TestUtilities {
  /** Turn off extraneous logs by spark and everything else, so we can see what is going on in tests */
  def turnOffLogSpew: Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
  }

  /**
   * Create the test graph from https://sites.google.com/site/findcommunities/, in which the original Louvain
   * algorithm is published.
   * @param sc A spark context in which to create the graph
   */
  def standardBGLLGraph[T: ClassTag] (sc: SparkContext, weightFcn: Double => T): Graph[Long, T] = {
    val weight = weightFcn(1.0)
    val nodes = sc.parallelize(0L to 15L).map(n => (n, n))
    val edges = sc.parallelize(List[Edge[T]](
      new Edge( 0L,  2L, weight), new Edge( 0L,  3L, weight), new Edge( 0L,  4L, weight), new Edge( 0L,  5L, weight),
      new Edge( 1L,  2L, weight), new Edge( 1L,  4L, weight), new Edge( 1L,  7L, weight),
      new Edge( 2L,  4L, weight), new Edge( 2L,  5L, weight), new Edge( 2L,  6L, weight),
      new Edge( 3L,  7L, weight),
      new Edge( 4L, 10L, weight),
      new Edge( 5L,  7L, weight), new Edge( 5L, 11L, weight),
      new Edge( 6L,  7L, weight), new Edge( 6L, 11L, weight),
      new Edge( 8L,  9L, weight), new Edge( 8L, 10L, weight), new Edge( 8L, 11L, weight), new Edge( 8L, 14L, weight), new Edge( 8L, 15L, weight),
      new Edge( 9L, 12L, weight), new Edge( 9L, 14L, weight),
      new Edge(10L, 11L, weight), new Edge(10L, 12L, weight), new Edge(10L, 13L, weight), new Edge(10L, 14L, weight),
      new Edge(11L, 13L, weight)
    ))
    Graph(nodes, edges)
  }

  /**
   * Construct a graph with a specific partitioning and order, both for vertices and edges.  In each map, the key
   * specifies the partition, the value, the contents of that partition.  Due to limitations in the public API for
   * GraphX, we cannot specify the order of nodes within a partition
   */
  def constructGraph[VD: ClassTag, ED: ClassTag] (sc: SparkContext,
                                                  nodes: Map[Int, Seq[(VertexId, VD)]],
                                                  edges: Map[Int, Seq[Edge[ED]]]): Graph[VD, ED] = {
    // We can't avoid having the Graph shuffle the entries internally.  However, with a bunch of work - notably by
    // using an RDD with a built-in partitioner that tells spark in what partition to place each record - we can
    // force the graph to be partitioned the way we want.
    val nodeRDD = VertexRDD(new FullySpecifiedRDD(sc, nodes))
    val edgeRDD = EdgeRDD.fromEdges[ED, VD](new FullySpecifiedRDD(sc, edges))
    GraphImpl.fromExistingRDDs(nodeRDD, edgeRDD)
  }

  /**
   * Construct a graph with a specified order for edges, and default nodes - the specified number of nodes, numbered
   * from 0 until nodes - 1, with their value being the same as their vertexId (except as an Int).
   * @param sc A spark context in which to create the graph
   * @param nodes The number of nodes in the graph
   * @param edges The edges of the graph
   * @tparam ED The value type of the graph edges
   * @return A graph composed from the above information
   */
  def constructGraph[ED: ClassTag] (sc: SparkContext, nodes: Int, edges: Map[Int, Seq[Edge[ED]]]): Graph[Int, ED] =
    constructGraph(sc, Map(0 -> (0 until nodes).map(n => (n.toLong, n)).toSeq), edges)

  /**
   * Create an RDD of data, where partition and order within partition is fixed and controlable.
   * @param sc The spark context in which the resultant RDD must work
   * @param data The data to convert to RDD form. The map key specifies the partition, the value, the contents of
   *             that partition, in order.
   * @tparam T The type of element data of the resultant RDD
   * @return An RDD with the same data as the values of the input map.
   */
  def parallelizePartitions[T: ClassTag] (sc: SparkContext, data: Map[Int, Seq[T]]): RDD[T] =
    new FullySpecifiedRDD[T](sc, data)

  /**
   * Collect an RDD, partition by partition
   */
  def collectPartitions[T] (data: RDD[T]): Map[Int, List[T]] =
    data.mapPartitionsWithIndex{case (partition, i) => Iterator((partition, i.toList))}.collect.toMap

  /**
   * Test if a given object is serializable
   */
  def testSerializability (data: AnyRef): Unit = {
    val stream = new ObjectOutputStream(new ByteArrayOutputStream())
    stream.writeObject(data)
  }
}


/**
 * A special RDD whose data is all passed in at the start, and which has a built-in partitioner, used to force Graphs
 * to partition their nodes and edges the way we want them to.
 * @param _sc A spark context
 * @param data The data we want in our resultant RDD.  The map keys are partitions, and every partition from the start
 *             key to the end key must be present.  The values are sequences each containing the contents of a single
 *             partition.  Though the keys must be present for every intermediate partition number, they value can be
 *             an empty sequence, if a blank partition is desired.* @param ev1
 * @tparam T The value type of the resultant RDD
 */
private[utilities] class FullySpecifiedRDD[T: ClassTag] (@transient private var _sc: SparkContext, data: Map[Int, Seq[T]]) extends RDD[T](_sc, Seq[Dependency[_]]()) {
  private val minPartition = data.map(_._1).reduce(_ min _)
  private val maxPartition = data.map(_._1).reduce(_ max _)
  private val numPartitions = maxPartition - minPartition + 1
  assert(numPartitions == data.size)
  private val backMap = data.flatMap { case (partition, elements) =>
    elements.map(element => (element, partition))
  }.toMap
  private val ourPartitions = (minPartition to maxPartition).map(partition =>
    new Partition {
      override def index: Int = partition - minPartition
    }
  ).toArray

  @DeveloperApi
  override def compute(split: Partition, context: TaskContext): Iterator[T] = data(split.index).iterator

  override protected def getPartitions: Array[Partition] = ourPartitions

  @transient override val partitioner = Some(new FullySpecifiedRDDPartitioner)

  class FullySpecifiedRDDPartitioner extends Partitioner {
    override def numPartitions: Int = FullySpecifiedRDD.this.numPartitions

    override def getPartition(key: Any): Int = {
      key match {
        case t: T => backMap(t)
        case _ => 0
      }
    }
  }
}

