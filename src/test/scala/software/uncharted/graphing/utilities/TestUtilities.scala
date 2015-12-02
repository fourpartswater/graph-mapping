package software.uncharted.graphing.utilities

import org.apache.log4j.{Level, Logger}
import org.apache.spark.annotation.DeveloperApi
import org.apache.spark._
import org.apache.spark.graphx.{VertexId, Graph, Edge}
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
 * Created by nkronenfeld on 12/1/2015.
 */
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
   * specifies the partition, the value, the contents (in order) of that partition.
   */
  def constructGraph[VD: ClassTag, ED: ClassTag] (sc: SparkContext,
                                                  nodes: Map[Int, Seq[(VertexId, VD)]],
                                                  edges: Map[Int, Seq[Edge[ED]]]): Graph[VD, ED] = {
    val nodeRDD = new FullySpecifiedRDD(sc, nodes)
    val edgeRDD = new FullySpecifiedRDD(sc, edges)
    Graph(nodeRDD, edgeRDD)
  }

  def collectPartitions[T] (data: RDD[T]): Map[Int, List[T]] =
    data.mapPartitionsWithIndex{case (partition, i) => Iterator((partition, i.toList))}.collect.toMap



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
}

