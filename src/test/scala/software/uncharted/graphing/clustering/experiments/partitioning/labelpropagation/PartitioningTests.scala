package software.uncharted.graphing.clustering.experiments.partitioning.labelpropagation

import scala.collection.mutable.{Map => MutableMap}
import org.apache.spark.SharedSparkContext
import org.apache.spark.graphx.{VertexId, Graph, Edge}
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfter, FunSuite}
import software.uncharted.graphing.RandomGraph
import software.uncharted.graphing.utilities.TestUtilities

import scala.reflect.ClassTag

/**
 * Created by nkronenfeld on 12/9/2015.
 */
class PartitioningTests extends FunSuite with SharedSparkContext with BeforeAndAfter {

  import TestUtilities._

  before(turnOffLogSpew)

  def getRandomGraph (n: Int) = {
    val links = (n*math.log10(n)*20).toInt
    val partitions = (math.log10(links)*3/2).toInt
    new RandomGraph(sc).makeRandomGraph(n, partitions, links, partitions, r => 1.0f)
  }

  def getRandomGraph (n: Int, links: Int) = {
    val partitions = (math.log10(links)*3/2).toInt
    new RandomGraph(sc).makeRandomGraph(n, partitions, links, partitions, r => 1.0f)
  }

  def getFullyConnectedGraph (n: Int) = {
    val partitions = (math.log10(n*(n-1))*3/2).toInt
    val edges: Seq[Edge[Float]] = (for (i <- 0 until n; j <- 0 until n) yield {
      val result: List[(Long, Long)] =
        if (i == j) List[(Long, Long)]()
        else List((i.toLong, j.toLong))
      result
    }).flatMap(list => list).map{case (src, dst) => new Edge(src, dst, 1.0f)}
    val edgeRDD: RDD[Edge[Float]] = sc.parallelize(edges, partitions)
    Graph.fromEdges(edgeRDD, -1).mapVertices{case (id, data) => id}
  }

  def getStarGraph (n: Int) = {
    val edges: Seq[Edge[Float]] = (for (i <- 1 until n) yield new Edge(0, i, 1.0f))
    val edgeRDD = sc.parallelize(edges)
    Graph.fromEdges(edgeRDD, 1).mapVertices{case (id, data) => id}
  }

  test("Test degree distribution") {
    val graph = getFullyConnectedGraph(1000)

    val degreeDistribution = RoughGraphPartitioner.getDegreeDistribution(graph).toList
    assert(List((1998, 1000)) === degreeDistribution)
  }

  test("Test partition annotation") {
    val graph = getRandomGraph(10000)
    val (graphWithPartition, partitions) = RoughGraphPartitioner.annotateGraphWithPartition(graph, 10, 1.0)
    println("Partition sizes (in nodes)")
    graphWithPartition.vertices.map{case (id, (data, partition)) => (partition, 1)}.reduceByKey(_ + _).collect.sortBy(_._1).foreach(println)

    println("Total links: "+graphWithPartition.edges.count)


    println("\n\n\nGraph with our partitioning:")
    val maxPartition = RoughGraphPartitioner.countLinks(graphWithPartition)


    println("\n\n\nGraph with random partitioning:")
    RoughGraphPartitioner.countLinks(RoughGraphPartitioner.annotateGraphWithRandomPartition(graph, maxPartition + 1))
  }

  def mergeGraphs[VD: ClassTag, ED: ClassTag] (graphs: Graph[VD, ED]*): Graph[VD, ED] = {
    val N = graphs.size
    val sizes = graphs.map(g => (g.vertices.count()))
    val sizeIncrements = (0 until N).map(n => (0 until n).map(i => sizes(i)).fold(0L)(_ + _))
    val shiftedGraphs = graphs.zip(sizeIncrements).map { case (graph, increment) =>
      (
        graph.vertices.map { case (id, data) => ((id + increment), data) },
        graph.edges.map(edge => new Edge(edge.srcId + increment, edge.dstId + increment, edge.attr))
        )
    }
    val combinedVertices = shiftedGraphs.map(_._1).reduce(_ union _)
    val combinedEdges = shiftedGraphs.map(_._2).reduce(_ union _)
    Graph(combinedVertices, combinedEdges)
  }
  test("Test partitioning") {
    // Given three star sub-graphs, make sure our partitioning breaks the graph up back into those stars
    val subGraphA = getStarGraph(10).mapVertices{case (id, data) => (data, "A")}
    val subGraphB = getStarGraph(10).mapVertices{case (id, data) => (data, "B")}
    val subGraphC = getStarGraph(10).mapVertices{case (id, data) => (data, "C")}
    val graph = mergeGraphs(subGraphA, subGraphB, subGraphC)
    val vertices = graph.vertices.collect
    val edges = graph.edges.collect

    val partitionFcn: (Iterator[(VertexId, (Long, String))], Int, Iterator[Edge[Float]]) => Iterator[(Set[(VertexId, (Long, String))], Set[Edge[Float]])] =
      (in, numNodes, ie) => {
      Iterator((in.toSet, ie.toSet))
    }
    val partitionedGraph = RoughGraphPartitioner.iterateOverPartitions(graph, 3, true, -1.0)(partitionFcn)
    val partitions = collectPartitions(partitionedGraph)

    partitions.foreach{partition =>
      val (index, contents) = partition
      assert(1 === contents.size)
      val nodes = contents.head._1.toList.sorted

      nodes.headOption.map { case head =>
        val indicator = head._1
        nodes.foreach { case (id, (baseId, subGraphName)) =>
          assert(baseId === id - indicator)
          indicator match {
            case  0 => assert("A" === subGraphName)
            case 10 => assert("B" === subGraphName)
            case 20 => assert("C" === subGraphName)
          }
        }
      }
    }
  }
}
