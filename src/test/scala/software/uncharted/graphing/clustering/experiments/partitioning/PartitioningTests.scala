package software.uncharted.graphing.clustering.experiments.partitioning

import scala.collection.mutable.{Map => MutableMap}
import org.apache.spark.SharedSparkContext
import org.apache.spark.graphx.{Graph, Edge}
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfter, FunSuite}
import software.uncharted.graphing.RandomGraph
import software.uncharted.graphing.utilities.TestUtilities

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

  test("Test degree distribution") {
    val graph = getFullyConnectedGraph(1000)

    val degreeDistribution = RoughGraphPartitioner.getDegreeDistribution(graph).toList
    assert(List((1998, 1000)) === degreeDistribution)
  }

  test("Test partitioning") {
    val graph = getRandomGraph(10000)
    val graphWithPartition = RoughGraphPartitioner.annotateGraphWithPartition(graph, 10)
    println("Partition sizes (in nodes)")
    graphWithPartition.vertices.map{case (id, (data, partition)) => (partition, 1)}.reduceByKey(_ + _).collect.sortBy(_._1).foreach(println)

    println("Total links: "+graphWithPartition.edges.count)


    println("\n\n\nGraph with our partitioning:")
    val maxPartition = RoughGraphPartitioner.countLinks(graphWithPartition)


    println("\n\n\nGraph with random partitioning:")
    RoughGraphPartitioner.countLinks(RoughGraphPartitioner.annotateGraphWithRandomPartition(graph, maxPartition + 1))
  }
}
