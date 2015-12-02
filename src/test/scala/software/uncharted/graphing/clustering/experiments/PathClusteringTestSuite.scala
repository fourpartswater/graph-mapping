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
package software.uncharted.graphing.clustering.experiments



import org.apache.spark.SharedSparkContext
import org.apache.spark.graphx._
import org.apache.spark.rdd.RDD
import org.scalatest.{BeforeAndAfter, FunSuite}

import software.uncharted.graphing.utilities.TestUtilities




/**
 * Created by nkronenfeld on 10/28/2015.
 */
class PathClusteringTestSuite extends FunSuite with SharedSparkContext with BeforeAndAfter {
  import TestUtilities._
  before(turnOffLogSpew)

  test("Test trivial path clustering") {
    val N = 6
    // Construct our graph
    val edges: RDD[Edge[Int]] = sc.parallelize(
      for (size <- 1 to N; first <- 1 to size; second <- first+1 to size) yield {
        val start = size * (size - 1) / 2
        new Edge(start+first, start+second, 1)
      }
    )
    val vertices: RDD[(VertexId, Int)] = sc.parallelize(
      for (size <- 1 to N; n <- 1 to size) yield {
        val start = size * (size - 1) / 2
        ((start + n).toLong, size)
      }
    )
    val graph: Graph[Int ,Int] = Graph(vertices, edges)

    val pc = new PathClustering
    val clustered: Graph[(Int, VertexId), Int] = pc.checkClusters(graph)
    val output: Array[(VertexId, (Int, VertexId))] = clustered.vertices.collect().sorted
    output.foreach { case (id, (size, cluster)) =>
      val min = size * (size - 1) / 2
      val max = size * (size + 1) / 2
      assert(min < id && id <= max)
      assert(max === cluster)
    }
  }

  test("Test small clusters with 2 interlinks each") {
    // 4 fully connected subgraphs of size 5 (11-15, 16-20, 21-25, 26-30) each with one connection to the next and
    // one to the subsequent
    val edges = sc.parallelize(List(
      (11, 12), (11, 13), (11, 14), (11, 15), (12, 13), (12, 14), (12, 15), (13, 14), (13, 15), (14, 15),
      (16, 17), (16, 18), (16, 19), (16, 20), (17, 18), (17, 19), (17, 20), (18, 19), (18, 20), (19, 20),
      (21, 22), (21, 23), (21, 24), (21, 25), (22, 23), (22, 24), (22, 25), (23, 24), (23, 25), (24, 25),
      (26, 27), (26, 28), (26, 29), (26, 30), (27, 28), (27, 29), (27, 30), (28, 29), (28, 30), (29, 30),
      (11, 16), (17, 22), (23, 28), (29, 14)
    )).map{case (start, end) => new Edge(start, end, 1)}
    val vertices = sc.parallelize(11 to 30).map(n => (n.toLong, (n-11)/5))
    val graph = Graph(vertices, edges)
    val input = graph.vertices.collect.sorted

    val pc = new PathClustering
    val clustered: Graph[(Int, VertexId), Int] = pc.checkClusters(graph)
    val output: Array[(VertexId, (Int, VertexId))] = clustered.vertices.collect().sorted
    output.foreach { case (id, (target, clusterTop)) =>
      val cluster = (clusterTop/5)-3
      assert(target === cluster)
    }
  }
}
