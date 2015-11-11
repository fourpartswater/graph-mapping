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
package software.uncharted.graphing.clustering.utilities



import org.scalatest.FunSuite
import org.scalatest.Matchers._

import org.apache.spark.SharedSparkContext
import org.apache.spark.graphx.{Edge, Graph}



/**
 * Created by nkronenfeld on 11/11/15.
 */
class GraphOperationsTestSuite extends FunSuite with SharedSparkContext {
  import GraphOperations._
  val epsilon = 1E-6

  test("Test individual modularity calculation") {
    // Try the test graph from https://sites.google.com/site/findcommunities/, in which the original Louvain algorithm is
    // published.
    val nodes = sc.parallelize(0L to 15L).map(n => (n, n))
    val edges = sc.parallelize(List[Edge[Double]](
      new Edge(0L, 2L, 1.0), new Edge(0L, 3L, 1.0), new Edge(0L, 4L, 1.0), new Edge(0L, 5L, 1.0),
      new Edge(1L, 2L, 1.0), new Edge(1L, 4L, 1.0), new Edge(1L, 7L, 1.0),
      new Edge(2L, 4L, 1.0), new Edge(2L, 5L, 1.0), new Edge(2L, 6L, 1.0),
      new Edge(3L, 7L, 1.0),
      new Edge(4L, 10L, 1.0),
      new Edge(5L, 7L, 1.0), new Edge(5L, 11L, 1.0),
      new Edge(6L, 7L, 1.0), new Edge(6L, 11L, 1.0),
      new Edge(8L, 9L, 1.0), new Edge(8L, 10L, 1.0), new Edge(8L, 11L, 1.0), new Edge(8L, 14L, 1.0), new Edge(8L, 15L, 1.0),
      new Edge(9L, 12L, 1.0), new Edge(9L, 14L, 1.0),
      new Edge(10L, 11L, 1.0), new Edge(10L, 12L, 1.0), new Edge(10L, 13L, 1.0), new Edge(10L, 14L, 1.0),
      new Edge(11L, 13L, 1.0)
    ))
    val canonicalGraph = Graph(nodes, edges)
    val canonicalModularity: Double = canonicalGraph.calculateIndividualModularity(d => d)
    canonicalModularity should be ((-1.0 / 14.0) +- epsilon)
  }

  test("Test grouped modularity calculation") {
    // Try the test graph from https://sites.google.com/site/findcommunities/, in which the original Louvain algorithm is
    // published.
    val nodes = sc.parallelize(0L to 15L).map(n => (n, n))
    val edges = sc.parallelize(List[Edge[Double]](
      new Edge(0L, 2L, 1.0), new Edge(0L, 3L, 1.0), new Edge(0L, 4L, 1.0), new Edge(0L, 5L, 1.0),
      new Edge(1L, 2L, 1.0), new Edge(1L, 4L, 1.0), new Edge(1L, 7L, 1.0),
      new Edge(2L, 4L, 1.0), new Edge(2L, 5L, 1.0), new Edge(2L, 6L, 1.0),
      new Edge(3L, 7L, 1.0),
      new Edge(4L, 10L, 1.0),
      new Edge(5L, 7L, 1.0), new Edge(5L, 11L, 1.0),
      new Edge(6L, 7L, 1.0), new Edge(6L, 11L, 1.0),
      new Edge(8L, 9L, 1.0), new Edge(8L, 10L, 1.0), new Edge(8L, 11L, 1.0), new Edge(8L, 14L, 1.0), new Edge(8L, 15L, 1.0),
      new Edge(9L, 12L, 1.0), new Edge(9L, 14L, 1.0),
      new Edge(10L, 11L, 1.0), new Edge(10L, 12L, 1.0), new Edge(10L, 13L, 1.0), new Edge(10L, 14L, 1.0),
      new Edge(11L, 13L, 1.0)
    ))
    val canonicalGraph = Graph(nodes, edges)
    // First with each vertex in its own group
    val individualModularity: Double = canonicalGraph.calculateModularity(d => d)
    individualModularity should be((-1.0 / 14.0) +- epsilon)

    // Then, with grouped pairwise (0 and 1, 2 and 3, etc)
    val pairModularity = canonicalGraph.calculateModularity(d => d, (vid, data) => vid / 2)
    val targetPairModularity = -1.0 / 14.0 + 2.0 * (3.0 - (12.0 + 10.0 + 16.0 + 12.0 + 15.0 + 30.0 + 4.0 + 3.0) / 56.0) / 56.0
    pairModularity should be (targetPairModularity +- epsilon)
  }
}
