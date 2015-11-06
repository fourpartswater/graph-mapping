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
package software.uncharted.graphing.clustering.reference

import org.apache.spark.SharedSparkContext
import org.apache.spark.graphx.Edge
import org.scalatest.FunSuite

/**
 * Created by nkronenfeld on 11/3/2015.
 */
class SparkBaselineConversionTestSuite extends FunSuite with SharedSparkContext {
  test("Test conversion from Spark graph to BGL graph") {
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
    val sparkGraph = org.apache.spark.graphx.Graph(nodes, edges)
    val bdlGraph = Graph(sparkGraph)


    assert(4 === bdlGraph.nb_neighbors(0))
    assert(3 === bdlGraph.nb_neighbors(1))
    assert(5 === bdlGraph.nb_neighbors(2))
    assert(2 === bdlGraph.nb_neighbors(3))
    assert(4 === bdlGraph.nb_neighbors(4))
    assert(4 === bdlGraph.nb_neighbors(5))
    assert(3 === bdlGraph.nb_neighbors(6))
    assert(4 === bdlGraph.nb_neighbors(7))
    assert(5 === bdlGraph.nb_neighbors(8))
    assert(3 === bdlGraph.nb_neighbors(9))
    assert(6 === bdlGraph.nb_neighbors(10))
    assert(5 === bdlGraph.nb_neighbors(11))
    assert(2 === bdlGraph.nb_neighbors(12))
    assert(2 === bdlGraph.nb_neighbors(13))
    assert(3 === bdlGraph.nb_neighbors(14))
    assert(1 === bdlGraph.nb_neighbors(15))

    assert(bdlGraph.neighbors(0).toList === List((2, 1.0), (3, 1.0), (4, 1.0), (5, 1.0)))
    assert(bdlGraph.neighbors(1).toList === List((2, 1.0), (4, 1.0), (7, 1.0)))
    assert(bdlGraph.neighbors(2).toList === List((0, 1.0), (1, 1.0), (4, 1.0), (5, 1.0), (6, 1.0)))
    assert(bdlGraph.neighbors(3).toList === List((0, 1.0), (7, 1.0)))
    assert(bdlGraph.neighbors(4).toList === List((0, 1.0), (1, 1.0), (2, 1.0), (10, 1.0)))
    assert(bdlGraph.neighbors(5).toList === List((0, 1.0), (2, 1.0), (7, 1.0), (11, 1.0)))
    assert(bdlGraph.neighbors(6).toList === List((2, 1.0), (7, 1.0), (11, 1.0)))
    assert(bdlGraph.neighbors(7).toList === List((1, 1.0), (3, 1.0), (5, 1.0), (6, 1.0)))
    assert(bdlGraph.neighbors(8).toList === List((9, 1.0), (10, 1.0), (11, 1.0), (14, 1.0), (15, 1.0)))
    assert(bdlGraph.neighbors(9).toList === List((8, 1.0), (12, 1.0), (14, 1.0)))
    assert(bdlGraph.neighbors(10).toList === List((4, 1.0), (8, 1.0), (11, 1.0), (12, 1.0), (13, 1.0), (14, 1.0)))
    assert(bdlGraph.neighbors(11).toList === List((5, 1.0), (6, 1.0), (8, 1.0), (10, 1.0), (13, 1.0)))
    assert(bdlGraph.neighbors(12).toList === List((9, 1.0), (10, 1.0)))
    assert(bdlGraph.neighbors(13).toList === List((10, 1.0), (11, 1.0)))
    assert(bdlGraph.neighbors(14).toList === List((8, 1.0), (9, 1.0), (10, 1.0)))
    assert(bdlGraph.neighbors(15).toList === List((8, 1.0)))

    assert(4.0 === bdlGraph.weighted_degree(0))
    assert(3.0 === bdlGraph.weighted_degree(1))
    assert(5.0 === bdlGraph.weighted_degree(2))
    assert(2.0 === bdlGraph.weighted_degree(3))
    assert(4.0 === bdlGraph.weighted_degree(4))
    assert(4.0 === bdlGraph.weighted_degree(5))
    assert(3.0 === bdlGraph.weighted_degree(6))
    assert(4.0 === bdlGraph.weighted_degree(7))
    assert(5.0 === bdlGraph.weighted_degree(8))
    assert(3.0 === bdlGraph.weighted_degree(9))
    assert(6.0 === bdlGraph.weighted_degree(10))
    assert(5.0 === bdlGraph.weighted_degree(11))
    assert(2.0 === bdlGraph.weighted_degree(12))
    assert(2.0 === bdlGraph.weighted_degree(13))
    assert(3.0 === bdlGraph.weighted_degree(14))
    assert(1.0 === bdlGraph.weighted_degree(15))

    assert(56.0 === bdlGraph.total_weight)
  }
}
