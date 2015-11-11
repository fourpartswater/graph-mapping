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



import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.Matchers._



/**
 * Created by nkronenfeld on 11/2/15.
 */
class GraphTestSuite extends FunSuite with BeforeAndAfter {
  val epsilon = 1E-6f

  def getUnweightedGraph = new Graph(
    Array(4, 7, 12, 14, 18, 22, 25, 29, 34, 37, 43, 48, 50, 52, 55, 56),
    Array(
        /*  0 */ 2, 3, 4, 5,
        /*  1 */ 2, 4, 7,
        /*  2 */ 0, 1, 4, 5, 6,
        /*  3 */ 0, 7,
        /*  4 */ 0, 1, 2, 10,
        /*  5 */ 0, 2, 7, 11,
        /*  6 */ 2, 7, 11,
        /*  7 */ 1, 3, 5, 6,
        /*  8 */ 9, 10, 11, 14, 15,
        /*  9 */ 8, 12, 14,
        /* 10 */ 4, 8, 11, 12, 13, 14,
        /* 11 */ 5, 6, 8, 10, 13,
        /* 12 */ 9, 10,
        /* 13 */ 10, 11,
        /* 14 */ 8, 9, 10,
        /* 15 */ 8
      )
    )
  def getWeightedGraph = new Graph(
    Array(4, 7, 12, 14, 18, 22, 25, 29, 34, 37, 43, 48, 50, 52, 55, 56),
    Array(
        /*  0 */ 2, 3, 4, 5,
        /*  1 */ 2, 4, 7,
        /*  2 */ 0, 1, 4, 5, 6,
        /*  3 */ 0, 7,
        /*  4 */ 0, 1, 2, 10,
        /*  5 */ 0, 2, 7, 11,
        /*  6 */ 2, 7, 11,
        /*  7 */ 1, 3, 5, 6,
        /*  8 */ 9, 10, 11, 14, 15,
        /*  9 */ 8, 12, 14,
        /* 10 */ 4, 8, 11, 12, 13, 14,
        /* 11 */ 5, 6, 8, 10, 13,
        /* 12 */ 9, 10,
        /* 13 */ 10, 11,
        /* 14 */ 8, 9, 10,
        /* 15 */ 8
      ),
      Some(
        Array(
          /*  0 */ 0.1f, 0.2f, 0.3f, 0.4f,
          /*  1 */ 0.1f, 0.2f, 0.3f,
          /*  2 */ 0.1f, 0.2f, 0.3f, 0.4f, 0.5f,
          /*  3 */ 0.1f, 0.2f,
          /*  4 */ 0.1f, 0.2f, 0.3f, 0.4f,
          /*  5 */ 0.1f, 0.2f, 0.3f, 0.4f,
          /*  6 */ 0.1f, 0.2f, 0.3f,
          /*  7 */ 0.1f, 0.2f, 0.3f, 0.4f,
          /*  8 */ 0.1f, 0.2f, 0.3f, 0.4f, 0.5f,
          /*  9 */ 0.1f, 0.2f, 0.3f,
          /* 10 */ 0.1f, 0.2f, 0.3f, 0.4f, 0.5f, 0.6f,
          /* 11 */ 0.1f, 0.2f, 0.3f, 0.4f, 0.5f,
          /* 12 */ 0.1f, 0.2f,
          /* 13 */ 0.1f, 0.2f,
          /* 14 */ 0.1f, 0.2f, 0.3f,
          /* 15 */ 0.1f
        )
      )
    )


  test("Test nb_neighbors on unweighted graph") {
    val graph = getUnweightedGraph
    assert(4 === graph.nb_neighbors(0))
    assert(3 === graph.nb_neighbors(1))
    assert(5 === graph.nb_neighbors(2))
    assert(2 === graph.nb_neighbors(3))
    assert(4 === graph.nb_neighbors(4))
    assert(4 === graph.nb_neighbors(5))
    assert(3 === graph.nb_neighbors(6))
    assert(4 === graph.nb_neighbors(7))
    assert(5 === graph.nb_neighbors(8))
    assert(3 === graph.nb_neighbors(9))
    assert(6 === graph.nb_neighbors(10))
    assert(5 === graph.nb_neighbors(11))
    assert(2 === graph.nb_neighbors(12))
    assert(2 === graph.nb_neighbors(13))
    assert(3 === graph.nb_neighbors(14))
    assert(1 === graph.nb_neighbors(15))
  }

  test("Test nb_neighbors on weighted graph") {
    val graphW = getWeightedGraph
    assert(4 === graphW.nb_neighbors(0))
    assert(3 === graphW.nb_neighbors(1))
    assert(5 === graphW.nb_neighbors(2))
    assert(2 === graphW.nb_neighbors(3))
    assert(4 === graphW.nb_neighbors(4))
    assert(4 === graphW.nb_neighbors(5))
    assert(3 === graphW.nb_neighbors(6))
    assert(4 === graphW.nb_neighbors(7))
    assert(5 === graphW.nb_neighbors(8))
    assert(3 === graphW.nb_neighbors(9))
    assert(6 === graphW.nb_neighbors(10))
    assert(5 === graphW.nb_neighbors(11))
    assert(2 === graphW.nb_neighbors(12))
    assert(2 === graphW.nb_neighbors(13))
    assert(3 === graphW.nb_neighbors(14))
    assert(1 === graphW.nb_neighbors(15))
  }

  test("Test neighbors on unweighted graph") {
    val graph = getUnweightedGraph
    assert(graph.neighbors(0).toList === List((2, 1.0), (3, 1.0), (4, 1.0), (5, 1.0)))
    assert(graph.neighbors(1).toList === List((2, 1.0), (4, 1.0), (7, 1.0)))
    assert(graph.neighbors(2).toList === List((0, 1.0), (1, 1.0), (4, 1.0), (5, 1.0), (6, 1.0)))
    assert(graph.neighbors(3).toList === List((0, 1.0), (7, 1.0)))
    assert(graph.neighbors(4).toList === List((0, 1.0), (1, 1.0), (2, 1.0), (10, 1.0)))
    assert(graph.neighbors(5).toList === List((0, 1.0), (2, 1.0), (7, 1.0), (11, 1.0)))
    assert(graph.neighbors(6).toList === List((2, 1.0), (7, 1.0), (11, 1.0)))
    assert(graph.neighbors(7).toList === List((1, 1.0), (3, 1.0), (5, 1.0), (6, 1.0)))
    assert(graph.neighbors(8).toList === List((9, 1.0), (10, 1.0), (11, 1.0), (14, 1.0), (15, 1.0)))
    assert(graph.neighbors(9).toList === List((8, 1.0), (12, 1.0), (14, 1.0)))
    assert(graph.neighbors(10).toList === List((4, 1.0), (8, 1.0), (11, 1.0), (12, 1.0), (13, 1.0), (14, 1.0)))
    assert(graph.neighbors(11).toList === List((5, 1.0), (6, 1.0), (8, 1.0), (10, 1.0), (13, 1.0)))
    assert(graph.neighbors(12).toList === List((9, 1.0), (10, 1.0)))
    assert(graph.neighbors(13).toList === List((10, 1.0), (11, 1.0)))
    assert(graph.neighbors(14).toList === List((8, 1.0), (9, 1.0), (10, 1.0)))
    assert(graph.neighbors(15).toList === List((8, 1.0)))
  }

  test("Test neighbors on weighted graph") {
    val graphW = getWeightedGraph
    assert(graphW.neighbors( 0).toList.map(_._1) === List( 2,  3,  4,  5))
    assert(graphW.neighbors( 1).toList.map(_._1) === List( 2,  4,  7))
    assert(graphW.neighbors( 2).toList.map(_._1) === List( 0,  1,  4,  5,  6))
    assert(graphW.neighbors( 3).toList.map(_._1) === List( 0,  7))
    assert(graphW.neighbors( 4).toList.map(_._1) === List( 0,  1,  2, 10))
    assert(graphW.neighbors( 5).toList.map(_._1) === List( 0,  2,  7, 11))
    assert(graphW.neighbors( 6).toList.map(_._1) === List( 2,  7, 11))
    assert(graphW.neighbors( 7).toList.map(_._1) === List( 1,  3,  5,  6))
    assert(graphW.neighbors( 8).toList.map(_._1) === List( 9, 10, 11, 14, 15))
    assert(graphW.neighbors( 9).toList.map(_._1) === List( 8, 12, 14))
    assert(graphW.neighbors(10).toList.map(_._1) === List( 4,  8, 11, 12, 13, 14))
    assert(graphW.neighbors(11).toList.map(_._1) === List( 5,  6,  8, 10, 13))
    assert(graphW.neighbors(12).toList.map(_._1) === List( 9, 10))
    assert(graphW.neighbors(13).toList.map(_._1) === List(10, 11))
    assert(graphW.neighbors(14).toList.map(_._1) === List( 8,  9, 10))
    assert(graphW.neighbors(15).toList.map(_._1) === List( 8))
  }

  test("Test weighted_degree on unweighted graph") {
    val graph = getUnweightedGraph
    assert(4.0 === graph.weighted_degree(0))
    assert(3.0 === graph.weighted_degree(1))
    assert(5.0 === graph.weighted_degree(2))
    assert(2.0 === graph.weighted_degree(3))
    assert(4.0 === graph.weighted_degree(4))
    assert(4.0 === graph.weighted_degree(5))
    assert(3.0 === graph.weighted_degree(6))
    assert(4.0 === graph.weighted_degree(7))
    assert(5.0 === graph.weighted_degree(8))
    assert(3.0 === graph.weighted_degree(9))
    assert(6.0 === graph.weighted_degree(10))
    assert(5.0 === graph.weighted_degree(11))
    assert(2.0 === graph.weighted_degree(12))
    assert(2.0 === graph.weighted_degree(13))
    assert(3.0 === graph.weighted_degree(14))
    assert(1.0 === graph.weighted_degree(15))
  }

  test("Test weighted_degree on weighted graph") {
    val graphW = getWeightedGraph
    graphW.weighted_degree(0) should be (1.0 +- epsilon)
    graphW.weighted_degree(1) should be (0.6 +- epsilon)
    graphW.weighted_degree(2) should be (1.5 +- epsilon)
    graphW.weighted_degree(3) should be (0.3 +- epsilon)
    graphW.weighted_degree(4) should be (1.0 +- epsilon)
    graphW.weighted_degree(5) should be (1.0 +- epsilon)
    graphW.weighted_degree(6) should be (0.6 +- epsilon)
    graphW.weighted_degree(7) should be (1.0 +- epsilon)
    graphW.weighted_degree(8) should be (1.5 +- epsilon)
    graphW.weighted_degree(9) should be (0.6 +- epsilon)
    graphW.weighted_degree(10) should be (2.1 +- epsilon)
    graphW.weighted_degree(11) should be (1.5 +- epsilon)
    graphW.weighted_degree(12) should be (0.3 +- epsilon)
    graphW.weighted_degree(13) should be (0.3 +- epsilon)
    graphW.weighted_degree(14) should be (0.6 +- epsilon)
    graphW.weighted_degree(15) should be (0.1 +- epsilon)
  }

  test("Test total weight of unweighted graph") {
    getUnweightedGraph.total_weight should be(56.0 +- epsilon)
  }

  test("Test total weight of weighted graph") {
    getWeightedGraph.total_weight should be (14.0 +- epsilon)
  }
}
