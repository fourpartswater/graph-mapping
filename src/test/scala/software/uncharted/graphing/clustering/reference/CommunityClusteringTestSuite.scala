package software.uncharted.graphing.clustering.reference

import org.scalatest.{BeforeAndAfter, FunSuite}
import org.scalatest.Matchers._

/**
 * Created by nkronenfeld on 11/3/2015.
 */
class CommunityClusteringTestSuite extends FunSuite with BeforeAndAfter {
  val epsilon = 1E-12
  var graph: Graph = null
  var graphW: Graph = null
  before {
    graph = new Graph(
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
    graphW = new Graph(
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
  }
  after {
    graph = null
    graphW = null
  }

  test("Test baseline Louvain clustering (make sure it runs)") {
    val LH = new CommunityHarness
    LH.run(graph, verbose=true)
  }

  test("Test baseline weighted Louvain clustering (make sure it runs)") {
    val LH = new CommunityHarness
    LH.run(graphW, verbose=true)
  }

  test("Test that consolidating communities into a single node does not change the modularity of the whole graph") {
    var c1 = new Community(graph, -1, 0.15)
    val improvement = c1.one_level()
    val mod1 = c1.modularity

    val g2 = c1.partition2graph_binary
    val c2 = new Community(g2, -1, 0.15)
    val mod2 = c2.modularity

    assert(mod1 === mod2)
  }

  test("Test simple modularity calculation") {
    val c = new Community(graph, -1, 0.15)
    c.modularity should be ((-1.0 / 14.0) +- epsilon)
  }

  test("Test one-level calculation to make sure the output numbers are what we expect") {
    // Is this really possible due to the randomization of node order? If so, can we override the random node order
    // to use a fixed one we've pre-calculated?
  }
}
