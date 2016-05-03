package software.uncharted.graphing.clustering.unithread

import org.scalatest.FunSuite

/**
  * Created by nkronenfeld on 2016-02-01.
  */
class CommunityModificationsTestSuite extends FunSuite {
  test("Test node degree modification") {
    // 3 3-node stars, with a central node (node 0) connected to everything
    val g: Graph = new Graph(
      Array(2, 4, 6, 10, 12, 14, 16, 20, 22, 24, 26, 30, 42),
      Array(
        3, 12,
        3, 12,
        3, 12,
        0, 1, 2, 12,
        7, 12,
        7, 12,
        7, 12,
        4, 5, 6, 12,
        11, 12,
        11, 12,
        11, 12,
        8, 9, 10, 12,
        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11
      ),
      (0 to 12).map(n => new NodeInfo(n.toLong, 1, None, Array(), Seq())).toArray
    )
    val c = new Community(g, -1, 0.15, new NodeDegreeAlgorithm(5))
    c.one_level(false)
    assert(3 === c.n2c(0))
    assert(3 === c.n2c(1))
    assert(3 === c.n2c(2))
    assert(3 === c.n2c(3))
    assert(7 === c.n2c(4))
    assert(7 === c.n2c(5))
    assert(7 === c.n2c(6))
    assert(7 === c.n2c(7))
    assert(11 === c.n2c(8))
    assert(11 === c.n2c(9))
    assert(11 === c.n2c(10))
    assert(11 === c.n2c(11))
    assert(12 === c.n2c(12))
  }
}
