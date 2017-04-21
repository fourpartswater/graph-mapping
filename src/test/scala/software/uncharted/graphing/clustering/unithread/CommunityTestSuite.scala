package software.uncharted.graphing.clustering.unithread

import org.scalatest.FunSuite

class CommunityTestSuite extends FunSuite {
  test("Remove node from community") {
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
      (0 to 12).map(n => new NodeInfo(n.toLong, 1, None, Array(), Array())).toArray
    )
    val c = new Community(g, -1, 0.15, new NodeDegreeAlgorithm(5))
    assert(c.n2c(0) === 0)
    assert(c.n2c(4) === 4)
    assert(c.n2c(11) === 11)

    c.oneLevel(false)
    assert(c.n2c(0) === 3)
    assert(c.n2c(4) === 7)
    assert(c.n2c(11) === 11)

    val initialSize = c.community_size(3)
    c.remove(0, 3, 2)
    assert(c.n2c(0) === -1)
    assert(c.community_size(3) == initialSize - 1)
  }
  test("Insert node in community") {
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
      (0 to 12).map(n => new NodeInfo(n.toLong, 1, None, Array(), Array())).toArray
    )
    val c = new Community(g, -1, 0.15, new NodeDegreeAlgorithm(5))
    assert(c.n2c(0) === 0)
    assert(c.n2c(4) === 4)
    assert(c.n2c(11) === 11)

    c.oneLevel(false)
    assert(c.n2c(0) === 3)
    assert(c.n2c(4) === 7)
    assert(c.n2c(11) === 11)

    val initialSize = c.community_size(3)
    c.insert(4, 3, 2)
    assert(c.n2c(4) === 3)
    assert(c.community_size(3) == initialSize + 1)
  }
}
