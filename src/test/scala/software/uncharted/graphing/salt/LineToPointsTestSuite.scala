package software.uncharted.graphing.salt

import org.scalatest.FunSuite

import scala.util.Try

/**
  * Created by nkronenfeld on 2/24/2016.
  */
class LineToPointsTestSuite extends FunSuite {
  test("get all points") {
    val l2p = new LineToPoints((4, 2), (20, 8))

    assert(17 === l2p.available)
    val points = l2p.rest()
    assert(points.length === 17)
    assert((4, 2) === points(0))
    assert((20, 8) === points(16))

    points.sliding(2).foreach { p =>
      assert(p(0)._1 === p(1)._1 - 1)
      assert(p(0)._2 == p(1)._2 || p(0)._2 == p(1)._2 - 1)
    }
  }

  test("reset") {
    val l2p = new LineToPoints((4, 2), (20, 8))
    val points1 = l2p.rest().toList
    l2p.reset()
    val points2 = l2p.rest().toList

    assert(points1 === points2)
  }

  test("get some points") {
    def testNth (n: Int): Unit = {
      val l2p = new LineToPoints((4, 2), (20, 8))
      val points1 = l2p.rest().toList.grouped(n).flatMap(group => Try(group(n-1)).toOption).toList
      l2p.reset()
      val points2 = (4 to 20).grouped(n).flatMap(group => Try(group(n-1)).toOption)
        .map(i => l2p.skip(n - 1)).toList

      assert(points1 === points2, "skipping "+n)
    }

    (2 to 16).map(testNth)
  }

  test("skip to point") {
    val l2p = new LineToPoints((4, 2), (20, 8))
    val reference = l2p.rest().toMap
    l2p.reset()

    assert(reference(8) === l2p.skipTo(8)._2)
    assert(10 === l2p.skipTo(10)._1)
    assert(11 === l2p.skipTo(11)._1)
    assert(13 === l2p.skipTo(13)._1)
    assert(reference(14) === l2p.skipTo(14)._2)
  }
}
