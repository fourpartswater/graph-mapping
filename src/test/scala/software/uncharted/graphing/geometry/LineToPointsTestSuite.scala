package software.uncharted.graphing.geometry



import org.scalatest.FunSuite

import scala.collection.mutable.{Buffer => MutableBuffer}
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

  test("Reversing input order should reverse output order") {
    val l2pF = new LineToPoints((4, 2), (20, 8))
    val pointsF = l2pF.rest().toList

    val l2pR = new LineToPoints((20, 8), (4, 2))
    val pointsR = l2pR.rest().toList

    assert(pointsF === pointsR.reverse)
  }

  test("Very short lines") {
    for (x0 <- -2 to 2; y0 <- -2 to 2; x1 <- -2 to 2; y1 <- -2 to 2) {
      if (x0 != x1 || y0 != y1) {
        val msg = "Line: [%d, %d] => [%d, %d]".format(x0, y0, x1, y1)

        val l2p = new LineToPoints((x0, y0), (x1, y1))
        val points = l2p.rest().toList
        assert((x0, y0) === points.head, msg)
        assert((x1, y1) === points.last, msg)

        val l2pr = new LineToPoints((x1, y1), (x0, y0))
        assert(points === l2pr.rest().toList.reverse, msg)
      }
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

  test("get some points for reversed line") {
    def testNth (n: Int): Unit = {
      val l2p = new LineToPoints((20, 8), (4, 2))
      val points1 = l2p.rest().toList.grouped(n).flatMap(group => Try(group(n-1)).toOption).toList
      l2p.reset()
      val points2 = (20 to 4 by -1).grouped(n).flatMap(group => Try(group(n-1)).toOption)
        .map(i => l2p.skip(n - 1)).toList

      assert(points1 === points2, "skipping "+n)
    }

    (2 to 16).map(testNth)
  }

  test("skip to point") {
    val l2p = new LineToPoints((4, 2), (20, 8))
    val reference = l2p.rest().toMap
    l2p.reset()

    assert((8, reference(8)) === l2p.skipTo(8))
    assert((10, reference(10)) === l2p.skipTo(10))
    assert((11, reference(11)) === l2p.skipTo(11))
    assert((13, reference(13)) === l2p.skipTo(13))
    assert((14, reference(14)) === l2p.skipTo(14))
  }

  test("Skip to point, horizontal line") {
    val l2p = new LineToPoints((0, 0), (22, 0))

    for (i <- 0 to 22) {
      assert((i, 0) === l2p.skipTo(i))
      l2p.reset()
    }
  }

  test("skip to point, vertical line") {
    val l2p = new LineToPoints((0, 0), (0, 22))

    for (i <- 0 to 22) {
      assert((0, i) === l2p.skipTo(i))
      l2p.reset()
    }
  }

  test("skip to point with reversed line") {
    val l2p = new LineToPoints((20, 8), (4, 2))
    val reference = l2p.rest().toMap
    l2p.reset()

    assert((14, reference(14)) === l2p.skipTo(14))
    assert((13, reference(13)) === l2p.skipTo(13))
    assert((11, reference(11)) === l2p.skipTo(11))
    assert((10, reference(10)) === l2p.skipTo(10))
    assert((8, reference(8)) === l2p.skipTo(8))
  }

  test("skip to distance from point") {
    val l2p = new LineToPoints((4, 2), (20, 8))
    val reference = l2p.rest().toMap
    l2p.reset()

    intercept[NoIntersectionException] {
      l2p.skipToDistance((-40, -40), 5)
    }

    intercept[NoIntersectionException] {
      l2p.skipToDistance((-40, 40), 5)
    }

    intercept[NoIntersectionException] {
      l2p.skipToDistance((40, -40), 5)
    }

    intercept[NoIntersectionException] {
      l2p.skipToDistance((40, 40), 5)
    }

    assert((7, reference(7)) === l2p.skipToDistance((10, 10), 7))
    assert((16, reference(16)) === l2p.skipToDistance((10, 10), 7))
    intercept[NoIntersectionException] {
      l2p.skipToDistance((10, 10), 7)
    }
  }

  test("skip to distance from point 2") {
    val l2p = new LineToPoints((20, 8), (4, 2))
    val reference = l2p.rest().toMap
    l2p.reset()

    intercept[NoIntersectionException] {
      l2p.skipToDistance((-40, -40), 5)
    }

    intercept[NoIntersectionException] {
      l2p.skipToDistance((-40, 40), 5)
    }

    intercept[NoIntersectionException] {
      l2p.skipToDistance((40, -40), 5)
    }

    intercept[NoIntersectionException] {
      l2p.skipToDistance((40, 40), 5)
    }

    // Going down, it errs on the other side of each boundary
    assert((17, reference(17)) === l2p.skipToDistance((10, 10), 7))
    assert((8, reference(8)) === l2p.skipToDistance((10, 10), 7))
    intercept[NoIntersectionException] {
      l2p.skipToDistance((10, 10), 7)
    }
  }


}
