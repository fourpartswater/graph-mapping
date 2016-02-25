package software.uncharted.graphing.salt

import org.scalatest.FunSuite

/**
  * Created by nkronenfeld on 2/23/2016.
  */
class SegmentTilingTestSuite extends FunSuite {
  import TestableSegmentProjection._

  test("tile/bin to universal coordinates, y not flipped") {
    assert((0, 0) === tb2ub((4, 0, 0), (0, 0), (3, 3), false))
    assert((3, 0) === tb2ub((4, 0, 0), (3, 0), (3, 3), false))
    assert((0, 3) === tb2ub((4, 0, 0), (0, 3), (3, 3), false))
    assert((3, 3) === tb2ub((4, 0, 0), (3, 3), (3, 3), false))

    assert((60, 60) === tb2ub((4, 15, 15), (0, 0), (3, 3), false))
    assert((60, 63) === tb2ub((4, 15, 15), (0, 3), (3, 3), false))
    assert((63, 60) === tb2ub((4, 15, 15), (3, 0), (3, 3), false))
    assert((63, 63) === tb2ub((4, 15, 15), (3, 3), (3, 3), false))
  }

  test("tile/bin to universal coordinates, y flipped") {
    assert((0, 60) === tb2ub((4, 0, 0), (0, 0), (3, 3), true))
    assert((0, 63) === tb2ub((4, 0, 0), (0, 3), (3, 3), true))
    assert((3, 60) === tb2ub((4, 0, 0), (3, 0), (3, 3), true))
    assert((3, 63) === tb2ub((4, 0, 0), (3, 3), (3, 3), true))

    assert((60, 0) === tb2ub((4, 15, 15), (0, 0), (3, 3), true))
    assert((60, 3) === tb2ub((4, 15, 15), (0, 3), (3, 3), true))
    assert((63, 0) === tb2ub((4, 15, 15), (3, 0), (3, 3), true))
    assert((63, 3) === tb2ub((4, 15, 15), (3, 3), (3, 3), true))
  }

  test("universal bin to tile/bin, y not flipped") {
    assert(((4, 0, 0), (0, 0)) === ub2tb(4, (0, 0), (3, 3), false))
    assert(((4, 0, 0), (3, 0)) === ub2tb(4, (3, 0), (3, 3), false))
    assert(((4, 0, 0), (0, 3)) === ub2tb(4, (0, 3), (3, 3), false))
    assert(((4, 0, 0), (3, 3)) === ub2tb(4, (3, 3), (3, 3), false))

    assert(((4, 15, 15), (0, 0)) === ub2tb(4, (60, 60), (3, 3), false))
    assert(((4, 15, 15), (0, 3)) === ub2tb(4, (60, 63), (3, 3), false))
    assert(((4, 15, 15), (3, 0)) === ub2tb(4, (63, 60), (3, 3), false))
    assert(((4, 15, 15), (3, 3)) === ub2tb(4, (63, 63), (3, 3), false))
  }

  test("universal bin to tile/bin, y flipped") {
    assert(((4, 0, 0), (0, 0)) === ub2tb(4, (0, 60), (3, 3), true))
    assert(((4, 0, 0), (0, 3)) === ub2tb(4, (0, 63), (3, 3), true))
    assert(((4, 0, 0), (3, 0)) === ub2tb(4, (3, 60), (3, 3), true))
    assert(((4, 0, 0), (3, 3)) === ub2tb(4, (3, 63), (3, 3), true))

    assert(((4, 15, 15), (0, 0)) === ub2tb(4, (60, 0), (3, 3), true))
    assert(((4, 15, 15), (0, 3)) === ub2tb(4, (60, 3), (3, 3), true))
    assert(((4, 15, 15), (3, 0)) === ub2tb(4, (63, 0), (3, 3), true))
    assert(((4, 15, 15), (3, 3)) === ub2tb(4, (63, 3), (3, 3), true))
  }

  test("tile/bin round trip through universal bin") {
    def roundTrip (z: Int, x: Int, y: Int, tms: Boolean): (Int, Int) = {
      val (tile, bin) = ub2tb(z, (x, y), (3, 3), tms)
      tb2ub(tile, bin, (3, 3), tms)
    }
    for (z <- 0 to 4) {
      val max = 1 << z
      for (x <- 0 until max; y <- 0 until max) {
        assert((x, y) === roundTrip(z, x, y, true))
        assert((x, y) === roundTrip(z, x, y, false))
      }
    }
  }

//  test("endpointsToBins with leader line limit") {
//    val calculator = new StraightSegmentCalculation {}
//
//    for (x1 <- 0 to 10; y1 <- 0 to 10; x2 <- 10 to 20; y2 <- 10 to 20) {
//      val allBins = calculator.endpointsToBins((x1, y1), (x2, y2), None, None, None)
//      val leaderBins = calculator.endpointsToBins((x1, y1), (x2, y2), Some(4), None, None)
//      if (x1 == x2 && y1 == y2) {
//        assert(1 === leaderBins.length)
//        assert((x1, y1) === leaderBins(0))
//      } else {
//        leaderBins.foreach { case (x, y) =>
//          assert(16 >= (x - x1) * (x - x1) + (y - y1) * (y - y1) || 16 >= (x - x2) * (x - x2) + (y - y2) * (y - y2))
//          assert(allBins.contains((x, y)))
//        }
//      }
//    }
//  }
}

object TestableSegmentProjection extends SegmentProjection {
  def tb2ub(tile: (Int, Int, Int),
            bin: (Int, Int),
            maxBin: (Int, Int),
            tms: Boolean): (Int, Int) =
    tileBinIndexToUniversalBinIndex(tile, bin, maxBin, tms)

  def ub2tb(z: Int, ub: (Int, Int), maxBin: (Int, Int), tms: Boolean): ((Int, Int, Int), (Int, Int)) =
    universalBinIndexToTileIndex(z, ub, maxBin, tms)
}