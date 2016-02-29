package software.uncharted.graphing.salt

import org.scalatest.FunSuite

/**
  * Created by nkronenfeld on 2016-02-25.
  */
class CartesianSegmentsTestSuite extends FunSuite {
  test("Leader line projection") {
    val maxBin = ((3, 3), (3, 3))
    val bounds = ((0.0, 0.0), (40.0, 40.0))
    val leaderLength = 7
    val tms = false

    val projection = new CartesianLeaderLineProjection(Seq(4), bounds._1, bounds._2, leaderLength, tms)
    val bruteForce = new BruteForceLeaderLineReducer(maxBin._1, bounds, 4, leaderLength, tms)
    for (x0 <- 0 to 20; y0 <- 0 to 20; x1 <- x0 until 40; y1 <- y0 until 40) {
      val tiles =
        try {
          projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin).get.map(_._1).toSet
        } catch {
          case e: Exception =>
            throw new Exception("Error getting tiles for line [%d, %d => %d, %d]".format(x0, y0, x1, y1), e)
        }
      val bruteForceTiles = bruteForce.getTiles(x0, y0, x1, y1).toSet
      assert(bruteForceTiles === tiles, "Points [%d, %d x %d, %d]".format(x0, y0, x1, y1))
    }
  }

  test("Single instance leader line projection") {
    val maxBin = ((3, 3), (3, 3))
    val bounds = ((0.0, 0.0), (40.0, 40.0))
    val leaderLength = 7
    val tms = false

    val projection = new CartesianLeaderLineProjection(Seq(4), bounds._1, bounds._2, leaderLength, tms)
    val bruteForce = new BruteForceLeaderLineReducer(maxBin._1, bounds, 4, leaderLength, tms)

    val x0 = 0
    val y0 = 0
    val x1 = 0
    val y1 = 12

    println("Checking [%d, %d -> %d, %d]".format(x0, y0, x1, y1))
    val tiles = projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin).get.map(_._1).toSet
    val bruteForceTiles = bruteForce.getTiles(x0, y0, x1, y1).toSet
    assert(bruteForceTiles === tiles, "Points [%d, %d x %d, %d]".format(x0, y0, x1, y1))
  }
}
class BruteForceLeaderLineReducer (maxBin: (Int, Int),
                                   bounds: ((Double, Double), (Double, Double)),
                                   level: Int,
                                   leaderLength: Int,
                                   tms: Boolean) extends SegmentProjection {
  def getTiles (x0: Double, y0: Double, x1: Double, y1: Double) = {
    def project(x: Double, y: Double) = {
      val scale = 1 << level
      val usx = (x - bounds._1._1) / (bounds._2._1 - bounds._1._1)
      val usy = (y - bounds._1._2) / (bounds._2._2 - bounds._1._2)
      val sx = usx * scale * (maxBin._1 + 1)
      val sy = usy * scale * (maxBin._2 + 1)
      (sx.floor.toInt, sy.floor.toInt)
    }

    val s = project(x0, y0)
    val e = project(x1, y1)

    val line = new LineToPoints(s, e)
    val points = line.rest()

    def distance(p0: (Int, Int), p1: (Int, Int)): Double = {
      val dx = p1._1 - p0._1
      val dy = p1._2 - p0._2
      math.sqrt(dx * dx + dy * dy)
    }

    points
      .filter(p => distance(p, s) <= leaderLength || distance(p, e) <= leaderLength)
      .map(p => universalBinIndexToTileIndex(level, p, maxBin, tms)._1)
      .distinct
  }
}
