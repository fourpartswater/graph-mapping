package software.uncharted.graphing.salt

import org.scalatest.FunSuite
import software.uncharted.graphing.geometry.Line._
import software.uncharted.graphing.geometry.{CartesianTileProjection2D, Line, LineToPoints}

/**
  * Created by nkronenfeld on 3/2/2016.
  */
class TwoStageLineProjectionTestSuite extends FunSuite {
  test("Leader line projection") {
    val maxBin = ((3, 3), (3, 3))
    val bounds = ((-40.0, -40.0), (40.0, 40.0))
    val leaderLength = 7
    val tms = false

    def checkSign(x: Int, sign: Int): Unit =
      if (0 != x) assert(sign === x.signum)
    val projection = new LeaderLineProjectionStageOne(Seq(4), leaderLength, bounds._1, bounds._2, tms)
    val bruteForce = new BruteForceLeaderLineReducer(maxBin._1, bounds, 4, leaderLength, tms)
    for (xm <- -1 to 1 by 2; ym <- -1 to 1 by 2) {
      for (x0 <- 0 to 20; y0 <- 0 to 20; x1 <- x0 until 40; y1 <- y0 until 40) {
        val tiles =
          try {
            projection.project(Some((xm * x0.toDouble, ym * y0.toDouble, xm * x1.toDouble, ym * y1.toDouble)), maxBin).get.map(_._1).toSet
          } catch {
            case e: Exception =>
              throw new Exception("Error getting tiles for line [%d, %d => %d, %d]".format(x0, y0, x1, y1), e)
          }
        val bruteForceTiles = bruteForce.getTiles(xm * x0, ym * y0, xm * x1, ym * y1).toSet
        assert(bruteForceTiles === tiles, "Points [%d, %d x %d, %d]".format(xm * x0, ym * y0, xm * x1, ym * y1))
      }
    }
  }

  // A single-instance check, to make it easier to find the problem when the above exhaustive check fails
  ignore("Single instance leader line projection") {
    val maxBin = ((3, 3), (3, 3))
    val bounds = ((-40.0, -40.0), (40.0, 40.0))
    val leaderLength = 7
    val tms = false

    val projection = new LeaderLineProjectionStageOne(Seq(4), leaderLength, bounds._1, bounds._2, tms)
    val bruteForce = new BruteForceLeaderLineReducer(maxBin._1, bounds, 4, leaderLength, tms)

    val x0 = -1
    val y0 = -2
    val x1 = -13
    val y1 = -16

    println("Checking [%d, %d -> %d, %d]".format(x0, y0, x1, y1))
    val bruteForceTiles = bruteForce.getTiles(x0, y0, x1, y1).toSet
    val tiles = projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin).get.map(_._1).toSet
    assert(bruteForceTiles === tiles, "Points [%d, %d x %d, %d]".format(x0, y0, x1, y1))
  }
}

