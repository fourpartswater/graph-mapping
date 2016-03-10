package software.uncharted.graphing.salt

import org.scalatest.FunSuite

/**
  * Created by nkronenfeld on 08/03/16.
  */
class OneStageLineProjectionTestSuite extends FunSuite {
  test("Test lots of possible leader lines") {
    val maxBin = (3, 3)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val leaderLength = 5
    val tms = false

    val projection = new SimpleLeaderLineProjection(Seq(4), leaderLength, bounds._1, bounds._2, tms)
    val bruteForce = new BruteForceLeaderLineReducer(maxBin, bounds, 4, leaderLength, tms)
    for (x0 <- -10 to 10; y0 <- -10 to 10; x1 <- -10 to 10; y1 <- -10 to 10) {
      if (x0 != x1 || y0 != y1) {
        try {
          val usingProjection = projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin).get.sorted.toList
          val usingBruteForce = bruteForce.getBins(x0, y0, x1, y1).sorted.toList
          assert(usingBruteForce === usingProjection, "Points [%d, %d x %d, %d]".format(x0, y0, x1, y1))
        } catch {
          case e: Exception => throw new Exception("Error processing point [%d, %d x %d, %d".format(x0, y0, x1, y1), e)
        }
      }
    }
  }

  test("Test lots of possible full lines") {
    // 2x2-bin tiles and a data range of 32 should give us a data range of 1 per bin
    val maxBin = (1, 1)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val minLength = Some(3.0)
    val maxLength = Some(7.0)
    val tms = false

    val projection = new SimpleLineProjection(Seq(4), minLength, maxLength, bounds._1, bounds._2, tms)
    for (x0 <- -10 to 10; y0 <- -10 to 10; x1 <- -10 to 10; y1 <- -10 to 10) {
      if (x0 != x1 || y0 != y1) {
        try {
          val expectedLength = math.sqrt((x1 - x0) * (x1 - x0) + (y1 - y0) * (y1 - y0))
          val bins = projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin)
          if (expectedLength >= 3.0 && expectedLength <= 7.0) {
            assert(bins.get.length >= 3 && bins.get.length <= 7)
          } else {
            assert(bins.isEmpty)
          }
        } catch {
          case e: Exception => throw new Exception("Error processing point [%d, %d x %d, %d".format(x0, y0, x1, y1), e)
        }
      }
    }
  }

  // This test is here to activate in case anything fails in the previous test, so as to make debugging it simpler.
  ignore("Test a single case that failed in the exhaustive test above") {
    val (x0, y0, x1, y1) = (-10, -5, -9, -10)
    val maxBin = (3, 3)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val leaderLength = 5
    val tms = false

    val projection = new SimpleLeaderLineProjection(Seq(4), leaderLength, bounds._1, bounds._2, tms)
    val bruteForce = new BruteForceLeaderLineReducer(maxBin, bounds, 4, leaderLength, tms)

    val usingProjection = projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin).get.sorted.toList
    val usingBruteForce = bruteForce.getBins(x0, y0, x1, y1).sorted.toList
    assert(usingBruteForce === usingProjection, "Points [%d, %d x %d, %d]".format(x0, y0, x1, y1))
  }

  test("Test leader line fading spreader function") {
    val spreader = new FadingSpreadingFunction(4, (3, 3), false)
    assert(List(4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0, 4.0) === spreader.spread(Seq(
      ((2, 0, 0), (0, 0)), ((2, 0, 0), (1, 0)), ((2, 0, 0), (2, 0)), ((2, 0, 0), (3, 0)),
      ((2, 1, 0), (0, 0)), ((2, 1, 0), (1, 0)), ((2, 1, 0), (2, 0)), ((2, 1, 0), (3, 0))
    ), Some(4.0)).map(_._3.get).toList)

    assert(List(4.0, 3.0, 2.0, 1.0, 1.0, 2.0, 3.0, 4.0) === spreader.spread(Seq(
      ((2, 0, 0), (0, 0)), ((2, 0, 0), (1, 0)), ((2, 0, 0), (2, 0)), ((2, 0, 0), (3, 0)),
      ((2, 1, 0), (1, 0)), ((2, 1, 0), (2, 0)), ((2, 1, 0), (3, 0)), ((2, 2, 0), (0, 0))
    ), Some(4.0)).map(_._3.get).toList)
  }
}
