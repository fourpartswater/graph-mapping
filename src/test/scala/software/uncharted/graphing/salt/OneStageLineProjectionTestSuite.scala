/**
  * Copyright (c) 2014-2016 Uncharted Software Inc. All rights reserved.
  *
  * Property of Uncharted(tm), formerly Oculus Info Inc.
  * http://uncharted.software/
  *
  * This software is the confidential and proprietary information of
  * Uncharted Software Inc. ("Confidential Information"). You shall not
  * disclose such Confidential Information and shall use it only in
  * accordance with the terms of the license agreement you entered into
  * with Uncharted Software Inc.
  */
package software.uncharted.graphing.salt



import org.scalatest.FunSuite
import org.scalatest.Matchers._
import software.uncharted.graphing.geometry.{ArcToPoints, Line}



class OneStageLineProjectionTestSuite extends FunSuite {
  test("Test lots of possible leader lines") {
    val maxBin = (3, 3)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val leaderLength = 5
    val tms = false

    val projection = new SimpleLeaderLineProjection(Seq(4), bounds._1, bounds._2, leaderLength, tms = tms)
    val bruteForce = new BruteForceLeaderLineReducer(maxBin, bounds, 4, leaderLength, tms = tms)
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

  // This test is here to activate in case anything fails in the previous test, so as to make debugging it simpler.
  ignore("Test a single case that failed in the exhaustive leader line test") {
    val (x0, y0, x1, y1) = (-10, -5, -9, -10)
    val maxBin = (3, 3)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val leaderLength = 5

    val projection = new SimpleLeaderLineProjection(Seq(4), bounds._1, bounds._2, leaderLength, tms = true)
    val bruteForce = new BruteForceLeaderLineReducer(maxBin, bounds, 4, leaderLength, tms = true)

    val usingProjection = projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin).get.sorted.toList
    val usingBruteForce = bruteForce.getBins(x0, y0, x1, y1).sorted.toList
    assert(usingBruteForce === usingProjection, "Points [%d, %d x %d, %d]".format(x0, y0, x1, y1))
  }

  test("Test lots of possible full lines") {
    // 2x2-bin tiles and a data range of 32 should give us a data range of 1 per bin
    val maxBin = (1, 1)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val minLength = Some(3)
    val maxLength = Some(7)
    val tms = false

    val projection = new SimpleLineProjection(Seq(4), bounds._1, bounds._2, minLength, maxLength, tms = tms)
    for (x0 <- -10 to 10; y0 <- -10 to 10; x1 <- -10 to 10; y1 <- -10 to 10) {
      if (x0 != x1 || y0 != y1) {
        try {
          val expectedLineLength = math.sqrt((x1 - x0) * (x1 - x0) + (y1 - y0) * (y1 - y0))
          val bins = projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin)
          if (expectedLineLength >= 3.0 && expectedLineLength <= 7.0) {
            val expectedPoints = (x1 - x0).abs max (y1 - y0).abs
            assert(bins.get.length === expectedPoints + 1)
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
  ignore("Test a single case that failed in the exhaustive full line test") {
    val (x0, y0, x1, y1) = (-10, -10, -7, -7)
    // 2x2-bin tiles and a data range of 32 should give us a data range of 1 per bin
    val maxBin = (1, 1)
    val bounds = ((-16.0, -16.0), (16.0, 16.0))
    val minLength = Some(3)
    val maxLength = Some(7)
    val tms = false

    val projection = new SimpleLineProjection(Seq(4), bounds._1, bounds._2, minLength, maxLength, tms = tms)
    val expectedLineLength = math.sqrt((x1 - x0) * (x1 - x0) + (y1 - y0) * (y1 - y0))
    val bins = projection.project(Some((x0.toDouble, y0.toDouble, x1.toDouble, y1.toDouble)), maxBin)
    if (expectedLineLength >= 3.0 && expectedLineLength <= 7.0) {
      val expectedPoints = (x1 - x0).abs max (y1 - y0).abs
      assert(bins.get.length === expectedPoints + 1)
    } else {
      assert(bins.isEmpty)
    }
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

  test("Test arc leader lines projection - no gap") {
    import Line.distance
    import Line.intPointToDoublePoint

    val projection = new SimpleLeaderArcProjection(Seq(4), (0.0, 0.0), (64.0, 64.0), 8)
    val pointsOpt = projection.project(Some((24.0, 24.0, 36.0, 33.0)), (3, 3))
    // All are in universal bin coordinates, so Y axis is reversed from these calculations.
    // Also, clockwise becomes counterclockwise because of the y axis reversal
    // Points are (24, 24) and (36, 33), creating equilateral triangle arc
    // length is sqrt(12^2 + 9^2) = 15
    // center should be (30 - 0.6 * (7.5 * math.sqrt(3)), 28.5 + 0.8 * (7.5 * math.sqrt(3)))
    val start = (24, 64 - 24)
    val end = (36, 64 - 33)
    val center = (30.0 - 0.6 * 7.5 * math.sqrt(3), 64.0 - (28.5 + 0.8 * 7.5 * math.sqrt(3)))
    println(s"Start: $start")
    println(s"End: $end")
    println(s"Center: $center")

    assert(pointsOpt.isDefined)
    val points = pointsOpt.get
    points.foreach{point =>
      val (tile, bin) = point
      val uBin = (tile._2 * 4 + bin._1, tile._3 * 4 + bin._2)
      println(point+"\t"+uBin)
      distance(center, uBin) should be (15.0 +- math.sqrt(0.5))
      assert(distance(start, uBin) < 9.0 || distance(end, uBin) < 9.0)
      assert(start._1 <= uBin._1 && uBin._1 <= end._1)
      assert(start._2 >= uBin._2 && uBin._2 >= end._2)
    }
  }

  test("Test arc leader lines projection - with gap") {
    import Line.distance
    import Line.intPointToDoublePoint


    val projection = new SimpleLeaderArcProjection(Seq(4), (0.0, 0.0), (64.0, 64.0), 8)
    val pointsOpt = projection.project(Some((34.0, 17.0, 10.0, 7.0)), (3, 3))
    // All are in universal bin coordinates, so Y axis is reversed from these calculations.
    // Also, clockwise becomes counterclockwise because of the y axis reversal
    // Points are (34, 17), (10, 7), creating equilateral triangle arc
    // Length is 26
    // Center should be (22, 12) + (-5/13, 12/13) * 13 * sqrt(3)
    val start = (34, 64 - 17)
    val end = (10, 64 - 7)
    val center = (22.0 + 5.0 * math.sqrt(3.0), 64.0 - (12.0 - 12.0 * math.sqrt(3.0)))

    assert(pointsOpt.isDefined)
    val points = pointsOpt.get
    points.foreach{point =>
      val (tile, bin) = point
      val uBin = (tile._2 * 4 + bin._1, tile._3 * 4 + bin._2)
      distance(center, uBin) should be (26.0 +- math.sqrt(0.5))
      assert(distance(start, uBin) < 9.0 || distance(end, uBin) < 9.0)
      assert(end._1 <= uBin._1 && uBin._1 <= start._1)
      assert(uBin._2 >= start._2)
    }
  }
}
