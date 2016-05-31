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
package software.uncharted.graphing.geometry



import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.matchers.{MatchResult, BeMatcher}



class ArcToPointsTestSuite extends FunSuite {
  import ArcToPoints._
  import Line.intPointToDoublePoint

  private def getRelativeAngle (baseAngle: Double, angle: Double) = {
    val simpleRelAngle = (angle - baseAngle) % 360.0
    if (simpleRelAngle > 180.0) simpleRelAngle - 360.0
    else if (simpleRelAngle <= -180.0) simpleRelAngle + 360.0
    else simpleRelAngle
  }

  private def testArc (center: (Int, Int), radius: Double, startAngle: Double, endAngle: Double) = {
    // Make sure the short way from startAngle to endAngle is clockwise (i.e., negative)
    val relAngle = getRelativeAngle(startAngle, endAngle)
    val (sa, ea) =
      if (relAngle >= 0) (endAngle, startAngle)
      else (startAngle, endAngle)
    val arcPoints = getArcPointsFromEndpoints(getCirclePoint(center, radius, sa), getCirclePoint(center, radius, ea), relAngle.abs)
    import Line.intPointToDoublePoint
    arcPoints.foreach { point =>
      val d = Line.distance(center, point)
      assert (radius - 1.0 <= d && d <= radius + 1.0)
    }
  }

  test("Closest modulus tests") {
    assert(3.0 === toClosestModulus(5.0, -5.0, 8.0))
    assert(4.0 === toClosestModulus(5.0, -4.0, 8.0))
    assert(5.0 === toClosestModulus(5.0, -3.0, 8.0))
    assert(6.0 === toClosestModulus(5.0, -2.0, 8.0))
    assert(7.0 === toClosestModulus(5.0, -1.0, 8.0))
    assert(8.0 === toClosestModulus(5.0,  0.0, 8.0))
    assert(1.0 === toClosestModulus(5.0,  1.0, 8.0))
    assert(2.0 === toClosestModulus(5.0,  2.0, 8.0))
    assert(3.0 === toClosestModulus(5.0,  3.0, 8.0))
    assert(4.0 === toClosestModulus(5.0,  4.0, 8.0))
    assert(5.0 === toClosestModulus(5.0,  5.0, 8.0))
    assert(6.0 === toClosestModulus(5.0,  6.0, 8.0))
    assert(7.0 === toClosestModulus(5.0,  7.0, 8.0))
    assert(8.0 === toClosestModulus(5.0,  8.0, 8.0))
    assert(1.0 === toClosestModulus(5.0,  9.0, 8.0))
    assert(2.0 === toClosestModulus(5.0, 10.0, 8.0))
    assert(3.0 === toClosestModulus(5.0, 11.0, 8.0))
  }

  test("Test angle of point on circle from center") {
    val pi = math.Pi
    assert(-3 * pi / 4 === getCircleAngle((0, 0), (-5, -5)))
    assert(-2 * pi / 4 === getCircleAngle((0, 0), (0, -5)))
    assert(-1 * pi / 4 === getCircleAngle((0, 0), (5, -5)))
    assert( 0 * pi / 4 === getCircleAngle((0, 0), (5, 0)))
    assert( 1 * pi / 4 === getCircleAngle((0, 0), (5, 5)))
    assert( 2 * pi / 4 === getCircleAngle((0, 0), (0, 5)))
    assert( 3 * pi / 4 === getCircleAngle((0, 0), (-5, 5)))
    assert( 4 * pi / 4 === getCircleAngle((0, 0), (-5, 0)))
  }

  test("Basic arc tests") {
    testArc((0, 0), 10, math.Pi/4, 3*math.Pi/4)
    testArc((0, 0), 10, 3*math.Pi/4, math.Pi/4)
  }


  test("Arc characteristic tests - direction") {
    val endpoint = true
    val midpoint = false
    val clockwise = true
    val counterclockwise = false

    ArcToPoints.getArcCharacteristics((-10, 5), (10, 5), 2*math.atan(2), clockwise)  shouldBe
      ArcCharacteristicMatcher((0.0,  0.0), math.sqrt(125), -0.5, 0.5, Seq((0, midpoint, endpoint), (1, midpoint, midpoint), (2, midpoint, midpoint), (3, endpoint, midpoint)))
    ArcToPoints.getArcCharacteristics((-10, 5), (10, 5), 2*math.atan(2), counterclockwise) shouldBe
      ArcCharacteristicMatcher((0.0, 10.0), math.sqrt(125), 0.5, -0.5, Seq((4, endpoint, midpoint), (5, midpoint, midpoint), (6, midpoint, midpoint), (7, midpoint, endpoint)))
    ArcToPoints.getArcCharacteristics((-5, 10), (5, 10), 2*math.atan(0.5), clockwise ) shouldBe
      ArcCharacteristicMatcher((0.0, 0.0), math.sqrt(125), -2.0, 2.0, Seq((1, midpoint, endpoint), (2, endpoint, midpoint)))
    ArcToPoints.getArcCharacteristics((-5, 10), (5, 10), 2*math.atan(0.5), counterclockwise) shouldBe
      ArcCharacteristicMatcher((0.0, 20.0), math.sqrt(125), 2.0, -2.0, Seq((5, endpoint, midpoint), (6, midpoint, endpoint)))

    ArcToPoints.getArcCharacteristics((5, 10), (5, -10), 2*math.atan(2), clockwise)  shouldBe
      ArcCharacteristicMatcher((0.0, 0.0), math.sqrt(125), 2.0, -2.0, Seq((6, midpoint, endpoint), (7, midpoint, midpoint), (0, midpoint, midpoint), (1, endpoint, midpoint)))
    ArcToPoints.getArcCharacteristics((5, 10), (5, -10), 2*math.atan(2), counterclockwise) shouldBe
      ArcCharacteristicMatcher((10.0, 0.0), math.sqrt(125), -2.0, 2.0, Seq((2, endpoint, midpoint), (3, midpoint, midpoint), (4, midpoint, midpoint), (5, midpoint, endpoint)))
    ArcToPoints.getArcCharacteristics((10, 5), (10, -5), 2*math.atan(0.5), clockwise)  shouldBe
      ArcCharacteristicMatcher((0.0, 0.0), math.sqrt(125), 0.5, -0.5, Seq((7, midpoint, endpoint), (0, endpoint, midpoint)))
    ArcToPoints.getArcCharacteristics((10, 5), (10, -5), 2*math.atan(0.5), counterclockwise)  shouldBe
      ArcCharacteristicMatcher((20.0, 0.0), math.sqrt(125), -0.5, 0.5, Seq((3, endpoint, midpoint), (4, midpoint, endpoint)))
  }
}

case class ArcCharacteristicMatcher (right: ((Double, Double), Double, Double, Double, Seq[(Int, Boolean, Boolean)]))
  extends BeMatcher[((Double, Double), Double, Double, Double, Seq[(Int, Boolean, Boolean)])]
{
  private val epsilon = 1E-12
  private def inRange (expected: Double, actual: Double) = {
    (expected - actual).abs < epsilon
  }

  override def apply(left: ((Double, Double), Double, Double, Double, Seq[(Int, Boolean, Boolean)])): MatchResult = {
    val matches = true
    val doesntMatch = false
    if (! inRange(right._1._1, left._1._1)) {
      MatchResult(doesntMatch, "Mismatched center coordinate X: expected "+right._1._1+", got "+left._1._1, "Matched arc characteristics")
    } else if (! inRange(right._1._2, left._1._2)) {
      MatchResult(doesntMatch, "Mismatched center coordinate Y: expected "+right._1._2+", got "+left._1._2, "Matched arc characteristics")
    } else if (! inRange(right._2, left._2)) {
      MatchResult(doesntMatch, "Mismatched radius: expected " + right._2 + ", got " + left._2, "Matched arc characteristics")
    } else if (! inRange(right._3, left._3)) {
      MatchResult(doesntMatch, "Mismatched initial slope: expected " + right._3 + ", got " + left._3, "Matched arc characteristics")
    } else if (! inRange(right._4, left._4)) {
      MatchResult(doesntMatch, "Mismatched initial slope: expected " + right._4 + ", got " + left._4, "Matched arc characteristics")
    } else if (right._5.toList != left._5.toList) {
      MatchResult(doesntMatch, "Mismatched octants: expected " + right._5+", got " + left._5, "Matched arc characteristics")
    } else {
      MatchResult(matches, "Mismatched arc characteristics", "Matched arc characteristics")
    }
  }

  // TODO: Test arcs to and from horizontal and vertical lines
}
