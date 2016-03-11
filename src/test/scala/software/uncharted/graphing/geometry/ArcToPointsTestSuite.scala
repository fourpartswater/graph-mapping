package software.uncharted.graphing.geometry

import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.matchers.{MatchResult, BeMatcher}

/**
  * Created by nkronenfeld on 11/03/16.
  */
class ArcToPointsTestSuite extends FunSuite {
  import ArcToPoints._

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
    val arcPoints = getArcPoints(getCirclePoint(center, radius, sa), getCirclePoint(center, radius, ea), relAngle.abs)
    import Line.intPointToDoublePoint
    arcPoints.foreach { point =>
      val d = Line.distance(center, point)
      assert (radius - 1.0 <= d && d <= radius + 1.0)
    }
  }

  test("Basic arc tests") {
    testArc((0, 0), 10, math.Pi/4, 3*math.Pi/4)
    testArc((0, 0), 10, 3*math.Pi/4, math.Pi/4)
  }


  test("Arc characteristic tests - direction") {
    ArcToPoints.getArcCharacteristics((-10, 5), (10, 5), 2*math.atan(2), true)  shouldBe
      ArcCharacteristicMatcher((0.0,  0.0), math.sqrt(125), -0.5, 0.5, Seq((0, false, true), (1, false, false), (2, false, false), (3, true, false)))
    ArcToPoints.getArcCharacteristics((-10, 5), (10, 5), 2*math.atan(2), false) shouldBe
      ArcCharacteristicMatcher((0.0, 10.0), math.sqrt(125), 0.5, -0.5, Seq((4, true, false), (5, false, false), (6, false, false), (7, false, true)))
    ArcToPoints.getArcCharacteristics((-5, 10), (5, 10), 2*math.atan(0.5), true ) shouldBe
      ArcCharacteristicMatcher((0.0, 0.0), math.sqrt(125), -2.0, 2.0, Seq((1, false, true), (2, true, false)))
    ArcToPoints.getArcCharacteristics((-5, 10), (5, 10), 2*math.atan(0.5), false) shouldBe
      ArcCharacteristicMatcher((0.0, 20.0), math.sqrt(125), 2.0, -2.0, Seq((5, true, false), (6, false, true)))

    ArcToPoints.getArcCharacteristics((5, 10), (5, -10), 2*math.atan(2), true)  shouldBe
      ArcCharacteristicMatcher((0.0, 0.0), math.sqrt(125), 2.0, -2.0, Seq((6, false, true), (7, false, false), (0, false, false), (1, true, false)))
    ArcToPoints.getArcCharacteristics((5, 10), (5, -10), 2*math.atan(2), false) shouldBe
      ArcCharacteristicMatcher((10.0, 0.0), math.sqrt(125), -2.0, 2.0, Seq((2, true, false), (3, false, false), (4, false, false), (5, false, true)))
    ArcToPoints.getArcCharacteristics((10, 5), (10, -5), 2*math.atan(0.5), true)  shouldBe
      ArcCharacteristicMatcher((0.0, 0.0), math.sqrt(125), 0.5, -0.5, Seq((7, false, true), (0, true, false)))
    ArcToPoints.getArcCharacteristics((10, 5), (10, -5), 2*math.atan(0.5), false)  shouldBe
      ArcCharacteristicMatcher((20.0, 0.0), math.sqrt(125), -0.5, 0.5, Seq((3, true, false), (4, false, true)))
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
    if (! inRange(right._1._1, left._1._1)) {
      MatchResult(false, "Mismatched center coordinate X: expected "+right._1._1+", got "+left._1._1, "Matched arc characteristics")
    } else if (! inRange(right._1._2, left._1._2)) {
      MatchResult(false, "Mismatched center coordinate Y: expected "+right._1._2+", got "+left._1._2, "Matched arc characteristics")
    } else if (! inRange(right._2, left._2)) {
      MatchResult(false, "Mismatched radius: expected " + right._2 + ", got " + left._2, "Matched arc characteristics")
    } else if (! inRange(right._3, left._3)) {
      MatchResult(false, "Mismatched initial slope: expected " + right._3 + ", got " + left._3, "Matched arc characteristics")
    } else if (! inRange(right._4, left._4)) {
      MatchResult(false, "Mismatched initial slope: expected " + right._4 + ", got " + left._4, "Matched arc characteristics")
    } else if (right._5.toList != left._5.toList) {
      MatchResult(false, "Mismatched octants: expected " + right._5+", got " + left._5, "Matched arc characteristics")
    } else {
      MatchResult(true, "Mismatched arc characteristics", "Matched arc characteristics")
    }
  }
}