package software.uncharted.graphing.geometry

import org.scalatest.FunSuite

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
}
