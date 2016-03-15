package software.uncharted.graphing.geometry

import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.matchers.{MatchResult, BeMatcher}

/**
  * Created by nkronenfeld on 14/03/16.
  */
class ArcBinnerTestSuite extends FunSuite {

  import ArcBinner._
  import Line.distance
  import Line.intPointToDoublePoint

  val epsilon = 1E-12

  implicit def toDoubleTupleMatcher(values: (Double, Double)) = new DoubleTupleMatcher(values, epsilon)

  test("Arc center") {
    getArcCenter((5.0, 0.0), (0.0, 5.0), math.Pi / 2, false) shouldBe ((0.0, 0.0) +- epsilon)
    getArcCenter((1.0, 5.0), (6.0, 0.0), math.Pi / 2, true) shouldBe ((1.0, 0.0) +- epsilon)
  }

  test("Arc radiius") {
    getArcRadius((10.0, 0.0), (5.0, 5.0), math.Pi / 2) should be(5.0 +- epsilon)
    getArcRadius((-2.0, 7.0), (3.0, 2.0), math.Pi / 2) should be(5.0 +- epsilon)
  }

  test("Quadrant determination") {
    assert(0 === getQuadrant(4, 0))
    assert(1 === getQuadrant(0, 4))
    assert(2 === getQuadrant(-4, 0))
    assert(3 === getQuadrant(0, -4))
  }

  test("Quadrant borderline determination") {
    assert(0 === getQuadrant(4, 4))
    assert(1 === getQuadrant(-4, 4))
    assert(2 === getQuadrant(-4, -4))
    assert(3 === getQuadrant(4, -4))
  }

  test("Double-octant determination") {
    assert(0 === getDoubleOctant(4, 0))
    assert(1 === getDoubleOctant(4, 4))
    assert(2 === getDoubleOctant(0, 4))
    assert(3 === getDoubleOctant(-4, 4))
    assert(4 === getDoubleOctant(-4, 0))
    assert(5 === getDoubleOctant(-4, -4))
    assert(6 === getDoubleOctant(0, -4))
    assert(7 === getDoubleOctant(4, -4))
  }

  test("Double-octant borderline determination") {
    val s = math.sin(math.Pi * 0.125)
    val c = math.cos(math.Pi * 0.125)
    assert(0 === getDoubleOctant(c, s - epsilon))
    assert(1 === getDoubleOctant(s + epsilon, c))
    assert(2 === getDoubleOctant(-s + epsilon, c))
    assert(3 === getDoubleOctant(-c, s + epsilon))
    assert(4 === getDoubleOctant(-c, -s + epsilon))
    assert(5 === getDoubleOctant(-s - epsilon, -c))
    assert(6 === getDoubleOctant(s - epsilon, -c))
    assert(7 === getDoubleOctant(c, -s - epsilon))
  }

  test("Simple test of full arc") {
    val arcBinner = new ArcBinner((5, 5), (-5, 5), math.Pi / 2, false)
    var last: (Int, Int) = arcBinner.next()
    assert((5, 5) === last)
    println(last)

    while (arcBinner.hasNext) {
      val next = arcBinner.next()
      println(next)
      assert(distance(next, last) < math.sqrt(2) + epsilon)
      assert(distance(next, (0, 0)) < math.sqrt(2) * 5.5 + epsilon)
      last = next
    }
    assert((-5, 5) === last)
  }
}

case class DoubleTupleMatcher (right: DoubleTuple, epsilon: Double = 1E-12) extends BeMatcher[DoubleTuple] {
  def +- (newEpsilon: Double) = DoubleTupleMatcher(right, newEpsilon)

  override def apply (left: DoubleTuple): MatchResult = {
    MatchResult(
      (right.x - left.x).abs < epsilon && (right.y - left.y).abs < epsilon,
      left+" != "+right,
      left+" == "+right
    )
  }
}
