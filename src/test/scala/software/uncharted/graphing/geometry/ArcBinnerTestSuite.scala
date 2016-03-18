package software.uncharted.graphing.geometry



import scala.languageFeature.implicitConversions
import org.scalatest.FunSuite
import org.scalatest.Matchers._
import org.scalatest.matchers.{MatchResult, BeMatcher}



/**
  * Created by nkronenfeld on 14/03/16.
  */
class ArcBinnerTestSuite extends FunSuite {
  private val  clockwise = true
  private val counterclockwise = false

  import ArcBinner._
  import Line.distance
  import Line.intPointToDoublePoint

  val epsilon = 1E-12

  implicit def toDoubleTupleMatcher(values: (Double, Double)): DoubleTupleMatcher = new DoubleTupleMatcher(values, epsilon)

  test("Arc center") {
    getArcCenter((5.0, 0.0), (0.0, 5.0), math.Pi / 2, counterclockwise) shouldBe ((0.0, 0.0) +- epsilon)
    getArcCenter((1.0, 5.0), (6.0, 0.0), math.Pi / 2, clockwise) shouldBe ((1.0, 0.0) +- epsilon)
  }

  test("Arc radiius") {
    getArcRadius((10.0, 0.0), (5.0, 5.0), math.Pi / 2) should be(5.0 +- epsilon)
    getArcRadius((-2.0, 7.0), (3.0, 2.0), math.Pi / 2) should be(5.0 +- epsilon)
  }

  test("Octant determination") {
    assert(0 === getOctant(4, 1))
    assert(1 === getOctant(1, 4))
    assert(2 === getOctant(-1, 4))
    assert(3 === getOctant(-4, 1))
    assert(4 === getOctant(-4, -1))
    assert(5 === getOctant(-1, -4))
    assert(6 === getOctant(1, -4))
    assert(7 === getOctant(4, -1))
  }

  test("Octant borderline determination") {
    assert(0 === getOctant(4, 4))
    assert(1 === getOctant(0, 4))
    assert(2 === getOctant(-4, 4))
    assert(3 === getOctant(-4, 0))
    assert(4 === getOctant(-4, -4))
    assert(5 === getOctant(0, -4))
    assert(6 === getOctant(4, -4))
    assert(7 === getOctant(4, 0))
  }

  test("Simple test of full arc, forward direction") {
    val arcBinner = new ArcBinner((5, 5), (-5, 5), math.Pi / 2, false)
    var last: (Int, Int) = arcBinner.next()
    assert((5, 5) === last)

    while (arcBinner.hasNext) {
      val next = arcBinner.next()
      assert(distance(next, last) < math.sqrt(2) + epsilon)
      assert(distance(next, (0, 0)) < math.sqrt(2) * 5.5 + epsilon)
      last = next
    }
    assert((-5, 5) === last)
  }

  test("test of iterable return, forward direction") {
    val arcBinner = new ArcBinner((5, 5), (-5, 5), math.Pi / 2, false)
    val points = arcBinner.remaining.toList
    assert((5, 5) === points.head)
    assert((-5, 5) === points.last)

    points.sliding(2).foreach { pair =>
      val first = pair.head
      val second = pair.last

      assert(distance(first, second) < math.sqrt(2) + epsilon)
      assert(distance(first, (0, 0)) < math.sqrt(2) * 5.5 + epsilon)
      assert(math.atan2(first._2, first._1) < math.atan2(second._2, second._1))
    }
  }

  test("Simple test of full arc, backward direction") {
    val arcBinner = new ArcBinner((5, 5), (-5, 5), math.Pi / 2, false)
    arcBinner.resetToEnd()
    var last: (Int, Int) = arcBinner.previous()
    assert((-5, 5) === last)

    while (arcBinner.hasPrevious) {
      val next = arcBinner.previous()
      assert(distance(next, last) < math.sqrt(2) + epsilon)
      assert(distance(next, (0, 0)) < math.sqrt(2) * 5.5 + epsilon)
      last = next
    }
    assert((5, 5) === last)
  }

  test("test of iterable return, backward direction") {
    val arcBinner = new ArcBinner((5, 5), (-5, 5), math.Pi / 2, false)
    arcBinner.resetToEnd()
    val points = arcBinner.preceding.toList
    assert((-5, 5) === points.head)
    assert((5, 5) === points.last)

    points.sliding(2).foreach{ pair =>
      val first = pair.head
      val second = pair.last

      assert(distance(first, second) < math.sqrt(2) + epsilon)
      assert(distance(first, (0, 0)) < math.sqrt(2) * 5.5 + epsilon)
      assert(math.atan2(first._2, first._1) > math.atan2(second._2, second._1))
    }
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
