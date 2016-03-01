package software.uncharted.graphing.geometry

import org.scalatest.FunSuite
import org.scalatest.Matchers._

import scala.collection.Set

class CircleTests extends FunSuite {
  private val epsilon = 1E-12

  test("Distance to line") {
    Circle((0, 0), 3).distanceTo(Line(1, 2, 10)) should be ((math.sqrt(20) - 3) +- epsilon)
    Circle((0, 0), 4).distanceTo(Line(1, 2, 10)) should be ((math.sqrt(20) - 4) +- epsilon)
    Circle((0, 0), 5).distanceTo(Line(1, 2, 10)) should be (0.0)
  }

  test("line intersection") {
    assert(Set((6.0, 8.0), (6.0, -8.0)) === Circle((0, 0), 10).intersection(Line(1, 0, 6)).productIterator.toSet)
    assert(Set((2.0, 4.0), (4.0, 2.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(1, 1, 6)).productIterator.toSet)
    assert(Set((2.0, 0.0), (0.0, 2.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(1, 1, 2)).productIterator.toSet)
    assert(Set((0.0, 2.0), (2.0, 4.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(1, -1, -2)).productIterator.toSet)
    assert(Set((2.0, 0.0), (4.0, 2.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(1, -1, 2)).productIterator.toSet)

    assert(Set((2.0, 0.0), (2.0, 4.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(1, 0, 2)).productIterator.toSet)
    assert(Set((0.0, 2.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(1, 0, 0)).productIterator.toSet)
    assert(Set((4.0, 2.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(1, 0, 4)).productIterator.toSet)

    assert(Set((0.0, 2.0), (4.0, 2.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(0, 1, 2)).productIterator.toSet)
    assert(Set((2.0, 0.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(0, 1, 0)).productIterator.toSet)
    assert(Set((2.0, 4.0)) === Circle(2.0, 2.0, 2.0).intersection(Line(0, 1, 4)).productIterator.toSet)
  }
}
