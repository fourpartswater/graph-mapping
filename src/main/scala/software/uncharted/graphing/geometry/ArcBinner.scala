package software.uncharted.graphing.geometry

import scala.collection.mutable.{Stack => MutableStack}

/**
  * Created by nkronenfeld on 14/03/16.
  */
object ArcBinner {
  private val clockwiseQuarterTurn = DoubleRotation(0, 1, -1, 0)
  private val counterclockwiseQuarterTurn = DoubleRotation(0, -1, 1, 0)

  /**
    * Get the center point of an arc
    *
    * @param start The starting point of the arc
    * @param end The ending point of the arc
    * @param arcLength The length of the arc, in radians
    * @param clockwise true if the arc goes clockwise from the start to the end; false if it goes counter-clockwise.
    * @return
    */
  def getArcCenter (start: DoubleTuple, end: DoubleTuple, arcLength: Double, clockwise: Boolean): DoubleTuple = {
    val delta = end - start

    // Go from the midpoint of our chord to the midpoint of the circle
    // The amount by which to scale the chord to get to the center
    val chordElevationScale = math.tan(arcLength / 2.0)
    val rotation = if (clockwise) clockwiseQuarterTurn else counterclockwiseQuarterTurn
    (start + end) / 2.0 + rotation * delta / (2.0 * chordElevationScale)
  }

  /**
    * Get the radius of an arc
    *
    * @param start The starting point of the arc
    * @param end The ending point of the arc
    * @param arcLength The length of the arc, in radians
    */
  def getArcRadius (start: DoubleTuple, end: DoubleTuple, arcLength: Double): Double = {
    val delta = end - start
    val chordLength = delta.length

    (chordLength / 2.0) / math.sin(arcLength / 2.0)
  }

  /**
    * Get the diagonal quadrant of the cartesian plane in which a given point lies.
    *
    * Quadrant 0 is centered on the positive X axis, quadrant 1, the positive Y axis, quadrant 2, the negative X axis,
    * and quadrant 3, the negative Y axis
    */
  def getQuadrant (coords: DoubleTuple): Int = {
    getQuadrant(coords.x, coords.y)
  }
  def getQuadrant (x: Double, y: Double): Int = {
    if (0.0 == x && 0.0 == y) {
      0
    } else if (x > 0.0) {
      if (y > x) 1
      else if (y <= -x) 3
      else 0
    } else {
      if (y >= -x) 1
      else if (y < x) 3
      else 2
    }
  }

  /**
    * Get the pair of octants most centered on the given coordinates.  0 is centered on the positive x axis, 1 on the
    * positive x=y diagonal, 2 on the positive y axis, etc.
    */
  def getDoubleOctant (coords: DoubleTuple): Int = {
    getDoubleOctant(coords.x, coords.y)
  }
  def getDoubleOctant (x: Double, y: Double): Int = {
    if (0.0 == x && 0.0 == y) {
      0
    } else if (x >= 0 && y > 0) {
      if (x * doubleOctantRatio >= y) 0
      else if (x >= y * doubleOctantRatio) 1
      else 2
    } else if (x < 0 && y >= 0) {
      if (y * doubleOctantRatio > -x) 2
      else if (y >= -x * doubleOctantRatio) 3
      else 4
    } else if (x <= 0 && y < 0) {
      if (-x * doubleOctantRatio >= -y) 4
      else if (-x >= doubleOctantRatio * -y) 5
      else 6
    } else {
      if (-y * doubleOctantRatio >= x) 6
      else if (-y >= doubleOctantRatio * x) 7
      else 0
    }
  }
  private val doubleOctantRatio = math.tan(math.Pi/8)

  def getSlope (p0: DoubleTuple, p1: DoubleTuple): Double =
    (p0.y - p1.y) / (p0.x - p1.x)

  /**
    * Find the angle of a a given point on a circle around the origin
    */
  def getCircleAngle (point: DoubleTuple) =
    math.atan2(point.y, point.x)

  /**
    * Find the solution to x = value % modulus that is closest to base
    */
  def toClosestModulus (base: Double, value:Double, modulus: Double) = {
    val diff = value - base
    val moduli = (diff / modulus).round
    value - moduli * modulus
  }
}

/**
  * A class that can report the individual pixels on an arc from start to end of the given arc length
  *
  * @param start The starting point of the arc
  * @param end The ending point of the arc
  * @param arcLength The angular length of the arc, in radians
  * @param clockwise True if the arc moves clockwise, false if counterclockwse
  */
class ArcBinner (start: DoubleTuple, end: DoubleTuple, arcLength: Double, clockwise: Boolean) {

  import ArcBinner._
  import DoubleTuple._

  private val center = getArcCenter(start, end, arcLength, clockwise)
  private val radius = getArcRadius(start, end, arcLength)

  private val startQuadrant = getQuadrant(start - center)
  private val startDoubleOctant = getDoubleOctant(start - center)
  private val startSlope = getSlope(start, center)
  private val startAngle = getCircleAngle(start - center)
  private val forwardsQuadrantStack = MutableStack[Int]()

  private val endQuadrant = getQuadrant(end - center)
  private val endDoubleOctant = getDoubleOctant(end - center)
  private val endSlope = getSlope(end, center)
  private val endAngle = getCircleAngle(end - center)
  private val backwardsQuadrantStack = MutableStack[Int]()

  private val quadrantList =
    if (clockwise) {
      if (endQuadrant < startQuadrant) startQuadrant to endQuadrant by -1
      else if (endQuadrant > startQuadrant) startQuadrant to (endQuadrant - 4) by -1
      else if (startSlope < endSlope) startQuadrant to endQuadrant by -1
      else startQuadrant to (endQuadrant - 4) by -1
    } else {
      if (endQuadrant > startQuadrant) startQuadrant to endQuadrant
      else if (endQuadrant < startQuadrant) startQuadrant to (endQuadrant + 4)
      else if (startSlope > endSlope) startQuadrant to endQuadrant
      else startQuadrant to (endQuadrant + 4) by -1
    }.map(n => (n+8)% 4).toList

  private var x = 0
  private var x2 = 0.0
  private var x2min = 0.0
  private var x2max = 0.0
  private var y = 0
  private var y2 = 0.0
  private var y2min = 0.0
  private var y2max = 0.0
  private var quadrant = 0
  private var slope = 0.0
  toStart()

  def toStart(): Unit = {
    x = start.x.floor.toInt
    y = start.y.floor.toInt
    quadrant = startQuadrant
    slope = getSlope((x, y), center)

    forwardsQuadrantStack.clear()
    forwardsQuadrantStack.push(quadrant)
    backwardsQuadrantStack.clear()
    backwardsQuadrantStack.pushAll(quadrantList.reverse)

    recalculateSquaredBounds(xBounds = true, yBounds = true)
  }

  def toEnd(): Unit = {
    x = end.x.floor.toInt
    y = end.y.floor.toInt
    quadrant = endQuadrant
    slope = getSlope((x, y), center)

    forwardsQuadrantStack.clear()
    forwardsQuadrantStack.pushAll(quadrantList)
    backwardsQuadrantStack.clear()
    backwardsQuadrantStack.push(quadrant)


    recalculateSquaredBounds(xBounds = true, yBounds = true)
  }

  private def recalculateSquaredBounds(xBounds: Boolean = false, yBounds: Boolean = false): Unit = {
    if (xBounds) {
      x2min = (x - 0.5 - center.x) * (x - 0.5 - center.x)
      x2max = (x + 0.5 - center.x) * (x + 0.5 - center.x)
      x2 = (x - center.x) * (x - center.x)
    }

    if (yBounds) {
      y2min = (y - 0.5 - center.y) * (y - 0.5 - center.y)
      y2max = (y + 0.5 - center.y) * (y + 0.5 - center.y)
      y2 = (y - center.y) * (y - center.y)
    }
  }

  private def incrementX(positive: Boolean): Unit = {
    val sign = if (positive) 1 else -1
    y2 = y2 - sign * 2.0 * (x - center.x) - 1.0
    x = x + sign

    if (y2 < y2min) {
      y = y - 1
      y2max = y2min
      y2min = (y - 0.5 - center.y) * (y - 0.5 - center.y)
    } else if (y2 >= y2max) {
      y = y + 1
      y2min = y2max
      y2max = (y + 0.5 - center.y) * (y + 0.5 - center.y)
    }
    val newQuadrant = getQuadrant(x, y)
    if (newQuadrant != quadrant) {
      setNewQuadrant(newQuadrant)
      recalculateSquaredBounds(yBounds = true)
    }
  }

  private def incrementY(positive: Boolean): Unit = {
    val sign = if (positive) 1 else -1
    x2 = x2 - sign * 2.0 * y - 1.0
    y = y + sign

    if (x2 < x2min) {
      x = x - 1
      x2max = x2min
      x2min = (x - 0.5 - center.x) * (x - 0.5 - center.x)
    } else if (x2 >= x2max) {
      x = x + 1
      x2min = x2max
      x2max = (x + 0.5 - center.x) * (x + 0.5 - center.x)
    }
    val newQuadrant = getQuadrant(x, y)
    if (newQuadrant != quadrant) {
      setNewQuadrant(newQuadrant)
      recalculateSquaredBounds(xBounds = true)
    }
  }

  private def setNewQuadrant (newQuadrant: Int): Unit = {
    val forwards =
      if (newQuadrant == quadrant + 1 || newQuadrant == quadrant - 3) {
        !clockwise
      } else {
        clockwise
      }
    if (forwards) {
      forwardsQuadrantStack.push(newQuadrant)
      assert(quadrant == backwardsQuadrantStack.pop())
    }
    quadrant = newQuadrant
  }

  private def nextClockwise(): (Int, Int) = {
    val curPoint = (x, y)
    quadrant match {
      case 0 => incrementY(false)
      case 1 => incrementX(true)
      case 2 => incrementY(true)
      case 3 => incrementX(false)
    }
    curPoint
  }

  private def nextCounterclockwise(): (Int, Int) = {
    val curPoint = (x, y)
    quadrant match {
      case 0 => incrementY(true)
      case 1 => incrementX(false)
      case 2 => incrementY(false)
      case 3 => incrementX(true)
    }
    curPoint
  }

  def next(): (Int, Int) =
    if (clockwise) nextClockwise()
    else nextCounterclockwise()

  def previous(): (Int, Int) =
    if (clockwise) nextCounterclockwise()
    else nextClockwise()

  def hasNext: Boolean = {
    if (forwardsQuadrantStack.length == quadrantList.length) {
      if (clockwise) slope <= endSlope
      else slope >= endSlope
    } else false
  }

  def hasPrevious: Boolean = {
    if (backwardsQuadrantStack.length == quadrantList.length) {
      if (clockwise) slope >= startSlope
      else slope <= startSlope
    } else false
  }
}

object DoubleTuple {
  implicit def fromTupleOfDouble (tuple: (Double, Double)): DoubleTuple = new DoubleTuple(tuple._1, tuple._2)
  implicit def fromTupleOfInt (tuple: (Int, Int)): DoubleTuple = new DoubleTuple(tuple._1.toDouble, tuple._2.toDouble)
  implicit def toTupleOfDouble (tuple: DoubleTuple): (Double, Double) = (tuple.x, tuple.y)
}

case class DoubleTuple (x: Double, y: Double) {
  def +(that: DoubleTuple): DoubleTuple = DoubleTuple(this.x + that.x, this.y + that.y)

  def -(that: DoubleTuple): DoubleTuple = DoubleTuple(this.x - that.x, this.y - that.y)

  def *(that: DoubleTuple): DoubleTuple = DoubleTuple(this.x * that.x, this.y * that.y)

  def /(that: DoubleTuple): DoubleTuple = DoubleTuple(this.x / that.x, this.y / that.y)

  def / (that: Double): DoubleTuple = DoubleTuple(this.x / that, this.y / that)

  def length = math.sqrt(x * x + y * y)

  implicit def toTuple: (Double, Double) = (x, y)
}

case class DoubleRotation (r00: Double, r01: Double, r10: Double, r11: Double) {
  def rotate(v: DoubleTuple): DoubleTuple = this * v

  def *(v: DoubleTuple): DoubleTuple =
    DoubleTuple(r00 * v.x + r01 * v.y, r10 * v.x + r11 * v.y)
}