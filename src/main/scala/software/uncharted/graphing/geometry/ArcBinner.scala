package software.uncharted.graphing.geometry

import scala.languageFeature.implicitConversions
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
    * Get the octant of the cartesian plane in which a given poitn lies.
    *
    * Octant 0 goes from the positive x axis to the 45deg line x=y, octant 1 goes from that line to the positive y
    * axis, etc.
    *
    * Octants are closed on the counter-clockwise side, and open on the clockwise side.
    */
  def getOctant (coords: DoubleTuple): Int = {
    getOctant(coords.x, coords.y)
  }
  def getOctant(x: Double, y: Double): Int = {
    if (0.0 == x && 0.0 == y) {
      0
    } else if (x >= 0 && y > 0) {
      if (y > x) 1 else 0
    } else if (x < 0 && y >=0) {
      if (-x > y) 3 else 2
    } else if (x <= 0 && y < 0) {
      if (-y > -x) 5 else 4
    } else { // x > 0 && y <= 0
      if (x > -y) 7 else 6
    }
  }

  /**
    * Get the sectors between start and end, assuming a cyclical sector system (like quadrants or octants)
    *
    * @param startSector The first sector
    * @param withinStartSector A relative proportion of the way through the start sector of the start point.  Exact
    *                          proportion doesn't matter, as long as it is the correct direction from withinEndSector
    * @param endSector The last sector
    * @param withinEndSector A relative proportion of the way through the end sector of the end point.  Exact
    *                        proportion doesn't matter, as long as it is the correct direction from withinStartSector
    * @param numSectors The total number of sectors
    * @param positive True if we want the sectors from start to end travelling in the positive direction; false if the
    *                 negative direction.
    * @return All sectors from the first to the last, including both.
    */
  def getInterveningSectors (startSector: Int, withinStartSector: Double,
                             endSector: Int, withinEndSector: Double,
                             numSectors: Int, positive: Boolean): Seq[Int] = {
    if (positive) {
      if (endSector > startSector) startSector to endSector
      else if (endSector < startSector) startSector to (endSector + numSectors)
      else if (withinStartSector > withinEndSector) startSector to endSector
      else startSector to (endSector + numSectors)
    } else {
      if (endSector < startSector) startSector to endSector by -1
      else if (endSector > startSector) startSector to (endSector - numSectors) by -1
      else if (withinStartSector < withinEndSector) startSector to endSector by -1
      else startSector to (endSector - numSectors) by -1
    }.map(n => (n + 2 * numSectors) % numSectors)
  }

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

  private val startOctant = getOctant(start - center)
  private val startSlope = getSlope(start, center)
  // The list of octants going forwards around the arc from the current octant
  private val forwardsOctantStack = MutableStack[Int]()

  private val endOctant = getOctant(end - center)
  private val endSlope = getSlope(end, center)
  // The list of octants behind the current octant
  private val backwardsOctantStack = MutableStack[Int]()

  private val octantList =
    getInterveningSectors(startOctant, startSlope, endOctant, endSlope, 8, !clockwise)

  private var x = 0
  private var x2 = 0.0
  private var x2min = 0.0
  private var x2max = 0.0
  private var y = 0
  private var y2 = 0.0
  private var y2min = 0.0
  private var y2max = 0.0
  private var octant = 0
  private var slope = 0.0
  private var atStart = false
  private var atEnd = false
  resetToStart()

  def resetToStart(): Unit = {
    x = start.x.floor.toInt
    y = start.y.floor.toInt
    octant = startOctant
    slope = getSlope((x, y), center)
    atStart = true

    forwardsOctantStack.clear()
    forwardsOctantStack.pushAll(octantList.drop(1))
    backwardsOctantStack.clear()

    recalculateSquaredBounds(xBounds = true, yBounds = true)
  }

  def resetToEnd(): Unit = {
    x = end.x.floor.toInt
    y = end.y.floor.toInt
    octant = endOctant
    slope = getSlope((x, y), center)
    atEnd = true

    forwardsOctantStack.clear()
    backwardsOctantStack.clear()
    backwardsOctantStack.pushAll(octantList.reverse.drop(1))

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
    slope = getSlope((x, y), center)
    val newOctant = getOctant((x, y) - center)
    if (newOctant != octant) {
      setNewOctant(newOctant)
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
    slope = getSlope((x, y), center)
    val newOctant = getOctant((x, y) - center)
    if (newOctant != octant) {
      setNewOctant(newOctant)
      recalculateSquaredBounds(xBounds = true)
    }
  }

  private def setNewOctant (newOctant: Int): Unit = {
    val forwards =
      if (newOctant == octant + 1 || newOctant == octant - 7) {
        !clockwise
      } else {
        clockwise
      }
    if (forwards) {
      if (!hasNext) atEnd = true
      if (!atEnd) {
        assert(newOctant == forwardsOctantStack.pop())
        backwardsOctantStack.push(octant)
      }
    } else {
      if (!hasPrevious) atStart = true
      if (!atStart) {
        assert(newOctant == backwardsOctantStack.pop())
        forwardsOctantStack.push(octant)
      }
    }
    octant = newOctant
  }

  private def nextClockwise(): (Int, Int) = {
    val curPoint = (x, y)
    octant match {
      case 7 => incrementY(false)
      case 0 => incrementY(false)
      case 1 => incrementX(true)
      case 2 => incrementX(true)
      case 3 => incrementY(true)
      case 4 => incrementY(true)
      case 5 => incrementX(false)
      case 6 => incrementX(false)
    }
    curPoint
  }

  private def nextCounterclockwise(): (Int, Int) = {
    val curPoint = (x, y)
    octant match {
      case 7 => incrementY(true)
      case 0 => incrementY(true)
      case 1 => incrementX(false)
      case 2 => incrementX(false)
      case 3 => incrementY(false)
      case 4 => incrementY(false)
      case 5 => incrementX(true)
      case 6 => incrementX(true)
    }
    curPoint
  }

  def next(): (Int, Int) = {
    atStart = false
    if (clockwise) nextClockwise()
    else nextCounterclockwise()
  }

  def previous(): (Int, Int) = {
    atEnd = false
    if (clockwise) nextCounterclockwise()
    else nextClockwise()
  }

  def hasNext: Boolean = {
    if (forwardsOctantStack.isEmpty) {
      if (clockwise) slope >= endSlope
      else slope <= endSlope
    } else true
  }

  def remaining: Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    override def hasNext: Boolean = ArcBinner.this.hasNext
    override def next(): (Int, Int) = ArcBinner.this.next()
  }

  def hasPrevious: Boolean = {
    if (backwardsOctantStack.isEmpty) {
      if (clockwise) slope <= startSlope
      else slope >= startSlope
    } else true
  }

  def preceding: Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    override def hasNext: Boolean = ArcBinner.this.hasPrevious
    override def next(): (Int, Int) = ArcBinner.this.previous()
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