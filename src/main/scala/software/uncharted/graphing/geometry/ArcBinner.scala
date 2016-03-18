package software.uncharted.graphing.geometry

import scala.languageFeature.implicitConversions

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

case class SquareBounds (min: Double, current: Double, max: Double) {
  def asTuple = (min, current, max)
}

case class ArcPointInfo (point: (Int, Int), center: DoubleTuple, slope: Double, octant: Int, inBounds: Boolean,
                         private var xSquared: Option[SquareBounds], private var ySquared: Option[SquareBounds]) {
  def getXSquareBounds: SquareBounds = {
    if (xSquared.isEmpty) {
      val x = point._1
      val x2min = (x - 0.5 - center.x) * (x - 0.5 - center.x)
      val x2max = (x + 0.5 - center.x) * (x + 0.5 - center.x)
      val x2 = (x - center.x) * (x - center.x)
      xSquared = Some(SquareBounds(x2min, x2, x2max))
    }
    xSquared.get
  }
  def getYSquareBounds: SquareBounds = {
    if (ySquared.isEmpty) {
      val y = point._2
      val y2min = (y - 0.5 - center.y) * (y - 0.5 - center.y)
      val y2max = (y + 0.5 - center.y) * (y + 0.5 - center.y)
      val y2 = (y - center.y) * (y - center.y)
      ySquared = Some(SquareBounds(y2min, y2, y2max))
    }
    ySquared.get
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

  private val endOctant = getOctant(end - center)
  private val endSlope = getSlope(end, center)

  private val octantList =
    getInterveningSectors(startOctant, startSlope, endOctant, endSlope, 8, !clockwise)

  private def isInBounds (point: DoubleTuple, slope: Double, octant: Int): Boolean = {
    if (startOctant == endOctant) {
      if (octant != startOctant) false
      else if (clockwise) endSlope <= slope && slope <= startSlope
      else startSlope <= slope && slope <= endSlope
    } else if (octant == startOctant) {
      if (clockwise) slope <= startSlope
      else startSlope <= slope
    } else if (octant == endOctant) {
      if (clockwise) endSlope <= slope
      else slope <= endSlope
    } else if (octantList.contains(octant)) true
    else false
  }

  private def getInBoundsPointInfo (point: DoubleTuple) = {
    val floor = point.floor
    val isInBounds = true
    ArcPointInfo(floor, center, getSlope(floor, center), getOctant(floor - center), isInBounds, None, None)
  }

  val startPointInfo = getInBoundsPointInfo(start)
  val endPointInfo = getInBoundsPointInfo(end)

  var currentPoint: ArcPointInfo = null
  var nextPoint: Option[ArcPointInfo] = None
  var previousPoint: Option[ArcPointInfo] = None

  resetToStart()

  def resetToStart(): Unit = {
    currentPoint = startPointInfo
    nextPoint = Some(currentPoint)
    previousPoint = None
  }

  def resetToEnd(): Unit = {
    currentPoint = endPointInfo
    nextPoint = None
    previousPoint = Some(currentPoint)
  }

  private def incrementX (current: ArcPointInfo, positive: Boolean): ArcPointInfo = {
    val sign = if (positive) 1 else -1
    var (x, y) = current.point
    var (y2min, y2, y2max) = current.getYSquareBounds.asTuple

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

    val point = (x, y)
    val slope = getSlope(point, center)
    val octant = getOctant(point - center)
    val inBounds = isInBounds(point, slope, octant)
    ArcPointInfo((x, y), center, slope, octant, inBounds, None, Some(SquareBounds(y2min, y2, y2max)))
  }

  private def incrementY (current: ArcPointInfo, positive: Boolean): ArcPointInfo = {
    val sign = if (positive) 1 else -1
    var (x, y) = current.point
    var (x2min, x2, x2max) = current.getYSquareBounds.asTuple

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

    val point = (x, y)
    val slope = getSlope(point, center)
    val octant = getOctant(point - center)
    val inBounds = isInBounds(point, slope, octant)
    ArcPointInfo((x, y), center, slope, octant, inBounds, Some(SquareBounds(x2min, x2, x2max)), None)
  }

  private val positive = true
  private val negative = false
  private def nextClockwise(): ArcPointInfo = {
    currentPoint.octant match {
      case 7 => incrementY(currentPoint, negative)
      case 0 => incrementY(currentPoint, negative)
      case 1 => incrementX(currentPoint, positive)
      case 2 => incrementX(currentPoint, positive)
      case 3 => incrementY(currentPoint, positive)
      case 4 => incrementY(currentPoint, positive)
      case 5 => incrementX(currentPoint, negative)
      case 6 => incrementX(currentPoint, negative)
    }
  }

  private def nextCounterclockwise(): ArcPointInfo = {
    currentPoint.octant match {
      case 7 => incrementY(currentPoint, positive)
      case 0 => incrementY(currentPoint, positive)
      case 1 => incrementX(currentPoint, negative)
      case 2 => incrementX(currentPoint, negative)
      case 3 => incrementY(currentPoint, negative)
      case 4 => incrementY(currentPoint, negative)
      case 5 => incrementX(currentPoint, positive)
      case 6 => incrementX(currentPoint, positive)
    }
  }

  def hasNext: Boolean = {
    if (nextPoint.isEmpty) {
      nextPoint = Some(
        if (clockwise) nextClockwise()
        else nextCounterclockwise()
      )
    }
    nextPoint.get.inBounds
  }

  def hasPrevious: Boolean = {
    if (previousPoint.isEmpty) {
      previousPoint = Some(
        if (clockwise) nextCounterclockwise()
        else nextClockwise()
      )
    }
    previousPoint.get.inBounds
  }

  def next(): (Int, Int) = {
    if (hasNext) {
      previousPoint = Some(currentPoint)
      currentPoint = nextPoint.get
      nextPoint = None

      currentPoint.point
    } else throw new Exception("Attempt to get past start of arc")
  }

  def previous(): (Int, Int) = {
    if (hasPrevious) {
      nextPoint = Some(currentPoint)
      currentPoint = previousPoint.get
      previousPoint = None

      currentPoint.point
    } else throw new Exception("Attempt to get past end of arc")
  }

  def remaining: Iterator[(Int, Int)] = new Iterator[(Int, Int)] {
    override def hasNext: Boolean = ArcBinner.this.hasNext
    override def next(): (Int, Int) = ArcBinner.this.next()
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
  implicit def toTupleOfInt (tuple: DoubleTuple): (Int, Int) = (tuple.x.toInt, tuple.y.toInt)
}

case class DoubleTuple (x: Double, y: Double) {
  def +(that: DoubleTuple): DoubleTuple = DoubleTuple(this.x + that.x, this.y + that.y)

  def -(that: DoubleTuple): DoubleTuple = DoubleTuple(this.x - that.x, this.y - that.y)

  def *(that: DoubleTuple): DoubleTuple = DoubleTuple(this.x * that.x, this.y * that.y)

  def /(that: DoubleTuple): DoubleTuple = DoubleTuple(this.x / that.x, this.y / that.y)

  def / (that: Double): DoubleTuple = DoubleTuple(this.x / that, this.y / that)

  def length = math.sqrt(x * x + y * y)

  def floor = DoubleTuple(x.floor, y.floor)

  def ceil = DoubleTuple(x.ceil, y.ceil)
}

case class DoubleRotation (r00: Double, r01: Double, r10: Double, r11: Double) {
  def rotate(v: DoubleTuple): DoubleTuple = this * v

  def *(v: DoubleTuple): DoubleTuple =
    DoubleTuple(r00 * v.x + r01 * v.y, r10 * v.x + r11 * v.y)
}