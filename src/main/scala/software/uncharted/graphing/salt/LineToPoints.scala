package software.uncharted.graphing.salt


/**
  * This class handles converting endpoints to all points in a line.
  *
  * The basis of it is Bressenham's algorithm.
  *
  * Iteration uses Bressenham's algorithm as is, but we also extend it to allow jumps.
  */
class LineToPoints (start: (Int, Int), end: (Int, Int)) {
  val totalLength = length(start, end)
  private val (steep, x0, y0, x1, y1) = {
    val (xs, ys) = start
    val (xe, ye) = end
    val steep = math.abs(ye - ys) > math.abs(xe - xs)

    if (steep) (steep, ys, xs, ye, xe)
    else (steep, xs, ys, xe, ye)
  }
  private val ystep = if (y0 < y1) 1 else -1
  private val xstep = if (x0 < x1) 1 else -1
  private val deltax = x1 - x0
  private val deltay = math.abs(y1 - y0)
  private var error: Int = 0
  private var y: Int = 0
  private var x: Int = 0
  reset()


  /** Reset this object to start back at the start point of the line */
  def reset(): Unit = {
    error = (deltax * xstep) >> 1
    y = y0
    x = x0
  }

  /** Get the current position along the longer axis of the line */
  def curLongAxisPosition = x

  /** Get the last position along the longer axis of the line */
  def lastLongAxisPosition = x1

  /** Get the distance along the longer axis from the start of the line */
  def curLongAxisStartDistance = x - x0

  /** Get the distance along the longer axis from the end of the line */
  def curLongAxisEndDistance = x1 - x

  /** Get the current distance from the start of the line */
  def curStartDistance = length((x, y), (x0, y0))

  /** Get the current distance from the end of the line */
  def curEndDistance = length((x, y), (x1, y1))

  /** Of an arbitrary pair of values, one for x, one for y, get the one that corresponds to the long axis */
  def longAxisValue[T] (values: (T, T)): T =
    if (steep) values._2 else values._1

  /** Of an arbitrary pair of values, one for x, one for y, get the one that corresponds to the short axis */
  def shortAxisValue[T] (values: (T, T)): T =
    if (steep) values._1 else values._2

  /** See if there are more points over which to iterate for this line */
  def hasNext = x * xstep <= x1 * xstep

  /** Get the number of points still available in this line */
  def available = (x1 - x) * xstep + 1

  /** Get the current point in this line */
  def current = (x, y)

  /** Get the next point in this line */
  def next(): (Int, Int) = {
    assert(hasNext)

    val ourY = y
    val ourX = x
    error = error - deltay

    // behavior at error == 0 differs if going up or down, so as to get the two to match exactly
    if ((1 == xstep && error < 0) || (-1 == xstep && error <= 0)) {
      y = y + ystep
      error = error + deltax * xstep
    }
    x = x + xstep

    val ev = (x, y, error)
    if (steep) (ourY, ourX)
    else (ourX, ourY)
  }

  /**
    * Skip some points, and return the one after those
    *
    * @param n The number of points to skip
    */
  def skip(n: Int): (Int, Int) = {
    if (n > 0) {
      // Convert to long to make sure to avoid overflow
      val errorL = error - deltay * n.toLong
      val ySteps = ((deltax * xstep - (if (1 == xstep) 1 else 0) - errorL) / (deltax * xstep)).toInt
      error = (errorL + ySteps * deltax * xstep).toInt
      y = y + ySteps * ystep
      x = x + n * xstep
    }

    next()
  }

  /**
    * Skip some points, and return the one at the specified position on the long axis
    *
    * @param targetX The point along the longer axis of the line to which to skip
    * @return The point in the line with the given long-axis coordinate
    */
  def skipTo(targetX: Int): (Int, Int) = skip((targetX - x) * xstep)

  /**
    * Return all points from the current point until we are farther than a specified distance from a reference point
    *
    * @param from The reference point from which distances are measured
    * @param distance The maximum distance a line point can be from the reference point to be returned
    * @return An iterator that will keep handing back more points until the they are too far from the reference
    *         point.  This iterator <em>will</em> affect this object - i.e., calling next() alternately on the iterator
    *         and the LineToPoint object from which the iterator was obtained will return alternate points on the line.
    */
  def toDistance(from: (Int, Int), distance: Double): Iterator[(Int, Int)] = {
    val referenceDSquared = distance * distance
    val referencePoint =
      if (steep) (from._2, from._1)
      else from

    new Iterator[(Int, Int)] {
      override def hasNext: Boolean = {
        if (!LineToPoints.this.hasNext) false
        else {
          val dSquared = lengthSquared((x, y), referencePoint)
          dSquared <= referenceDSquared
        }
      }

      override def next(): (Int, Int) = LineToPoints.this.next()
    }
  }

  /**
    * Skip points until we get to at or within the specified distance from a reference point
    *
    * @param from The reference point from which distances are measured
    * @param distance The maximum distance a line point can be from the reference point to be returned
    * @return The first point from current within the specified distance from the reference point
    */
  def skipToDistance (from: (Int, Int), distance: Double): (Int, Int) = {
    // Get the intersection point of the circle around <code>from</code> of the given radius, and our basic
    // underlying line

    val (xf, yf) = from
    val d = distance
    // Line is _A x + _B y = 1
    // Circle is (x - xf)^2 + (y - yf)^2 = d^2
    // solve:
    //    x^2 - 2 x xf + xf^2 + y^2 - 2 y yf + yf^2 = d^2
    //    y = (1 - _A x) / _B
    // or (if _B = 0)
    //    x = (1 - _B y) / _A
    //
    // if |_B| >>  0
    //    y = (1 - _A x) / _B = 1/_B - _A/_B x
    //    (x - xf)^2 + (1/_B - _A/_B x - yf)^2 = d^2
    //    (x - xf)^2 + (1/_B - yf - _A/_B x)^2 = d^2
    //    x^2 - 2 x xf + xf^2 + (1/_B - yf)^2 - 2 _A/_B (1/_B - yf) x + (_A/_B)^2 x^2 = d^2
    //    (1 + (_A/_B)^2) x^2 - 2 (xf + _A/_B(1/_B - yf)) x + xf^2 + (1/_B - yf)^2 - d^2 = 0
    //
    // if |_B| ~= 0
    //    x = (1 - _B y) / _A = 1/_A - _B/_A y
    //    (1/_A - _B/_A y - xf)^2 + (y - yf)^2 = d^2
    //    (y - yf)^2 + (1/_A - xf - _B/_A y)^2 = d^2
    //    y^2 - 2 yf y + yf^2 + (1/_A - xf)^2 - 2 _B/_A (1/_A - xf) y + (_B/_A)^2 y^2 = d^2
    //    (1 + (_B/_A)^2) y^2 - 2 (yf + _B/_A (1/_A - xf)) y + yf^2 + (1/_A - xf)^2 - d^2 = 0

    val (x1, y1, x2, y2) =
    if (_A.abs > _B.abs) {
      val BoverA = _B / _A
      val E = 1/_A - xf
      val a = 1 + BoverA * BoverA
      val b = -2 * (yf + BoverA * E)
      val c = yf * yf + E * E - d * d

      val determinate = math.sqrt(b * b - 4 * a * c)
      if (determinate.isNaN) throw new NoIntersectionException("Circle doesn't intersect line in the real plane (case A > B)")
      val y1 = (-b + determinate) / (2 * a)
      val x1 = (1 - _B * y1) / _A
      val y2 = (-b - determinate) / (2 * a)
      val x2 = (1 - _B * y2) / _A

      (x1, y1, x2, y2)
    } else {
      val AoverB = _A / _B
      val E = 1/_B - yf
      val a = 1 + AoverB * AoverB
      val b = -2 * (xf + AoverB * E)
      val c = xf * xf + E * E - d * d

      val determinate = math.sqrt(b * b - 4 * a * c)
      if (determinate.isNaN) throw new NoIntersectionException("Circle doesn't intersect line in the real plane (case B > A)")
      val x1 = (-b + determinate) / (2 * a)
      val y1 = (1 - _A * x1) / _B
      val x2 = (-b - determinate) / (2 * a)
      val y2 = (1 - _A * x2) / _B

      (x1, y1, x2, y2)
    }

    val (la1, la2) =
      if (steep) (y1, y2)
      else (x1, x2)

    def before (lhs: Double, rhs: Double) = lhs * xstep < rhs * xstep

    if (before(la1, x) && before(la2, x)) throw new NoIntersectionException("circle doesn't intersect plane after current point")
    else if (before(la1, x)) skipTo(la2.floor.toInt)
    else if (before(la2, x)) skipTo(la1.floor.toInt)
    else if (before(la1, la2)) skipTo(la1.floor.toInt)
    else skipTo(la2.floor.toInt)
  }

  // our line can be written in the form Ax + By = 1
  // Doing so requires the following values of A and B:
  private lazy val _denom = (x0 * y1 - y0 * x1).toDouble
  private lazy val _A = (y1 - y0) / _denom
  private lazy val _B = (x0 - x1) / _denom


  /** Get the next N points in this line */
  def next(n: Int): Array[(Int, Int)] = {
    assert(n <= available)

    val result = new Array[(Int, Int)](n)
    for (i <- 0 until n) result(i) = next()

    result
  }

  /** Get the rest of the points in this line */
  def rest(): Array[(Int, Int)] = next(available)


  private def lengthSquared(a: (Int, Int), b: (Int, Int)): Long = {
    val (xa, ya) = a
    val (xb, yb) = b
    val dx = (xb - xa).toLong
    val dy = (yb - ya).toLong
    dx * dx + dy * dy
  }

  private def length(a: (Int, Int), b: (Int, Int)): Double = math.sqrt(lengthSquared(a, b))
}

class NoIntersectionException (message: String, cause: Throwable) extends Exception(message, cause) {
  def this () = this(null, null)
  def this (message: String) = this(message, null)
  def this (cause: Throwable) = this(null, cause)
}