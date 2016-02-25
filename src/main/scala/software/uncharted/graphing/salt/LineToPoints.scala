package software.uncharted.graphing.salt


/**
  * This class handles converting endpoints to all points in a line.
  *
  * The basis of it is Bressenham's algorithm.
  *
  * Iteration uses Bressenham's algorithm as is, but we also extend it to allow jumps.
  */
class LineToPoints (start: (Int, Int), end: (Int, Int)) {
  private val totalLength = length(start, end)
  private val (steep, x0, y0, x1, y1) = {
    val (xs, ys) = start
    val (xe, ye) = end
    val steep = math.abs(ye - ys) > math.abs(xe - xs)

    if (steep) {
      if (ys > ye) (steep, ye, xe, ys, xs)
      else (steep, ys, xs, ye, xe)
    } else {
      if (xs > xe) (steep, xe, ye, xs, ys)
      else (steep, xs, ys, xe, ye)
    }
  }
  private val deltax = x1 - x0
  private val deltay = math.abs(y1 - y0)
  private val ystep = if (y0 < y1) 1 else -1
  private var error: Int = 0
  private var y: Int = 0
  private var x: Int = 0
  reset()


  /** Reset this object to start back at the start point of the line */
  def reset(): Unit = {
    error = deltax >> 1
    y = y0
    x = x0
  }

  /** Get the current position along the longer axis of the line */
  def curLongAxisPosition = x

  /** Get the distance along the longer axis from the start of the line */
  def curLongAxisStartDistance = x - x0

  /** Get the distance along the longer axis from the end of the line */
  def curLongAxisEndDistance = x1 - x

  /** Get the current distance from the start of the line */
  def curStartDistance = length((x, y), (x0, y0))

  /** Get the current distance from the end of the line */
  def curEndDistance = length((x, y), (x1, y1))

  /** See if there are more points over which to iterate for this line */
  def hasNext = x <= x1

  /** Get the number of points still available in this line */
  def available = (x1 - x + 1)

  /** Get the next point in this line */
  def next(): (Int, Int) = {
    assert(hasNext)
    val ourY = y
    val ourX = x
    error = error - deltay
    if (error < 0) {
      y = y + ystep
      error = error + deltax
    }
    x = x + 1

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
      val ySteps = ((deltax - 1 - errorL) / deltax).toInt
      error = (errorL + ySteps * deltax).toInt
      y = y + ySteps * ystep
      x = x + n
    }

    next()
  }

  /**
    * Skip some points, and return the one at the specified position on the long axis
    *
    * @param targetX The point along the longer axis of the line to which to skip
    * @return The point in the line with the given long-axis coordinate
    */
  def skipTo(targetX: Int): (Int, Int) = skip(targetX - x)

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

    // Line is _A x + _B y = 1
    // Circle is (x - xf)^2 + (y - yf)^2 = d^2
    // solve:
    //    x^2 - 2 x xf + xf^2 + y^2 - 2 y yf + yf^2 = d^2
    //    y = (1 - _A x) / _B
    // or (if _B = 0)
    //    x = (1 - _B y) / _A
    //
    //    x^2 - 2 x xf + xf^2 + ((1 - _A x) / _B)^2 - 2 ((1 - _A x) / _B) yf + yf^2 = d^2
    //    x^2 - 2 x xf + xf^2 + 1/_B^2 (1 - 2 _A x + _A^2 x^2) - 2 ((1 - _A x) / _B) yf + yf^2 = d^2
    //    x^2 - 2 x xf + xf^2 + 1/_B^2 - 2 _A/_B^2 x + (_A/_B)^2 x^2) - 2 ((1 - _A x) / _B) yf + yf^2 = d^2
    //    x^2 - 2 x xf + xf^2 + 1/_B^2 - 2 _A/_B^2 x + (_A/_B)^2 x^2) - 2 yf / _B + 2 _A/_B x yf + yf^2 = d^2
    //

    null
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
