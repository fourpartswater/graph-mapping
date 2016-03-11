package software.uncharted.graphing.geometry

object ArcToPoints {
  /**
    * Find the point on a circle at the given angle
    */
  def getCirclePoint (center: (Int, Int), radius: Double, angle: Double) = {
    ((center._1 + radius * math.cos(angle)).round.toInt, (center._2 + radius * math.sin(angle)).round.toInt)
  }

  /**
    * Find the angular length of an arc with a given chord length (for a circle of a given radius)
    */
  def getArcLength (radius: Double, chordLength: Double): Double = {
    // sin(1/2 theta) = 1/s chord length / radius
    2.0 * math.asin(chordLength / (2.0 * radius))
  }

  /**
    * Get all the points on the arc from start to end of the given arc length
    *
    * @return
    */
  def getArcPoints (start: (Int, Int), end: (Int, Int), arcLength: Double = math.Pi/3): Seq[(Int, Int)] = {
    val (x0, y0) = start
    val (x1, y1) = end
    val ((xc, yc), radius, startSlope, endSlope, octants) = getArcCharacteristics(start, end, arcLength, true)

    // Offset from y from 0 so the y coordinate is the center of its column.
    var yOffset = math.round(yc) - yc
    var y = yOffset
    // x1^2 = x0^2 - 2 y0 dy - dy^2, and y0 = 0
    var x2 = radius*radius - yOffset*yOffset
    var x = math.sqrt(x2)

    new WhileIterator(
      () => x >= y,
      () => {
        val curX = x
        val curY = y

        x2 = x2 - 2 * y - 1
        y = y + 1
        var nextX = math.round(x)-0.5
        if (x2 <= nextX*nextX) x = x - 1

        (math.round(curX).toInt, math.round(curY).toInt)
      }
    ).flatMap{case (x, y) =>
      octants.flatMap{octant =>
        val (xr, yr) = octantTransform(x, y, octant._1)
        val slope = yr.toDouble/xr
        if ((octant._2 && slope <= startSlope) ||
          (octant._3 && slope >= endSlope) ||
          (!(octant._2 || octant._3))) {
          Some((math.round(xc+xr).toInt, math.round(yc+yr).toInt))
        } else {
          None
        }
      }
    }.toSeq
  }


  /**
    * Get the defining characteristics of a given arc going clockwise from a given start point to a given end point
    *
    * @param start The starting point of the arc
    * @param end The ending point of the arc
    * @param arcLength The length of the arc, in radians.
    * @param clockwise If true, the arc is taken to go clockwise from start to end; if false, counterclockwise.
    * @return <ol>
    *           <li>The center point of the arc</li>
    *           <li>The radius of the arc</li>
    *           <li>The slope of the starting radius</li>
    *           <li>The slope of the ending radius</li>
    *           <li>The octants intersected by this arc (octant 0 is the one directly counter-clockwise from the x
    *           axis, and they continue counter-clockwise)</li>
    *           </ol>
    */
  def getArcCharacteristics (start: (Int, Int), end: (Int, Int), arcLength: Double, clockwise: Boolean) = {
    val (x0, y0) = start
    val (x1, y1) = end
    val dx = x1 - x0
    val dy = y1 - y0
    val chordLength = math.sqrt(dx * dx + dy * dy)

    val radius = (chordLength / 2.0) / math.sin(arcLength / 2.0)

    // Go from the midpoint of our chord to the midpoint of the circle
    // The amount by which to scale the chord to get to the center
    val chordElevationScale = math.tan(arcLength / 2.0)
    // The choice of signs here (for dx and dy) determines the direction of the arc as clockwise.  To
    // get a counter-clockwise arc, reverse the signs.
    val sign = if (clockwise) 1.0 else -1.0
    val xc = (x0 + x1) / 2.0 + sign * dy / (2.0 * chordElevationScale)
    val yc = (y0 + y1) / 2.0 - sign * dx / (2.0 * chordElevationScale)

    // Find the relevant octants
    def findOctant(x: Double, y: Double, isStart: Boolean): Int = {
      if (x == 0.0) if ((isStart && y >= 0.0) || (!isStart && y <= 0.0)) 0 else 4
      else if (y == 0.0) if ((isStart && x > 0.0) || (!isStart && x < 0.0)) 2 else 6
      else if (x > 0.0 && y > 0.0) if (x > y) 0 else 1
      else if (x < 0.0 && y > 0.0) if (y > -x) 2 else 3
      else if (x < 0.0 && y < 0.0) if (-x > -y) 4 else 5
      else if (-y > x) 6 else 7
    }

    val startOctant = findOctant(x0 - xc, y0 - yc, true)
    val endOctant = findOctant(x1 - xc, y1 - yc, false)

    val octants: Seq[(Int, Boolean, Boolean)] = {
      val rawOctants: Seq[Int] =
        if (clockwise) {
          if (endOctant < startOctant) endOctant to startOctant
          else endOctant to startOctant + 8
        } else {
          if (startOctant < endOctant) startOctant to endOctant
          else startOctant to endOctant + 8
        }

      rawOctants.map(_ % 8).map(octant => (octant, octant == startOctant, octant == endOctant))
    }

    (
      (xc, yc),
      radius,
      (y0 - yc) / (x0 - xc),
      (y1 - yc) / (x1 - xc),
      octants
      )
  }

  private def octantTransform (x: Int, y: Int, octant: Int): (Int, Int) =
    octant match {
      case 0 => (x, y)
      case 1 => (y, x)
      case 2 => (-y, x)
      case 3 => (-x, y)
      case 4 => (-x, -y)
      case 5 => (-y, -x)
      case 6 => (y, -x)
      case 7 => (x, -y)
    }

  private def rotate [@specialized(Double, Int) N: Numeric] (x: N, y: N, rotation: Int): (N, N) = {
    val num: Numeric[N] = implicitly[Numeric[N]]
    import num.mkNumericOps

    rotation match {
      case -6 => (-y, x)
      case -4 => (-x, -y)
      case -2 => (y, -x)
      case 0 => (x, y)
      case 2 => (-y, x)
      case 4 => (-x, -y)
      case 6 => (y, -x)
      case _ => throw new IllegalArgumentException("Bad rotation "+rotation)
    }
  }
  private def pairAbs [@specialized(Double, Int) N: Numeric] (pair: (N, N)): (N, N) = {
    val num: Numeric[N] = implicitly[Numeric[N]]
    (num.abs(pair._1), num.abs(pair._2))
  }
}

class WhileIterator[T] (more: () => Boolean, fcn: () => T) extends Iterator[T] {
  def hasNext: Boolean = more()
  def next(): T = fcn()
}
