package software.uncharted.graphing.geometry

/**
  * Created by nkronenfeld on 11/03/16.
  */
class ArcToPoints (start: (Int, Int), end: (Int, Int), arcLength: Double = math.Pi/3) {
  def all: Seq[(Int, Int)] = {
    val (x0, y0) = start
    val (x1, y1) = end
    val (xc, yc, radius, startSlope, endSlope, octants) = initializeArc(start, end)

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

  private def initializeArc (start: (Int, Int), end: (Int, Int)) = {
    val (x0, y0) = start
    val (x1, y1) = end
    val dx = x1 - x0
    val dy = y1 - y0
    val chordLength = math.sqrt(dx * dx + dy * dy)

    val radius = (chordLength / 2.0) / math.sin(arcLength/2.0)

    // Go from the midpoint of our chord to the midpoint of the circle
    // The amount by which to scale the radius to get to the center
    val chordRadiusScale = math.cos(arcLength/2.0)
    // The choice of signs here (for dx and dy) determines the direction of the arc as clockwise.  To
    // get a counter-clockwise arc, reverse the signs.
    val xc = (x0+x1)/2.0 + dy * chordRadiusScale
    val yc = (y0+y1)/2.0 - dx * chordRadiusScale

    // Find the relevant octants
    def findOctant (x: Double, y: Double, isStart: Boolean): Int = {
      if (x == 0.0)      if ((isStart && y >= 0.0) || (!isStart && y <= 0.0)) 0 else 4
      else if (y == 0.0) if ((isStart && x > 0.0) || (!isStart && x < 0.0)) 2 else 6
      else if (x > 0.0 && y > 0.0) if (x > y) 0 else 1
      else if (x < 0.0 && y > 0.0) if (y > -x) 2 else 3
      else if (x < 0.0 && y < 0.0) if (-x > -y) 4 else 5
      else if (-y > x) 6 else 7
    }

    val startOctant = findOctant(x0-xc, y0-yc, true)
    val endOctant = findOctant(x1-xc, y1-yc, false)
    val octants =
      (if (endOctant < startOctant) (endOctant to startOctant)
      else (endOctant to (startOctant+8)).map(_ % 8))
        .map(octant =>
          (octant, octant == startOctant, octant == endOctant))

    (xc,
      yc,
      radius,
      (y0-yc)/(x0-xc),
      (y1-yc)/(x1-xc),
      octants)
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
