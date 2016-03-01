package software.uncharted.graphing.geometry

/**
  * A line of the form Ax + By = C
  */
case class Line (A: Double, B: Double, C: Double) {
  override def equals(other: Any): Boolean = other match {
    case that: Line =>
      if (0.0 == this.C) {
        if (0.0 == this.B) {
          0.0 == that.C && 0.0 == that.B
        } else {
          this.A / this.B == that.A / that.B
        }
      } else {
        this.A / this.C == that.A / that.C && this.B / this.C == that.B / that.C
      }
  }

  def ~=(epsilon: Double)(that: Line): Boolean = {
    if (this.C.abs < epsilon) {
      if (this.B.abs < epsilon) {
        that.C.abs < epsilon && that.B.abs < epsilon
      } else {
        (this.A / this.B - that.A / that.B).abs < epsilon
      }
    } else {
      (this.A / this.C - that.A / that.C).abs < epsilon &&
        (this.B / this.C - that.B / that.C).abs < epsilon
    }
  }

  /** Get the Y coordinate of this line at the given X coordinate.  Undefined for vertical lines. */
  def yOf(x: Double): Double =
    if (0.0 == B) Double.NaN
    else (C - A * x) / B

  /** Get the X coordinate on this line at the given Y coordinate.  Undefined for horizontal lines. */
  def xOf(y: Double): Double =
    if (0.0 == A) Double.NaN
    else (C - B * y) / A

  /**
    * Get the intersection of two lines.  Return value is undefined if lines are parallel
    */
  def intersection (that: Line): (Double, Double) = {
    // A0 X + B0 Y = C0
    // A1 X + B1 Y = C1
    //
    // A0 A1 X + A1 B0 Y = A1 C0
    // A0 A1 X + A0 B1 Y = A0 C1
    // (A0 B1 - A1 B0) Y = (A0 C1 - A1 C0)
    // Y = (A0 C1 - A1 C0) / (A0 B1 - A1 B0)
    //
    // A0 B1 X + B0 B1 Y = B1 C0
    // A1 B0 X + B0 B1 Y = B0 C1
    // X = (B1 C0 - B0 C1) / (A0 B1 - A1 B0)
    val denom = this.A * that.B - that.A * this.B
    if (0.0 == denom) {
      throw new NoIntersectionException("Lines are parallel")
    } else {
      ((that.B * this.C - this.B * that.C) / denom, (this.A * that.C - that.A * this.C) / denom)
    }
  }

  /**
    * Get the intersection between this line and a circle.  Return value is undefined if there is no
    * intersection.
    */
  def intersection (that: Circle): ((Double, Double), (Double, Double)) =
    that.intersection(this)

  /** Get the distance from this line to a given point. */
  def distanceTo (x: Double, y: Double): Double = {
    val (perpA, perpB) = if (0 == A) (-B, A) else (B, -A)
    val closest = intersection(Line(perpA, perpB, perpA * x + perpB * y))
    val dx = x - closest._1
    val dy = y - closest._2
    math.sqrt(dx * dx + dy * dy)
  }

  /** Get the distance from this line to a given circle */
  def distanceTo (circle: Circle) = circle.distanceTo(this)
}

object Line {
  /** Create a line object from two points of the line. */
  def apply (pt0: (Double, Double), pt1: (Double, Double)): Line = {
    if (pt0 == pt1) throw new IllegalArgumentException("Can't make a line from a single point")

    val a = pt1._2 - pt0._2
    val b = pt0._1 - pt1._1
    val c = a * pt0._1 + b * pt0._2

    Line(a, b, c)
  }

  implicit def intPointToDoublePoint (pt: (Int, Int)): (Double, Double) = (pt._1.toDouble, pt._2.toDouble)

  def distance (pt0: (Double, Double), pt1: (Double, Double)): Double =
    math.sqrt(distanceSquared(pt0, pt1))

  def distanceSquared (pt0: (Double, Double), pt1: (Double, Double)): Double = {
    val (x0, y0) = pt0
    val (x1, y1) = pt1
    val dx = x1 - x0
    val dy = y1 - y0
    dx * dx + dy * dy
  }

  def distanceSquared (pt0: (Int, Int), pt1: (Int, Int)): Int = {
    val (x0, y0) = pt0
    val (x1, y1) = pt1
    val dx = x1 - x0
    val dy = y1 - y0
    dx * dx + dy * dy
  }
}