/**
  * Copyright (c) 2014-2016 Uncharted Software Inc. All rights reserved.
  *
  * Property of Uncharted(tm), formerly Oculus Info Inc.
  * http://uncharted.software/
  *
  * This software is the confidential and proprietary information of
  * Uncharted Software Inc. ("Confidential Information"). You shall not
  * disclose such Confidential Information and shall use it only in
  * accordance with the terms of the license agreement you entered into
  * with Uncharted Software Inc.
  */
package software.uncharted.graphing.layout

import scala.util.Random

/**
  * An affine 2-dimensional vector
  */
case class V2 (x: Double, y: Double) {
  def + (that: V2): V2 = V2(this.x + that.x, this.y + that.y) //scalastyle:ignore
  def - (that: V2): V2 = V2(this.x - that.x, this.y - that.y) //scalastyle:ignore
  def * (that: Double): V2 = V2(this.x * that, this.y * that) //scalastyle:ignore
  def / (that: Double): V2 = V2(this.x / that, this.y / that) //scalastyle:ignore
  def min (that: V2): V2 = V2(this.x min that.x, this.y min that.y)
  def max (that: V2): V2 = V2(this.x max that.x, this.y max that.y)
  def dot (that: V2): Double = this.x * that.x + this.y * that.y
  def lengthSquared: Double = this dot this
  def length: Double = math.sqrt(lengthSquared)
}
object V2 {
  def apply (tuple: (Double, Double)): V2 = V2(tuple._1, tuple._2)
  val zero = V2(0.0, 0.0)
  def unitVector (angle: Double): V2 = V2(math.cos(angle), math.sin(angle))
  def randomVector (random: Random): V2 = V2(random.nextDouble, random.nextDouble)
}
