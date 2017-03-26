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
package software.uncharted.graphing.clustering.experiments.partitioning.force




/**
  * Created by nkronenfeld on 2016-01-16.
  */
class Vector (private[force] val coords: List[Double]) extends Serializable {
  def degree: Int = coords.length

  //scalastyle:off method.name
  def + (that: Vector): Vector = {
    new Vector((this.coords zip that.coords).map(coordsN => coordsN._1 + coordsN._2))
  }

  def - (that: Vector): Vector = {
    new Vector((this.coords zip that.coords).map(coordsN => coordsN._1 - coordsN._2))
  }

  def unary_- : Vector = {
    new Vector(this.coords.map(c => -c))
  }

  def * (that: Double): Vector = {
    new Vector(this.coords.map(_ * that))
  }

  def / (that: Double): Vector = {
    new Vector(this.coords.map(_ / that))
  }

  def o (that: Vector): Double = {
    (this.coords zip that.coords).map(coordsN => coordsN._1 * coordsN._2).fold(0.0)(_ + _)
  }
  //scalastyle:on method.name

  def length: Double = math.sqrt(this o this)

  def lengthSquared: Double = (this o this)

  def apply (index: Int): Double = this.coords(index)

  def to (that: Vector): Vector = {
    if (this == that) {
      Vector.zeroVector(coords.length)
    } else {
      that - this
    }
  }

  def dimensions: Int = this.coords.length

  override def toString: String = this.coords.mkString("[", ", ", "]")

  override def equals (rawThat: Any): Boolean = {
    rawThat match {
      case that: Vector => this.coords == that.coords
      case _ => false
    }
  }

  override def hashCode(): Int = {
    this.coords.foldLeft(0) {(base, value) =>
      base.hashCode() * 31 + value.hashCode()
    }
  }
}

object Vector {
  def randomVector(dimensions: Int): Vector =
    new Vector((1 to dimensions).map(n => ThreadRandomizer.get.nextDouble).toList)

  def zeroVector(dimensions: Int): Vector =
    new Vector((1 to dimensions).map(n => 0.0).toList)

  def constantVector(dimensions: Int, value: Double): Vector =
    new Vector((1 to dimensions).map(n => value).toList)
}
