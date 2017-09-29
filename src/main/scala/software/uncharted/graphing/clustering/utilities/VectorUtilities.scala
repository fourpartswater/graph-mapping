/**
  * Copyright (c) 2014-2017 Uncharted Software Inc. All rights reserved.
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
package software.uncharted.graphing.clustering.utilities


import scala.collection.mutable.{Buffer => MutableBuffer, Set => MutableSet}
import org.apache.spark.ml.linalg.{DenseVector, SparseVector, Vector, Vectors}

import scala.util.Random



/**
  * A repository of basic mathematical functions on Spark MLLib's Vector class
  */
object VectorUtilities {
  /*
   * Combine three identically sized vectors, entry-by-entry.
   */
  private def trinaryVectorFunction (fcn: (Double, Double, Double) => Double)
                                    (a: Vector, b: Vector, c: Vector): Vector = {
    assert(a.size == b.size && a.size == c.size)

    (a, b, c) match {
      case (sa: SparseVector, sb: SparseVector, sc: SparseVector) =>
        val activeIndices = MutableSet[Int]()
        sa.foreachActive { case (i, v) => activeIndices += i }
        sb.foreachActive { case (i, v) => activeIndices += i }
        sc.foreachActive { case (i, v) => activeIndices += i }
        Vectors.sparse(a.size,
          activeIndices.map(i => (i, fcn(sa(i), sb(i), sc(i)))).toSeq
        )
      case _ =>
        Vectors.dense {
          (a.toArray zip b.toArray zip c.toArray).map { case ((aa, bb), cc) =>
            fcn(aa, bb, cc)
          }
        }
    }
  }

  /*
   * Combine two identically sized vectors, entry-by-entry.
   */
  private def binaryVectorFunction (fcn: (Double, Double) => Double)(a: Vector, b: Vector): Vector = {
    assert(a.size == b.size)

    (a, b) match {
      case (sa: SparseVector, sb: SparseVector) =>
        val activeIndices = MutableSet[Int]()
        sa.foreachActive { case (i, v) => activeIndices += i }
        sb.foreachActive { case (i, v) => activeIndices += i }
        Vectors.sparse(a.size,
          activeIndices.map(i => (i, fcn(sa(i), sb(i)))).toSeq
        )
      case _ =>
        Vectors.dense{
          (a.toArray zip b.toArray).map { case (aa, bb) => fcn(aa, bb) }
        }
    }
  }

  /*
   * Operate on a vector, entry-by-entry.
   */
  private def unaryVectorFunction (fcn: Double => Double)(a: Vector): Vector = {
    a match {
      case sa: SparseVector =>
        val resultValues = MutableBuffer[(Int, Double)]()
        sa.foreachActive { case (i, v) =>
          resultValues.append((i, fcn(v)))
        }
        Vectors.sparse(sa.size, resultValues)
      case _ =>
        Vectors.dense(a.toArray.map(fcn))
    }
  }

  /**
    * Add two vectors.  The length of the two vectors must be identical.
    *
    * @param a The first vector
    * @param b The second vector
    * @return A third vector whose entries are the sum of the corresponding entries of the two
    *         input vectors
    */
  def add (a: Vector, b: Vector): Vector = binaryVectorFunction(_ + _)(a, b)

  /**
    * Subtract one vector from another.  The length of the two vectors must be identical.
    *
    * @param a The minuend (the base vector)
    * @param b The subtrahend (the vector being subtracted out)
    * @return A third vector whose entries are the difference of the corresponding entries of
    *         the two input vectors
    */
  def subtract (a: Vector, b: Vector): Vector = binaryVectorFunction(_ - _)(a, b)

  /**
    * Multiply the corresponding entries of two vectors together.  The length of the two vectors
    * must be identical.
    *
    * @param a The first vector
    * @param b The second vector
    * @return A third vector whose entries are the product of the corresponding entries of the
    *         two input vectors.
    */
  def multiplyByEntry (a: Vector, b: Vector): Vector = binaryVectorFunction(_ * _)(a, b)

  /**
    * Divide the entries of one vector by the corresponding entries of another vector.  The length
    * length of the two vectors must be identical.
    *
    * @param a The dividend (numerator)
    * @param b The divisor (denominator)
    * @return A third vector whose entries are the quotient of the corresponding entries of the
    *         two input vectors.
    */
  def divideByEntry (a: Vector, b: Vector): Vector = binaryVectorFunction(_ / _)(a, b)

  /**
    * Find the minimum in each dimension of two vectors.  The length of the two vectors must be
    * identical.
    *
    * @param a The first vector
    * @param b The second vector
    * @return A third vector whose entries are the minimum of the corresponding entries of the
    *         two input vectors.
    */
  def min (a: Vector, b: Vector): Vector = binaryVectorFunction(_ min _)(a, b)

  /**
    * Find the maximum in each dimension of two vectors.  The length of the two vectors must be
    * identical.
    *
    * @param a The first vector
    * @param b The second vector
    * @return A third vector whose entries are the maximum of the corresponding entries of the
    *         two input vectors.
    */
  def max (a: Vector, b: Vector): Vector = binaryVectorFunction(_ max _)(a, b)

  /**
    * Scale a vector by a scalar.
    *
    * @param a The vector
    * @param b The scalar
    * @return Another vector, the input <code>a</code> scaled by <code>b</code>
    */
  def scale (a: Vector, b: Double): Vector = unaryVectorFunction(_ * b)(a)

  /**
    * Return a random vector, with each entry bounded by the corresponding entries in the
    * specified maxV and minV vectors
    *
    * @param r A random number generator
    * @param minV A vector specifying the minimum value in each dimension
    * @param maxV A vector specifying the maximum value in each dimension
    * @return
    */
  def randomVector (r: Random)(minV: Vector, maxV: Vector): Vector = {
    val range = subtract(maxV, minV);
    add(
      minV,
      unaryVectorFunction(entry => r.nextDouble() * entry)(range)
    )
  }

  /**
    * Take a vector, and translate and scale each entry so that they map [0, 1) in the
    * input vector to [minV_i, maxV_i)
    *
    * @param minV The lower bound of the scaled region.  The zero vector is mapped to this.
    * @param maxV The upper bound of the scaled region.  A vector of all 1s is mapped to this.
    * @param v The input vector
    * @return The scaled output vector
    */
  def toRange (minV: Vector, maxV: Vector)(v: Vector): Vector = {
    add(minV, multiplyByEntry(v, subtract(maxV, minV)))
  }

  /**
    * Take a vector, and translate and scale each entry so as to map [minV_i, maxV_i) in the
    * input vector to [0, 1).
    *
    * @param minV The lower bound of the scaled region.  The zero vector is mapped to this.
    * @param maxV The upper bound of the scaled region.  A vector of all 1s is mapped to this.
    * @param v The input vector
    * @return The scaled output vector
    */
  def fromRange (minV: Vector, maxV: Vector)(v: Vector): Vector = {
    trinaryVectorFunction{(minE, maxE, e) =>
      if (minE == maxE) {
        e
      } else {
        (e - minE) / (maxE - minE)
      }
    }(minV, maxV, v)
  }
}
