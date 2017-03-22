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

import org.apache.spark.util.LongAccumulator
import org.apache.spark.rdd.RDD

/**
  * An object that can calculate the median of a dataset
  */
object MedianCalculator {
  def median (dataset: RDD[Double], r: Int): Double = {
    val sc = dataset.context
    val partitionMids: Array[Array[Double]] = dataset.mapPartitions { i =>
      // Find the median n numbers in this partition
      var root: TreeNode = null

      i.foreach(value =>
        if (null == root) {
          root = new LeafNode(value)
        } else {
          root = root.add(value)
        }
      )

      if (null == root) {
        List().iterator
      } else {
        val mid = root.size/2
        val rr = r min mid
        val mids: Array[Double] = (for (i <- -rr to rr) yield root.nth(mid + i)).toArray
        List(mids).iterator
      }
    }.collect()

    // Combine our partition medians, and see which is closest to the real median
    val potentialMids: Set[(Double, LongAccumulator, LongAccumulator)] =
      partitionMids.flatMap((a: Array[Double]) => a)
        .toSet
        .map((m: Double) => (m, sc.longAccumulator("potentialMidsAccumulatorLess"),sc.longAccumulator("potentialMidsAccumulatorMore")))


    dataset.foreach{n =>
      potentialMids.foreach{case (m, less, more) =>
        if (n < m) less.add(1)
        if (n > m) more.add(1)
      }
    }

    val sortedMids = potentialMids.toList.map{case (m, less, more) =>
      (m, (less.value - more.value).abs)
    }.sortBy(_._2)

    if (0 == sortedMids.length) {
      0.0
    } else if (1 == sortedMids.length) {
      sortedMids(0)._1
    }  else if (sortedMids(0)._2 == sortedMids(1)._2) {
      (sortedMids(0)._1 + sortedMids(1)._1)/2.0
    } else {
      sortedMids(0)._1
    }
  }
}
private[layout] trait TreeNode {
  var size: Int
  def leftBound: Double
  def rightBound: Double
  def add (value: Double): TreeNode
  def nth (n: Int): Double
}
private[layout] case class LeafNode (value: Double) extends TreeNode {
  var size: Int = 1
  def leftBound: Double = value
  def rightBound: Double = value
  def add (newValue: Double): TreeNode = {
    if (newValue < value) {
      new BranchNode(new LeafNode(newValue), this)
    }
    else {
      new BranchNode(this, new LeafNode(newValue))
    }
  }
  def nth (n: Int): Double = {
    if (0 == n) value else throw new IndexOutOfBoundsException("Non-zero index to leaf node");
  }
}
private[layout] class BranchNode (var left: TreeNode, var right: TreeNode) extends TreeNode {
  var size: Int = left.size + right.size
  def leftBound: Double = left.leftBound
  def rightBound: Double = right.rightBound
  def add (newValue: Double): TreeNode = {
    if (newValue <= left.rightBound) {
      left = left.add(newValue)
    } else if (newValue >= right.leftBound) {
      right = right.add(newValue)
    } else if (left.size < right.size) {
      left = left.add(newValue)
    } else {
      right = right.add(newValue)
    }
    size += 1
    this
  }
  def nth (n: Int): Double = {
    if (n < left.size) left.nth(n) else right.nth(n-left.size)
  }
}
