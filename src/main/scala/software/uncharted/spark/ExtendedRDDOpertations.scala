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
package software.uncharted.spark



import org.apache.spark.Partitioner
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag



/**
 * A set of operations to run on RDDs beyond the normal distributed with Spark.
 */
class ExtendedRDDOpertations[T: ClassTag] (rdd: RDD[T]) {

  //scalastyle:off method.length cyclomatic.complexity
  /**
   * Repartition an RDD into roughly equal-sized partitions, without changing order
   * @param partitions The number of partitions into which to divide the RDD
   * @param handleCaching Whether the operation should handle caching and uncaching pieces used multiply.  This should
   *                      be false if the input RDD is already cached.  If false, the output RDD will not be cached
   *                      either, or have a preliminary count done on it to solidify it.  If true, the input RDD will
   *                      be cached for processing (because multiple passes over it are needed for this operation),
   *                      processed, and repartitioned; the new RDD will be cached, counted (to solidify), and the
   *                      input RDD will be uncached.
   * @return A new RDD of the same type, with records in the same order, and with the given number of partitions, all
   *         as equal in size as possible.
   */
  def repartitionEqually(partitions: Int, handleCaching: Boolean = false): RDD[T] = {
    if (handleCaching) {
      rdd.cache
    }

    // Figure out the points at which to best cut our vertex list into the given number of partitions
    val partitionSizes = rdd.mapPartitionsWithIndex { case (partition, iter) =>
      Iterator((partition, iter.size))
    }.collect.toMap
    val totalSize = partitionSizes.values.fold(0)(_ + _)
    val cutIndices = (0 until partitions).map(n => (totalSize.toDouble * n.toDouble / partitions.toDouble).floor.toLong).toArray

    // Figure out, for each current partition, the partitions and indices within it at which to cut
    // This is all done locally, on the driver.
    var soFar = 0L
    val cutPoints = (0 until partitionSizes.size).map { currentPartition =>
      val start: Long = (0 until currentPartition).map(p => partitionSizes(p).toLong).fold(0L)(_ + _)
      val end: Long = start + partitionSizes(currentPartition)

      val newPartitions = (0 until cutIndices.size).filter { n =>
        val nS: Long = cutIndices(n)
        val nE: Long = if (n < (cutIndices.size - 1)) cutIndices(n + 1) else Long.MaxValue
        // We want segments with an endpoint inside our range, or surrounding our range
        ((start <= nS && nS < end) ||
          (start <= nE && nE < end) ||
          (nS <= start && end <= nE))
      }.map { newPartition =>
        val targetPartitionStart = cutIndices(newPartition)
        val targetPartitionEnd = if (newPartition == partitions - 1) totalSize else cutIndices(newPartition + 1)
        (newPartition, (targetPartitionStart - start).toInt, (targetPartitionEnd - start).toInt)
      }
      (currentPartition, newPartitions)
    }.toMap
    // Send the results out to workers
    val cutPointsB = rdd.context.broadcast(cutPoints)

    // Label our points by target partition
    val labelledRDD = rdd.mapPartitionsWithIndex { case (partition, pIter) =>
      val cutPointsForPartition = cutPointsB.value(partition)
      pIter.zipWithIndex.map { case (datum, index) =>
        val targetPartition = cutPointsForPartition.filter { case (p, start, end) =>
          start <= index && index < end
        }.head._1
        (targetPartition, datum)
      }
    }

    // Repartition using the stored value...
    val repartitionedRDD = labelledRDD.partitionBy(new KeyPartitioner(partitions))

    /// .. And, finally, remove the stored value
    val result = repartitionedRDD.map { case (partition, datum) => datum }

    // If we were told to handle caching, cache our output appropriately.
    if (handleCaching) {
      result.cache
      result.count
      rdd.unpersist(false)
    }

    result
  }
  //scalastyle:on method.length cyclomatic.complexity
}

object ExtendedRDDOpertations {
  implicit def getExtendedOperator[T: ClassTag] (rdd: RDD[T]): ExtendedRDDOpertations[T] = new ExtendedRDDOpertations[T](rdd)
}

/**
 * A partitioner that assumes an RDD[(Key, Value)], where the key is the partition into which each record should be
 * placed.
 *
 * @param size The number of partitions expected.  If any key is not in the range [0, size), behavior of this
 *             partitioner is underfined.
 */
private[spark] class KeyPartitioner(size: Int) extends Partitioner {
  override def numPartitions: Int = size

  override def getPartition(key: Any): Int = key.asInstanceOf[Int]
}
