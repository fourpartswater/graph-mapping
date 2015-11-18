/**
 * Copyright © 2014-2015 Uncharted Software Inc. All rights reserved.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 * http://uncharted.software/
 *
 * This software is the confidential and proprietary information of
 * Uncharted Software Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Uncharted Software Inc.
 */
package software.uncharted.spark



import scala.reflect.ClassTag

import org.scalatest.FunSuite

import org.apache.spark.{SparkContext, SharedSparkContext}
import org.apache.spark.rdd.RDD



object ExtendedRDDOperationsTestSuite {
  def mapToPartitions[K: ClassTag] (sc: SparkContext, data: Map[Int, Iterator[K]], optPartitions: Option[Int] = None): RDD[K] = {
    val partitions = optPartitions.getOrElse(data.map(_._1).reduce(_ max _)) + 1
    sc.parallelize(0 until partitions, partitions).mapPartitionsWithIndex { case (partition, i) =>
      data.get(partition).getOrElse(Iterator[K]())
    }
  }
  def collectByPartition[K: ClassTag] (data: RDD[K]): Map[Int, List[K]] = {
    data.mapPartitionsWithIndex{case (p, i) =>
      Iterator((p, i.toList))
    }.collect.toMap
  }
}
class ExtendedRDDOperationsTestSuite extends FunSuite with SharedSparkContext {
  import ExtendedRDDOpertations._
  import ExtendedRDDOperationsTestSuite._

  test("Simple test of repartitionEqually") {
    val data = mapToPartitions(sc,
      Map(
        1 -> Iterator("a", "b", "c", "d", "e"),
        4 -> Iterator("f", "g"),
        5 -> Iterator("h", "i", "j", "k", "l")
      ),
      Some(8)
    )
    val repartitioned = data.repartitionEqually(4)
    assert(4 === repartitioned.partitions.size)
    val partitions = collectByPartition(repartitioned)

    assert(List("a", "b", "c") === partitions(0))
    assert(List("d", "e", "f") === partitions(1))
    assert(List("g", "h", "i") === partitions(2))
    assert(List("j", "k", "l") === partitions(3))
    assert(4 === partitions.size)
  }

  test("Test repartitionEqually to make sure that all partitions' sizes are within one of each other") {
    val data = sc.parallelize(0 to 59)
    val repartitioned = data.repartitionEqually(8)
    val partitions = collectByPartition(repartitioned)
    partitions.foreach { case (partition, contents) =>
      assert(7 === contents.size || 8 === contents.size)
      println("Partition " + partition + ": " + contents)
    }
  }
}
