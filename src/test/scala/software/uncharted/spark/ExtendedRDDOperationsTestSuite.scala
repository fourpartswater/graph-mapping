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



import org.scalatest.FunSuite

import org.apache.spark.SharedSparkContext

import software.uncharted.graphing.utilities.TestUtilities



class ExtendedRDDOperationsTestSuite extends FunSuite with SharedSparkContext {
  import TestUtilities._
  import ExtendedRDDOpertations._

  test("Simple test of repartitionEqually") {
    val data = parallelizePartitions(sc,
      Map(
        0 -> Seq[String](),
        1 -> Seq("a", "b", "c", "d", "e"),
        2 -> Seq[String](),
        3 -> Seq[String](),
        4 -> Seq("f", "g"),
        5 -> Seq("h", "i", "j", "k", "l"),
        6 -> Seq[String](),
        7 -> Seq[String]()
      )
    )
    val repartitioned = data.repartitionEqually(4)
    assert(4 === repartitioned.partitions.size)
    val partitions = collectPartitions(repartitioned)

    assert(List("a", "b", "c") === partitions(0))
    assert(List("d", "e", "f") === partitions(1))
    assert(List("g", "h", "i") === partitions(2))
    assert(List("j", "k", "l") === partitions(3))
    assert(4 === partitions.size)
  }

  test("Test repartitionEqually to make sure that all partitions' sizes are within one of each other") {
    val data = sc.parallelize(0 to 59)
    val repartitioned = data.repartitionEqually(8)
    val partitions = collectPartitions(repartitioned)
    partitions.foreach { case (partition, contents) =>
      assert(7 === contents.size || 8 === contents.size)
      println("Partition " + partition + ": " + contents)
    }
  }
}
