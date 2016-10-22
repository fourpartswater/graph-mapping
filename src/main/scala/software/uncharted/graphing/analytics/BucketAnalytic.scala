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
package software.uncharted.graphing.analytics

import software.uncharted.salt.core.analytic.Aggregator
import com.typesafe.config.Config

class BucketAnalytic (c: Int, minValue: Double, maxValue: Double, bins: Int) extends CustomGraphAnalytic[Array[Int]] {
  val analyticKey = "analytic"
  val bucketKey = "bucket"
  val columnKey = "column"
  val minValueKey = "minValue"
  val maxValueKey = "maxValue"
  val binsKey = "bins"


  override val name: String = s"bucket column $c"

  def this() = this(0, 0, 0, 0)

  /**
    * Take two processed, aggregated values, and determine the maximum value of the pair.
    */
  override def max(left: String, right: String): String = {
    val leftValue = left.split(",").map(_.toInt).sum
    val rightValue = right.split(",").map(_.toInt).sum
    if (leftValue >= rightValue) left else right
  }

  /**
    * Take two processed, aggregated values, and determine the minimum value of the pair.
    */
  override def min(left: String, right: String): String = {
    val leftValue = left.split(",").map(_.toInt).sum
    val rightValue = right.split(",").map(_.toInt).sum
    if (leftValue < rightValue) left else right
  }

  override val column: Int = c
  override val aggregator: Aggregator[String, Array[Int], String] =
    new WrappingClusterAggregator[Double, Array[Int], Array[Int]](
      new BucketAggregator(minValue, maxValue, bins),
      (input: String) => input.toDouble,
      (output: Array[Int]) => output.mkString(",")
    )

  /**
    * Initialize a new instance of the aggregator using configuration parameters.
    * @param configs Configuration to use for initialization
    * @return Configured instance
    */
  override def initialize(configs: Config): CustomGraphAnalytic[Array[Int]] = {
    val analyticConfig = configs.getConfig(analyticKey)
    val bucketConfig = analyticConfig.getConfig(bucketKey)
    val c = bucketConfig.getInt(columnKey)
    val minValue = bucketConfig.getDouble(minValueKey)
    val maxValue = bucketConfig.getDouble(maxValueKey)
    val bins = bucketConfig.getInt(binsKey)

    new BucketAnalytic(c, minValue, maxValue, bins)
  }
}

class BucketAggregator (minValue: Double, maxValue: Double, bins: Int) extends Aggregator[Double, Array[Int], Array[Int]] {
  override def default(): Array[Int] = Array.fill(bins)(0)

  override def finish(intermediate: Array[Int]): Array[Int] = intermediate

  override def merge(left: Array[Int], right: Array[Int]): Array[Int] = {
    val result: Array[Int] = Array.fill(left.length)(0)
    for (i <- left.indices) result(i) = left(i) + right(i)
    result
  }

  override def add(current: Array[Int], next: Option[Double]): Array[Int] = {
    next.map{value =>
      val bin = (bins * (value - minValue) / (maxValue - minValue)).floor.toInt
      current(bin) = current(bin) + 1
    }
    current
  }
}

