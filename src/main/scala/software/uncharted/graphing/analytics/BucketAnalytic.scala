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
package software.uncharted.graphing.analytics

import software.uncharted.salt.core.analytic.Aggregator
import com.typesafe.config.Config

class BucketAnalytic (c: Int, bins: Array[Bucket]) extends CustomGraphAnalytic[Array[Int]] {
  val analyticKey = "analytic"
  val bucketKey = "bucket"
  val columnKey = "column"
  val minValueKey = "minValue"
  val maxValueKey = "maxValue"
  val binsKey = "bins"
  val indexKey = "index"
  val bucketTypeKey = "type"
  val userBucketListKey = "bucket"


  override val name: String = s"bucket column $c"

  def this() = this(0, new Array[Bucket](0))

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
      new BucketAggregator(bins),
      (input: String) => input.toDouble,
      (output: Array[Int]) => output.mkString(",")
    )

  //Buckets can be either user defined or equal size. Equal size buckets have their limits calculated.
  //Expected formats:
  //    analytic {
  //      bucket {
  //        type = "user-defined"
  //        column = 2
  //        bucket: [
  //        {
  //          index = 0
  //          minValue = 0
  //          maxValue = 24
  //        },
  //        {
  //          index = 1
  //          minValue = 25
  //          maxValue = 49
  //        },
  //        {
  //          index = 2
  //          minValue = 50
  //          maxValue = 74
  //        },
  //        {
  //          index = 3
  //          minValue = 75
  //          maxValue = 99
  //        }
  //        ]
  //      }
  //    }
  //
  //    analytic {
  //      bucket {
  //        type = "equal-size"
  //        column = 2
  //        minValue = 0
  //        maxValue = 100
  //        bins = 4
  //      }
  //    }

  /**
    * Initialize a new instance of the aggregator using configuration parameters.
    * @param configs Configuration to use for initialization
    * @return Configured instance
    */
  override def initialize(configs: Config): CustomGraphAnalytic[Array[Int]] = {

    val analyticConfig = configs.getConfig(analyticKey)
    val bucketConfig = analyticConfig.getConfig(bucketKey)
    val bucketType = bucketConfig.getString(bucketTypeKey)
    val c = bucketConfig.getInt(columnKey)

    bucketType match {
      case "equal-size" =>
        //Every bucket is of equal size over the whole window defined in the config file.
        val minValue = bucketConfig.getDouble(minValueKey)
        val maxValue = bucketConfig.getDouble(maxValueKey)
        val binCount = bucketConfig.getInt(binsKey)

        val interval = ((maxValue - minValue) / binCount).ceil.toInt
        val buckets = new Array[Bucket](binCount)
        for ( i <- 0 to (buckets.length - 1)) {
          buckets(i) = new Bucket(minValue + i * interval, minValue + (i + 1) * interval - 1, i)
        }

        new BucketAnalytic(c, buckets)

      case "user-defined" =>
        //Series of buckets defined by the user in the config file.
        val binConfig = bucketConfig.getConfigList(userBucketListKey)
        val buckets = binConfig.toArray().map(c => c.asInstanceOf[Config])
          .map(b => new Bucket(b.getDouble(minValueKey), b.getDouble(maxValueKey), b.getInt(indexKey)))

        new BucketAnalytic(c, buckets)
      case _ => this
    }
  }
}

class BucketAggregator (bins: Array[Bucket]) extends Aggregator[Double, Array[Int], Array[Int]] {
  override def default(): Array[Int] = Array.fill(bins.length)(0)

  override def finish(intermediate: Array[Int]): Array[Int] = intermediate

  override def merge(left: Array[Int], right: Array[Int]): Array[Int] = {
    val result: Array[Int] = Array.fill(left.length)(0)
    for (i <- left.indices) result(i) = left(i) + right(i)
    result
  }

  override def add(current: Array[Int], next: Option[Double]): Array[Int] = {
    next.map{value =>
      val bin = bins.filter(b => b.withinBucket(value)).foreach(b => current(b.bucketIndex) = current(b.bucketIndex) + 1)
    }
    current
  }
}

class Bucket(val lowerLimit: Double, val upperLimit: Double, val bucketIndex: Int) {
  def withinBucket(value: Double): Boolean = {
    value >= lowerLimit && value <= upperLimit
  }
}

