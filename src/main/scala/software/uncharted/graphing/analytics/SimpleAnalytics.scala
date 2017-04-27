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
import software.uncharted.salt.core.analytic.collection.TopElementsAggregator
import software.uncharted.salt.core.analytic.numeric.{MaxAggregator, MeanAggregator, MinAggregator, SumAggregator}

import scala.collection.Map
import com.typesafe.config.Config



class AggregatorBasedAnalytic[T] (base: Aggregator[Double, T, Double], c: Int, aggName: String) extends CustomGraphAnalytic[T] {
  override val name: String = s"$aggName column $c"
  override val column: Int = c
  override val aggregator: Aggregator[String, T, String] =
    new WrappingClusterAggregator(
      base,
      (input: String) => input.toDouble,
      (output: Double) => output.toString
    )

  /**
    * Take two processed, aggregated values, and determine the minimum value of the pair.
    */
  override def min(left: String, right: String): String = (left.toDouble min right.toDouble).toString

  /**
    * Take two processed, aggregated values, and determine the maximum value of the pair.
    */
  override def max(left: String, right: String): String = (left.toDouble max right.toDouble).toString

  /**
    * Initialize a new instance of the aggregator using configuration parameters.
    * @param configs Configuration to use for initialization
    * @return Configured instance
    */
  override def initialize(configs: Config): CustomGraphAnalytic[T] = this
}

class SumAnalytic0 extends AggregatorBasedAnalytic(SumAggregator, 0, "sum")
class SumAnalytic1 extends AggregatorBasedAnalytic(SumAggregator, 1, "sum")
class SumAnalytic2 extends AggregatorBasedAnalytic(SumAggregator, 2, "sum")
class SumAnalytic3 extends AggregatorBasedAnalytic(SumAggregator, 3, "sum")
class SumAnalytic4 extends AggregatorBasedAnalytic(SumAggregator, 4, "sum")
class SumAnalytic5 extends AggregatorBasedAnalytic(SumAggregator, 5, "sum")
class SumAnalytic6 extends AggregatorBasedAnalytic(SumAggregator, 6, "sum")
class SumAnalytic7 extends AggregatorBasedAnalytic(SumAggregator, 7, "sum")
class SumAnalytic8 extends AggregatorBasedAnalytic(SumAggregator, 8, "sum")
class SumAnalytic9 extends AggregatorBasedAnalytic(SumAggregator, 9, "sum")
class SumAnalytic10 extends AggregatorBasedAnalytic(SumAggregator, 10, "sum")

class MinAnalytic0 extends AggregatorBasedAnalytic(MinAggregator, 0, "min")
class MinAnalytic1 extends AggregatorBasedAnalytic(MinAggregator, 1, "min")
class MinAnalytic2 extends AggregatorBasedAnalytic(MinAggregator, 2, "min")
class MinAnalytic3 extends AggregatorBasedAnalytic(MinAggregator, 3, "min")
class MinAnalytic4 extends AggregatorBasedAnalytic(MinAggregator, 4, "min")
class MinAnalytic5 extends AggregatorBasedAnalytic(MinAggregator, 5, "min")
class MinAnalytic6 extends AggregatorBasedAnalytic(MinAggregator, 6, "min")
class MinAnalytic7 extends AggregatorBasedAnalytic(MinAggregator, 7, "min")
class MinAnalytic8 extends AggregatorBasedAnalytic(MinAggregator, 8, "min")
class MinAnalytic9 extends AggregatorBasedAnalytic(MinAggregator, 9, "min")
class MinAnalytic10 extends AggregatorBasedAnalytic(MinAggregator, 10, "min")

class MaxAnalytic0 extends AggregatorBasedAnalytic(MaxAggregator, 0, "max")
class MaxAnalytic1 extends AggregatorBasedAnalytic(MaxAggregator, 1, "max")
class MaxAnalytic2 extends AggregatorBasedAnalytic(MaxAggregator, 2, "max")
class MaxAnalytic3 extends AggregatorBasedAnalytic(MaxAggregator, 3, "max")
class MaxAnalytic4 extends AggregatorBasedAnalytic(MaxAggregator, 4, "max")
class MaxAnalytic5 extends AggregatorBasedAnalytic(MaxAggregator, 5, "max")
class MaxAnalytic6 extends AggregatorBasedAnalytic(MaxAggregator, 6, "max")
class MaxAnalytic7 extends AggregatorBasedAnalytic(MaxAggregator, 7, "max")
class MaxAnalytic8 extends AggregatorBasedAnalytic(MaxAggregator, 8, "max")
class MaxAnalytic9 extends AggregatorBasedAnalytic(MaxAggregator, 9, "max")
class MaxAnalytic10 extends AggregatorBasedAnalytic(MaxAggregator, 10, "max")


class MeanAnalytic0 extends AggregatorBasedAnalytic(MeanAggregator, 0, "mean")
class MeanAnalytic1 extends AggregatorBasedAnalytic(MeanAggregator, 1, "mean")
class MeanAnalytic2 extends AggregatorBasedAnalytic(MeanAggregator, 2, "mean")
class MeanAnalytic3 extends AggregatorBasedAnalytic(MeanAggregator, 3, "mean")
class MeanAnalytic4 extends AggregatorBasedAnalytic(MeanAggregator, 4, "mean")
class MeanAnalytic5 extends AggregatorBasedAnalytic(MeanAggregator, 5, "mean")
class MeanAnalytic6 extends AggregatorBasedAnalytic(MeanAggregator, 6, "mean")
class MeanAnalytic7 extends AggregatorBasedAnalytic(MeanAggregator, 7, "mean")
class MeanAnalytic8 extends AggregatorBasedAnalytic(MeanAggregator, 8, "mean")
class MeanAnalytic9 extends AggregatorBasedAnalytic(MeanAggregator, 9, "mean")
class MeanAnalytic10 extends AggregatorBasedAnalytic(MeanAggregator, 10, "mean")


/**
  * The category analytic counts the instances of categorized nodes.
  *
  * Each node must have a column which lists the categories it is in (comma-separated).  If only one category
  * per node is desired, then just listing said single category is sufficient.
  *
  * @param c
  */
class CategoryAnalytic (c: Int) extends CustomGraphAnalytic[Map[String, Int]] {
  override val name: String = s"category column $c"

  private def decode (encodedMap: String): Map[String, Int] = {
    encodedMap.split(",").map{entry =>
      val fields = entry.split(":")
      val key = fields(0).trim
      val value = fields(1).trim.toInt
      key -> value
    }.toMap
  }
  /**
    * Take two processed, aggregated values, and determine the maximum value of the pair.
    */
  override def max(left: String, right: String): String = {
    val leftSum = decode(left).map(_._2).sum
    val rightSum = decode(right).map(_._2).sum
    if (leftSum > rightSum) left else right
  }

  /**
    * Take two processed, aggregated values, and determine the minimum value of the pair.
    */
  override def min(left: String, right: String): String = {
    val leftSum = decode(left).map(_._2).sum
    val rightSum = decode(right).map(_._2).sum
    if (leftSum < rightSum) left else right
  }

  override val column: Int = c
  override val aggregator: Aggregator[String, Map[String, Int], String] =
    new WrappingClusterAggregator[Seq[String], Map[String, Int], List[(String, Int)]](
      new TopElementsAggregator[String](10),
      (input: String) => input.split(",").toSeq.filter(_.length > 0),
      (output: List[(String, Int)]) => output.map{case (key, value) => s"$key:$value"}.mkString(",")
    )

  /**
    * Initialize a new instance of the aggregator using configuration parameters.
    * @param configs Configuration to use for initialization
    * @return Configured instance
    */
  override def initialize(configs: Config): CustomGraphAnalytic[Map[String, Int]] = this
}

class CategoryAnalytic0 extends CategoryAnalytic(0)
class CategoryAnalytic1 extends CategoryAnalytic(1)
class CategoryAnalytic2 extends CategoryAnalytic(2)
class CategoryAnalytic3 extends CategoryAnalytic(3)
class CategoryAnalytic4 extends CategoryAnalytic(4)
class CategoryAnalytic5 extends CategoryAnalytic(5)
class CategoryAnalytic6 extends CategoryAnalytic(6)
class CategoryAnalytic7 extends CategoryAnalytic(7)
class CategoryAnalytic8 extends CategoryAnalytic(8)
class CategoryAnalytic9 extends CategoryAnalytic(9)
class CategoryAnalytic10 extends CategoryAnalytic(10)
