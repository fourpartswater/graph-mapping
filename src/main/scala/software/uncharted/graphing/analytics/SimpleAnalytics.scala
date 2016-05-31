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
import software.uncharted.salt.core.analytic.numeric.{MeanAggregator, MaxAggregator, MinAggregator, SumAggregator}



class AggregatorBasedAnalytic[T] (base: Aggregator[Double, T, Double], c: Int) extends CustomGraphAnalytic[T] {
  override val name: String = s"sum column $c"
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
}

class SumAnalytic0 extends AggregatorBasedAnalytic(SumAggregator, 0)
class SumAnalytic1 extends AggregatorBasedAnalytic(SumAggregator, 1)
class SumAnalytic2 extends AggregatorBasedAnalytic(SumAggregator, 2)
class SumAnalytic3 extends AggregatorBasedAnalytic(SumAggregator, 3)
class SumAnalytic4 extends AggregatorBasedAnalytic(SumAggregator, 4)
class SumAnalytic5 extends AggregatorBasedAnalytic(SumAggregator, 5)
class SumAnalytic6 extends AggregatorBasedAnalytic(SumAggregator, 6)
class SumAnalytic7 extends AggregatorBasedAnalytic(SumAggregator, 7)
class SumAnalytic8 extends AggregatorBasedAnalytic(SumAggregator, 8)
class SumAnalytic9 extends AggregatorBasedAnalytic(SumAggregator, 9)
class SumAnalytic10 extends AggregatorBasedAnalytic(SumAggregator, 10)

class MinAnalytic0 extends AggregatorBasedAnalytic(MinAggregator, 0)
class MinAnalytic1 extends AggregatorBasedAnalytic(MinAggregator, 1)
class MinAnalytic2 extends AggregatorBasedAnalytic(MinAggregator, 2)
class MinAnalytic3 extends AggregatorBasedAnalytic(MinAggregator, 3)
class MinAnalytic4 extends AggregatorBasedAnalytic(MinAggregator, 4)
class MinAnalytic5 extends AggregatorBasedAnalytic(MinAggregator, 5)
class MinAnalytic6 extends AggregatorBasedAnalytic(MinAggregator, 6)
class MinAnalytic7 extends AggregatorBasedAnalytic(MinAggregator, 7)
class MinAnalytic8 extends AggregatorBasedAnalytic(MinAggregator, 8)
class MinAnalytic9 extends AggregatorBasedAnalytic(MinAggregator, 9)
class MinAnalytic10 extends AggregatorBasedAnalytic(MinAggregator, 10)

class MaxAnalytic0 extends AggregatorBasedAnalytic(MaxAggregator, 0)
class MaxAnalytic1 extends AggregatorBasedAnalytic(MaxAggregator, 1)
class MaxAnalytic2 extends AggregatorBasedAnalytic(MaxAggregator, 2)
class MaxAnalytic3 extends AggregatorBasedAnalytic(MaxAggregator, 3)
class MaxAnalytic4 extends AggregatorBasedAnalytic(MaxAggregator, 4)
class MaxAnalytic5 extends AggregatorBasedAnalytic(MaxAggregator, 5)
class MaxAnalytic6 extends AggregatorBasedAnalytic(MaxAggregator, 6)
class MaxAnalytic7 extends AggregatorBasedAnalytic(MaxAggregator, 7)
class MaxAnalytic8 extends AggregatorBasedAnalytic(MaxAggregator, 8)
class MaxAnalytic9 extends AggregatorBasedAnalytic(MaxAggregator, 9)
class MaxAnalytic10 extends AggregatorBasedAnalytic(MaxAggregator, 10)


class MeanAnalytic0 extends AggregatorBasedAnalytic(MeanAggregator, 0)
class MeanAnalytic1 extends AggregatorBasedAnalytic(MeanAggregator, 1)
class MeanAnalytic2 extends AggregatorBasedAnalytic(MeanAggregator, 2)
class MeanAnalytic3 extends AggregatorBasedAnalytic(MeanAggregator, 3)
class MeanAnalytic4 extends AggregatorBasedAnalytic(MeanAggregator, 4)
class MeanAnalytic5 extends AggregatorBasedAnalytic(MeanAggregator, 5)
class MeanAnalytic6 extends AggregatorBasedAnalytic(MeanAggregator, 6)
class MeanAnalytic7 extends AggregatorBasedAnalytic(MeanAggregator, 7)
class MeanAnalytic8 extends AggregatorBasedAnalytic(MeanAggregator, 8)
class MeanAnalytic9 extends AggregatorBasedAnalytic(MeanAggregator, 9)
class MeanAnalytic10 extends AggregatorBasedAnalytic(MeanAggregator, 10)
