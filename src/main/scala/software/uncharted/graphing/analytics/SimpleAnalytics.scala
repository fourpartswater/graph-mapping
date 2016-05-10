package software.uncharted.graphing.analytics

import software.uncharted.salt.core.analytic.Aggregator
import software.uncharted.salt.core.analytic.numeric.{MaxAggregator, MinAggregator, SumAggregator}

import scala.util.parsing.json.JSONObject

class AggregatorBasedAnalytic (base: Aggregator[Double, Double, Double], c: Int) extends CustomGraphAnalytic[Double, Double] {
  override val name: String = s"sum column $c"
  override val column: Int = c
  override val clusterAggregator: Aggregator[String, Double, String] =
    new WrappingClusterAggregator(
      base,
      (input: String) => input.toDouble,
      (output: Double) => output.toString
    )
  override val tileAggregator: Aggregator[String, Double, JSONObject] =
    new WrappingTileAggregator(
      base,
      (input: String) => input.toDouble,
      (output: Double) => new JSONObject(Map("value" -> output))
    )
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


