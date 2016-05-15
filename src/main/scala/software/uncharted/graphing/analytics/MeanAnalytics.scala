//package software.uncharted.graphing.analytics
//
//import software.uncharted.salt.core.analytic.Aggregator
//import software.uncharted.salt.core.analytic.numeric.MeanAggregator
//
//import scala.util.parsing.json.JSONObject
//
//class MeanAnalytic  (c: Int) extends CustomGraphAnalytic[(Double, Int)] {
//  override val name: String = s"mean column $c"
//  override val column: Int = c
//  override val aggregator = new WrappingClusterAggregator(MeanAggregator, (s: String) => s.toDouble, (d: Double) => d.toString)
//
//  /**
//    * Take two processed, aggregated values, and determine the minimum value of the pair.
//    */
//  override def min(left: String, right: String): String = (left.toDouble min right.toDouble).toString
//
//  /**
//    * Take two processed, aggregated values, and determine the maximum value of the pair.
//    */
//  override def max(left: String, right: String): String = (left.toDouble max right.toDouble).toString
//}
//
//class MeanAnalytic0 extends MeanAnalytic(0)
//class MeanAnalytic1 extends MeanAnalytic(1)
//class MeanAnalytic2 extends MeanAnalytic(2)
//class MeanAnalytic3 extends MeanAnalytic(3)
//class MeanAnalytic4 extends MeanAnalytic(4)
//class MeanAnalytic5 extends MeanAnalytic(5)
//class MeanAnalytic6 extends MeanAnalytic(6)
//class MeanAnalytic7 extends MeanAnalytic(7)
//class MeanAnalytic8 extends MeanAnalytic(8)
//class MeanAnalytic9 extends MeanAnalytic(9)
//class MeanAnalytic10 extends MeanAnalytic(10)
