package software.uncharted.graphing.analytics

import software.uncharted.salt.core.analytic.Aggregator

import scala.util.parsing.json.JSONObject

class MeanAnalytic  (c: Int) extends CustomGraphAnalytic[(Double, Int), (Double, Int)] {
  override val name: String = s"mean column $c"
  override val column: Int = c
  override val clusterAggregator: Aggregator[String, (Double, Int), String] =
    new Aggregator[String, (Double, Int), String] {
      override def default(): (Double, Int) = (0.0, 0)

      override def finish(intermediate: (Double, Int)): String = {
        val (sum, count) = intermediate
        s"$sum\t$count"
      }

      override def merge(left: (Double, Int), right: (Double, Int)): (Double, Int) =
        (left._1 + right._1, left._2 + right._2)

      override def add(current: (Double, Int), next: Option[String]): (Double, Int) =
        next.map(n => (current._1 + n.toDouble, current._2 + 1)).getOrElse(current)
    }
  override val tileAggregator: Aggregator[String, (Double, Int), JSONObject] =
    new Aggregator[String, (Double, Int), JSONObject] {
      override def default(): (Double, Int) = (0.0, 0)

      override def finish(intermediate: (Double, Int)): JSONObject = {
        val (sum, count) = intermediate
        val mean = sum / count
        new JSONObject(Map("mean" -> mean))
      }

      override def merge(left: (Double, Int), right: (Double, Int)): (Double, Int) =
        (left._1 + right._1, left._2 + right._2)

      override def add(current: (Double, Int), next: Option[String]): (Double, Int) =
        next.map { n =>
          val parts = n.split("\t")
          val Mean = parts(0).toDouble
          val count = parts(1).toInt
          (current._1 + Mean, current._2 + count)
        }.getOrElse(current)
    }
}

class MeanAnalytic0 extends MeanAnalytic(0)
class MeanAnalytic1 extends MeanAnalytic(1)
class MeanAnalytic2 extends MeanAnalytic(2)
class MeanAnalytic3 extends MeanAnalytic(3)
class MeanAnalytic4 extends MeanAnalytic(4)
class MeanAnalytic5 extends MeanAnalytic(5)
class MeanAnalytic6 extends MeanAnalytic(6)
class MeanAnalytic7 extends MeanAnalytic(7)
class MeanAnalytic8 extends MeanAnalytic(8)
class MeanAnalytic9 extends MeanAnalytic(9)
class MeanAnalytic10 extends MeanAnalytic(10)
