package software.uncharted.graphing.analytics

import software.uncharted.salt.core.analytic.Aggregator

import scala.collection.mutable.{HashMap => MHashMap, ListBuffer => MListBuffer, PriorityQueue => MPriorityQueue}
import scala.reflect.ClassTag

/**
  * Aggregator for producing the top scored N common elements in a
  * collection column, along with their respective scores.
  *
  * @param elementLimit    Produce up to this number of "top elements" in finish()
  * @param scoreAggregator An aggregator to aggregate scores for multiple instances of a single value.
  * @tparam ET The element type
  * @tparam IS the intermediate score type
  */
class TopScoredElementsAggregator[ET: ClassTag, IS](elementLimit: Int, scoreAggregator: Aggregator[Double, IS, Double])
  extends Aggregator[(ET, Double), MHashMap[ET, IS], List[(ET, Double)]] {
  override def default(): MHashMap[ET, IS] = new MHashMap[ET, IS]()

  override def finish(intermediate: MHashMap[ET, IS]): List[(ET, Double)] = {
    val x = new MPriorityQueue[(ET, Double)]()(Ordering.by(
      a => a._2
    ))
    intermediate.foreach { case (value, score) =>
      x.enqueue((value, scoreAggregator.finish(score)))
    }
    val result = new MListBuffer[(ET, Double)]
    for (i <- 0 until Math.min(elementLimit, x.length)) {
      result.append(x.dequeue)
    }
    result.toList
  }

  override def merge(left: MHashMap[ET, IS], right: MHashMap[ET, IS]): MHashMap[ET, IS] = {
    right.foreach { case (value, score) =>
      left.put(
        value,
        scoreAggregator.merge(score, left.getOrElse(value, scoreAggregator.default()))
      )
    }
    left
  }

  override def add(current: MHashMap[ET, IS], next: Option[(ET, Double)]): MHashMap[ET, IS] = {
    next.foreach { case (value, score) =>
      current.put(
        value,
        scoreAggregator.add(
          current.getOrElse(value, scoreAggregator.default()),
          Some(score)
        )
      )
    }
    current
  }
}

object TopScoredElementsAggregator {
  def apply[ES: ClassTag, IS](limit: Int, scoreAggregator: Aggregator[Double, IS, Double]) =
    new TopScoredElementsAggregator[ES, IS](limit, scoreAggregator)
}