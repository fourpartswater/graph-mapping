package software.uncharted.graphing.analytics

import software.uncharted.salt.core.analytic.Aggregator

/**
  * Takes a simple Aggregator[T] and wraps its input and output as strings
  */
class WrappingClusterAggregator[-I, N, O] (base: Aggregator[I, N, O],
                                           inputConversion: String => I,
                                           outputConversion: O => String) extends Aggregator[String, N, String] {
  override def default(): N = base.default()

  override def finish(intermediate: N): String = outputConversion(base.finish(intermediate))

  override def merge(left: N, right: N): N = base.merge(left, right)

  override def add(current: N, next: Option[String]): N = base.add(current, next.map(inputConversion))
}
