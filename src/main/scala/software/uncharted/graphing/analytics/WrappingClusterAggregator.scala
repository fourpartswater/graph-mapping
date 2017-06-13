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



/**
  * Takes a simple Aggregator[T] and wraps its input and output as strings
  */
class WrappingClusterAggregator[-I, N, O] (base: Aggregator[I, N, O],
                                           inputConversion: String => I,
                                           outputConversion: O => String) extends Aggregator[String, N, String] {
  assert(null != base) //scalastyle:ignore

  override def default(): N = base.default()

  override def finish(intermediate: N): String = outputConversion(base.finish(intermediate))

  override def merge(left: N, right: N): N = base.merge(left, right)

  override def add(current: N, next: Option[String]): N = base.add(current, next.map(inputConversion))
}
