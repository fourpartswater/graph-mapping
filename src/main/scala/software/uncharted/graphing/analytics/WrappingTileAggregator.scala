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

import scala.util.parsing.json.JSONObject



/**
  * Takes a simple Aggregator[T] and wraps its input as strings, and its output as json
  */
class WrappingTileAggregator [-I, N, O] (base: Aggregator[I, N, O],
                                         inputConversion: String => I,
                                         outputConversion: O => JSONObject) extends Aggregator[String, N, JSONObject] {
  override def default(): N = base.default()

  override def finish(intermediate: N): JSONObject = outputConversion(base.finish(intermediate))

  override def merge(left: N, right: N): N = base.merge(left, right)

  override def add(current: N, next: Option[String]): N = base.add(current, next.map(inputConversion))
}
