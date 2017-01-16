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



import net.liftweb.json.JsonAST.{JString, JValue}
import org.json4s.JField
import software.uncharted.salt.core.analytic.Aggregator


/**
  * Takes a simple Aggregator[T] and wraps its input as strings, and its output as json
  */class WrappingTileAggregator [-I, N, O] (base: Aggregator[I, N, O],
                                         inputConversion: String => I,
                                         outputConversion: O => Map[JString, Any]) extends Aggregator[String, N, Map[JString, Any]] {

  override def default(): N = base.default()

  override def add(current: N, next: Option[String]): N = base.add(current, next.map(inputConversion))

  override def merge(left: N, right: N): N = base.merge(left, right)

  override def finish(intermediate: N): Map[JString, Any] = outputConversion(base.finish(intermediate))

}

