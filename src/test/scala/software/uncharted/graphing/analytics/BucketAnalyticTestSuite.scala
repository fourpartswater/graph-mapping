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

import org.scalatest.FunSuite

class BucketAnalyticTestSuite extends FunSuite {
  test("Bucket analytics") {
    val bucket1 = new Bucket(0, 9, 0)
    val bucket2 = new Bucket(10, 19, 1)
    val bucket3 = new Bucket(20, 29, 2)
    val analytic = new BucketAnalytic(0, Array(bucket1, bucket2, bucket3))
    val agg = analytic.aggregator

    var value1 = agg.default()
    value1 = agg.add(value1, Some("4"))
    value1 = agg.add(value1, Some("10"))
    value1 = agg.add(value1, Some("9"))
    value1 = agg.add(value1, Some("25"))
    value1 = agg.add(value1, Some("29"))

    var value2 = agg.default()
    value2 = agg.add(value2, Some("10"))
    value2 = agg.add(value2, Some("19"))
    value2 = agg.add(value2, Some("20"))
    value2 = agg.add(value2, Some("29"))
    value2 = agg.add(value2, Some("22"))
    value2 = agg.add(value2, Some("22"))
    value2 = agg.add(value2, Some("-1"))
    value2 = agg.add(value2, None)

    assert("0,2,4" === analytic.max(agg.finish(value1), agg.finish(value2)))

    assert("2,1,2" === analytic.min(agg.finish(value1), agg.finish(value2)))

    val value = agg.merge(value1, value2)
    assert(Array(2, 3, 6) === value)
    assert("2,3,6" === agg.finish(value))
  }
}
