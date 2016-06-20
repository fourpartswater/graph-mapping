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

class CategoryAnalyticTestSuite extends FunSuite {
  test("Category analytics") {
    val analytic = new CategoryAnalytic(0)
    val agg = analytic.aggregator

    var value1 = agg.default()
    value1 = agg.add(value1, Some("apple"))
    value1 = agg.add(value1, Some("apple"))
    value1 = agg.add(value1, Some("apple,orange"))
    value1 = agg.add(value1, Some("orange,apple"))
    value1 = agg.add(value1, Some("orange"))

    var value2 = agg.default()
    value2 = agg.add(value2, Some("banana,pineapple"))
    value2 = agg.add(value2, Some("banana"))
    value2 = agg.add(value2, Some("banana"))
    value2 = agg.add(value2, Some("banana,kiwi"))
    value2 = agg.add(value2, Some("banana,kiwi"))
    value2 = agg.add(value2, Some(""))
    value2 = agg.add(value2, None)

    // Test min and max before we merge - merge will merge in place.
    assert("banana:5,kiwi:2,pineapple:1" === analytic.max(agg.finish(value1), agg.finish(value2)))

    assert("apple:4,orange:3" === analytic.min(agg.finish(value1), agg.finish(value2)))

    val value = agg.merge(value1, value2)
    assert(Map("banana" -> 5, "apple" -> 4, "orange" -> 3, "kiwi" -> 2, "pineapple" -> 1) === value)
    assert("banana:5,apple:4,orange:3,kiwi:2,pineapple:1" === agg.finish(value))
  }
}
