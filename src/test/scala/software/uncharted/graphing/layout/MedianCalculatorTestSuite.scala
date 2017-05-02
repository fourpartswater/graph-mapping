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
package software.uncharted.graphing.layout

import org.apache.spark.SharedSparkContext
import org.scalatest.FunSuite

class MedianCalculatorTestSuite extends FunSuite with SharedSparkContext {
  test("Median calculation") {
    val data = sc.parallelize(List(
      1.0, 1.5, 5.4, 3.2, 12.8, 7.1, -4.5, 5.4, 0.0, 12.0,
      4.4, 4.3, 3.4, 4.1, 15.1, 0.1, -0.1, -1.1, -1.0, 4.5
    ), 2)
    // Median is between 3.4 and 4.1
    val median = MedianCalculator.median(data, 1)
    assert((3.4 + 4.1) / 2.0 === median)
  }
}
