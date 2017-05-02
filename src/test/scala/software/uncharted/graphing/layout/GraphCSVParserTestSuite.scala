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

class GraphCSVParserTestSuite extends FunSuite with SharedSparkContext {
  test("Test edge parsing") {
    val data = sc.parallelize(List(
      "edge\t1\t2\t1",
      "edge\t3\t2\t1",
      "edge\t1\t3\t1",
      "node\t1\t2\t1\t2",
      "node\t2\t2\t4\t5",
      "node\t3\t2\t1\t1"
    ))

    val parser = new GraphCSVParser()
    val edges = parser.parseEdgeData(data, "\t", 1, 2, 3).collect()

    assert(edges.length == 3)
    assert(edges(0).dstId == 2)
    assert(edges(1).srcId == 3)
    assert(edges(2).attr == 1)
  }

  test("Test node parsing") {
    val data = sc.parallelize(List(
      "edge\t1\t2\t1",
      "edge\t3\t2\t1",
      "edge\t1\t3\t1",
      "node\t1\t2\t1\t2",
      "node\t2\t2\t4\t5",
      "node\t3\t2\t1\t1"
    ))

    val parser = new GraphCSVParser()
    val nodes = parser.parseNodeData(data, "\t", 1, 2, 3, 4, true).collect()

    assert(nodes.length == 3)
    assert(nodes(0).id == 1)
    assert(nodes(1).parentId == 2)
    assert(nodes(2).degree == 1)
    assert(nodes(1).internalNodes == 4)
  }
}
