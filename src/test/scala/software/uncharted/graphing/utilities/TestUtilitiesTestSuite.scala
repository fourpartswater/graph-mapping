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
package software.uncharted.graphing.utilities



import org.scalatest.{BeforeAndAfter, FunSuite}

import org.apache.spark.SharedSparkContext
import org.apache.spark.graphx.Edge



/**
 * Created by nkronenfeld on 12/1/2015.
 */
class TestUtilitiesTestSuite extends FunSuite with BeforeAndAfter with SharedSparkContext {
  import TestUtilities._
  before(turnOffLogSpew)

  test("Test fully specified rdd") {
    val rdd = new FullySpecifiedRDD(sc, Map(
      0 -> List("a", "b", "c"),
      1 -> List("d", "e", "f"),
      2 -> List("g", "h", "i")
    ))
    val collected = collectPartitions(rdd)
    assert(List("a", "b", "c") === collected(0))
    assert(List("d", "e", "f") === collected(1))
    assert(List("g", "h", "i") === collected(2))
  }

  test("Test partition specification") {
    val graph = constructGraph(sc, Map(
      0 -> Seq((0L, "a"), (1L, "b"), (3L, "c")),
      1 -> Seq((5L, "d"), (6L, "e"), (9L, "f")),
      2 -> Seq((8L, "g"), (10L, "h"))
    ), Map(
      0 -> Seq(new Edge(0L, 3L, 1.1), new Edge(0L, 1L, 1.2), new Edge(1L, 0L, 1.3), new Edge(3L, 6L, 1.4)),
      1 -> Seq(new Edge(5L, 0L, 2.1), new Edge(6L, 1L, 2.2), new Edge(9L, 8L, 2.3), new Edge(9L, 6L, 3.5)),
      2 -> Seq(new Edge(0L, 8L, 3.1), new Edge(1L, 8L, 3.2), new Edge(5L, 10L, 3.3), new Edge(10L, 9L, 3.4))
    )
    )

    val nodes = collectPartitions(graph.vertices)
    assert(Set((0L, "a"), (1L, "b"), (3L, "c")) === nodes(0).toSet)
    assert(Set((5L, "d"), (6L, "e"), (9L, "f")) === nodes(1).toSet)
    assert(Set((8L, "g"), (10L, "h")) === nodes(2).toSet)
  }
}
