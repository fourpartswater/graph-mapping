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
package software.uncharted.graphing.layout



import com.typesafe.config.ConfigFactory
import org.apache.spark.SharedSparkContext
import org.apache.spark.graphx.{Edge, Graph}
import org.scalatest.FunSuite
import software.uncharted.graphing.layout.forcedirected.{ForceDirectedLayoutParameters, ForceDirectedLayoutParametersParser, LayoutNode}



class HierarchicalFDLayoutTestSuite extends FunSuite with SharedSparkContext {
  private def getConfig =
    HierarchicalLayoutConfig(null, None, null, null, None, 256.0, 1, 8)
  private def getParams =
    ForceDirectedLayoutParametersParser.parse(ConfigFactory.empty)
  private def getInputGraph: Seq[Graph[GraphNode, Long]] = {
    // raw level
    val nodes0 = Seq[GraphNode](
      GraphNode(0, 0, 2, 3, "Node 0"),
      GraphNode(1, 0, 1, 2, "Node 1"),
      GraphNode(2, 0, 1, 3, "Node 2"),
      GraphNode(3, 3, 2, 3, "Node 3"),
      GraphNode(4, 3, 1, 3, "Node 4"),
      GraphNode(5, 3, 1, 2, "Node 5"),
      GraphNode(6, 6, 2, 2, "Node 6"),
      GraphNode(7, 6, 1, 3, "Node 7"),
      GraphNode(8, 6, 1, 3, "Node 8")
    )
    val edges0 = Seq[Edge[Long]](
      Edge(0, 1, 1), Edge(0, 3, 1),
      Edge(1, 2, 1),
      Edge(2, 0, 1),
      Edge(3, 4, 1),
      Edge(4, 5, 1), Edge(4, 7, 1),
      Edge(5, 3, 1),
      Edge(6, 7, 1),
      Edge(7, 8, 1),
      Edge(8, 6, 1), Edge(8, 2, 1)
    )
    // clustered level
    val nodes1 = Seq[GraphNode](
      GraphNode(0, -1, 4, 8, "Node 0"),
      GraphNode(3, -1, 4, 8, "Node 3"),
      GraphNode(6, -1, 4, 8, "Node 6")
    )
    val edges1 = Seq[Edge[Long]](
      Edge(0, 0, 3), Edge(0, 3, 1),
      Edge(3, 3, 3), Edge(3, 6, 1),
      Edge(6, 6, 3), Edge(6, 0, 1)
    )

    Seq(
      Graph(sc.parallelize(nodes0.map(node => (node.id, node))), sc.parallelize(edges0)),
      Graph(sc.parallelize(nodes1.map(node => (node.id, node))), sc.parallelize(edges1))
    )
  }

  test("Inline layout should lay nodes out without writing anything") {
    val config: HierarchicalLayoutConfig = getConfig
    val params: ForceDirectedLayoutParameters = getParams.get
    val inputGraph: Seq[Graph[GraphNode, Long]] = getInputGraph

    val layouts = HierarchicFDLayout.determineLayout[Graph[LayoutNode, Long]](config, params)(
      level => (inputGraph(level), Some(-1L)),
      (level, layout, width, maxLevel) => layout
    )

    assert(2 === layouts.length)
    assert(9 === layouts(1).vertices.count)
    assert(12 === layouts(1).edges.count)
    assert(3 === layouts(0).vertices.count)
    assert(6 === layouts(0).edges.count())

    for (i <- Seq(0, 1); a <- layouts(i).vertices.collect(); b <- layouts(i).vertices.collect()) {
      if (a != b) {
        assert(a._2.geometry != b._2.geometry)
      }
    }
  }
}
