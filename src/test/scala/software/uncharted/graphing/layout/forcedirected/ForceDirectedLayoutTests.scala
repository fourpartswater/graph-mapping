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
package software.uncharted.graphing.layout.forcedirected


import com.typesafe.config.ConfigFactory

import scala.collection.mutable.Buffer
import org.scalatest.FunSpec
import software.uncharted.graphing.layout.{GraphEdge, GraphNode}



class ForceDirectedLayoutTests extends FunSpec {
  private val g5 = (Seq(
    GraphNode(1L, 1L, 4, 3, "central node"),
    GraphNode(2L, 1L, 4, 2, "node 2"),
    GraphNode(3L, 1L, 3, 2, "node 3"),
    GraphNode(4L, 1L, 2, 3, "node 4"),
    GraphNode(5L, 1L, 1, 4, "node 5")
  ),
  Seq(
    GraphEdge(1L, 2L, 1L),
    GraphEdge(1L, 3L, 1L),
    GraphEdge(1L, 4L, 1L),
    GraphEdge(1L, 5L, 1L),
    GraphEdge(2L, 3L, 1L),
    GraphEdge(2L, 4L, 1L),
    GraphEdge(2L, 5L, 1L),
    GraphEdge(3L, 4L, 1L),
    GraphEdge(3L, 5L, 1L),
    GraphEdge(4L, 5L, 1L)
  )
  )

  describe("Force-directed layout of graphs") {
    it("Should lay out a simple 5-node graph in a circle when ignoring node size") {
      val arranger = new ForceDirectedLayout
      val layout = arranger.run(g5._1, g5._2, 1L, (-1.0, -1.0, 1.0, 1.0), 1).map{ node =>
        (node.id, node)
      }.toMap

      assert(V2(0.0, 0.0) === layout(1L).geometry.position)
      val radii = layout.filter(_._1 != 1L).mapValues(_.geometry.position.length)
      val avgRadius = radii.values.sum / radii.size
      radii.foreach { case (id, radius) =>
          assert(radius > avgRadius * 3.0 / 4.0, s"node $id is outside of bounds rings")
          assert(radius < avgRadius * 4.0 / 3.0, s"node $id is inside of bounds rings")
      }
    }
    it("Should lay out a simple 5--node graph in a less precise circle when not ignoring node size") {
      val config = ConfigFactory.parseString(
        """layout = {
          |  force-directed = {
          |    use-node-sizes = true
          |  }
          |}
        """.stripMargin)
      val parameters = ForceDirectedLayoutParameters(config).get
      val arranger = new ForceDirectedLayout(parameters)
      val layout = arranger.run(g5._1, g5._2, 1L, (-1.0, -1.0, 1.0, 1.0), 1).map{ node =>
        (node.id, node)
      }.toMap

      assert(V2(0.0, 0.0) === layout(1L).geometry.position)
      val radii = layout.filter(_._1 != 1L).mapValues(_.geometry.position.length)
      val avgRadius = radii.values.sum / radii.size
      radii.foreach { case (id, radius) =>
        assert(radius > avgRadius * 3.0 / 4.0, s"node $id is out of bounds")
        assert(radius < avgRadius * 4.0 / 3.0, s"node $id is out of bounds")
      }

      assert(layout(1L).geometry.radius === layout(2L).geometry.radius)
      assert(layout(2L).geometry.radius > layout(3L).geometry.radius)
      assert(layout(3L).geometry.radius > layout(4L).geometry.radius)
      assert(layout(4L).geometry.radius > layout(5L).geometry.radius)
    }
  }
}
