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
package software.uncharted.graphing.layout.forcedirected


import com.typesafe.config.ConfigFactory

import scala.collection.mutable.{Buffer => MutableBuffer}
import org.scalatest.FunSpec
import software.uncharted.graphing.layout.{Circle, V2, GraphEdge, GraphNode}



class ForceDirectedLayoutTests extends FunSpec {
  private val g2 = (Seq(
    GraphNode(1834232L, 1834232L, 129051, 3433500, "7075520 0,0,1,5,127,23785,59858,45275"),
    GraphNode(1548108L, 1834232L,  68833, 1660994, "9213857 0,0,1,0,47,13033,32166,23586")
  ),
    Seq(
      GraphEdge(1834232L, 1548108L, 113422L),
      GraphEdge(1548108L, 1834232L, 113422L),
      GraphEdge(1901205L, 1834232L,    658L),
      GraphEdge(1834232L, 1901205L,    658L),
      GraphEdge( 245936L, 1834232L,    998L),
      GraphEdge(1834232L,  245936L,    998L),
      GraphEdge(1901205L, 1834232L,      4L),
      GraphEdge(1834232L, 1901205L,      4L)
    )
    )
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
  private val epsilon = 1E-12

  def defaultParameters: ForceDirectedLayoutParameters = {
    import ForceDirectedLayoutParametersParser._
    ForceDirectedLayoutParameters(
      defaultOverlappingNodesRepulsionFactor, defaultNodeAreaFactor, defaultStepLimitFactor, defaultBorderPercent,
      defaultIsolatedDegreeThreshold, defaultQuadTreeNodeThreshold, defaultQuadTreeTheta, defaultProportionalConstraint,
      defaultMaxIterations, defaultUseEdgeWeights, defaultUseNodeSizes, defaultRandomHeating, Some(defaultRandomSeed)
    )
  }
  def nodeSizeParameters: ForceDirectedLayoutParameters = {
    import ForceDirectedLayoutParametersParser._
    ForceDirectedLayoutParameters(
      defaultOverlappingNodesRepulsionFactor, defaultNodeAreaFactor, defaultStepLimitFactor, defaultBorderPercent,
      defaultIsolatedDegreeThreshold, defaultQuadTreeNodeThreshold, defaultQuadTreeTheta, defaultProportionalConstraint,
      defaultMaxIterations, defaultUseEdgeWeights, useNodeSizes = true, defaultRandomHeating, Some(defaultRandomSeed)
    )
  }


  describe("Force-directed layout of graphs") {

    val unitCircle = Circle(V2(0.0, 0.0), 1.0)
    it("Should lay out a simple 2-node graph within the bounds provided") {
      val arranger = new ForceDirectedLayout(nodeSizeParameters)
      val bounds = Circle(V2(128.0, 128.0), 11.729350489692258)
      val layout = arranger.run(g2._1, g2._2, 1834232L, bounds).map { node =>
        (node.id, node)
      }.toMap
      val gPrimus = layout(1834232L).geometry
      val gSecundus = layout(1548108L).geometry
      assert(bounds.center === gPrimus.center)
      assert((gSecundus.center - gPrimus.center).length + gSecundus.radius < bounds.radius)
      assert(gPrimus.radius > 0.0)
      assert(gSecundus.radius > 0.0)
      assert(gPrimus.radius + gSecundus.radius < (gSecundus.center - gPrimus.center).length)
    }
    it("Should lay out a simple 5-node graph in a circle when ignoring node size") {
      val arranger = new ForceDirectedLayout(defaultParameters)
      val layout = arranger.run(g5._1, g5._2, 1L, unitCircle).map { node =>
        (node.id, node)
      }.toMap

      assert(V2(0.0, 0.0) === layout(1L).geometry.center)
      val radii = layout.filter(_._1 != 1L).mapValues(_.geometry.center.length)
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
      val parameters = ForceDirectedLayoutParametersParser.parse(config).get
      val arranger = new ForceDirectedLayout(parameters)
      val layout = arranger.run(g5._1, g5._2, 1L, unitCircle).map { node =>
        (node.id, node)
      }.toMap

      assert(V2(0.0, 0.0) === layout(1L).geometry.center)
      val radii = layout.filter(_._1 != 1L).mapValues(_.geometry.center.length)
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
    describe("#layoutIsolatedNodes") {
      val circle15 = Circle(V2(0.0, 0.0), 15.0)
      val nodes = (1L to 1000L).map(n => GraphNode(n, n, 1, 0, s"Node $n"))
      val arranger = new ForceDirectedLayout(defaultParameters)
      it("should lay things out in the allotted space only") {
        val results = arranger.layoutIsolatedNodes(nodes, circle15, 10.0)
        results.foreach { r =>
          val dist = r.geometry.center.length
          val rad = r.geometry.radius
          assert(dist + rad <= 15.0)
          assert(dist - rad >= 10.0)
        }
      }
      it("should lay things out in 1 row when the number of items is appropriate") {
        // 16 ~= number of items in one row
        val results = arranger.layoutIsolatedNodes(nodes.take(4 * 4), circle15, 10.0)
        assert(1 === countConcentricCircles(results).size)
      }
      it("should lay things out in 2 rows when the number of items is appropriate") {
        // 2 rows => 2^2 * items in one row
        val results = arranger.layoutIsolatedNodes(nodes.take(8 * 8), circle15, 10.0)
        assert(2 === countConcentricCircles(results).size)
      }
      it("should lay things out in 3 rows when the number of items is appropriate") {
        // 3 rows => 3^2 * items in one row
        val results = arranger.layoutIsolatedNodes(nodes.take(12 * 12), circle15, 10.0)
        assert(3 === countConcentricCircles(results).size)
      }
      it("should increase the number of items per row linearly") {
        val results = arranger.layoutIsolatedNodes(nodes, circle15, 1.0)
        val circles = countConcentricCircles(results)
        val increases = circles.sliding(2).map(p => p(1)._2 - p(0)._2).toList
        val mean = increases.sum.toDouble / increases.length
        increases.foreach { inc =>
          assert(math.abs(inc - mean) < 1.0)
        }
      }
    }
  }

  private def countConcentricCircles(nodes: Iterable[LayoutNode]) = {
    val nodeCounts = MutableBuffer[(Double, Int)]()
    nodes.foreach { node =>
      val d0 = node.geometry.center.length
      val i = nodeCounts.indexWhere(p => math.abs(p._1 - d0) < epsilon)
      if (-1 == i) {
        nodeCounts += ((d0, 1))
      } else {
        nodeCounts(i) = (nodeCounts(i)._1, nodeCounts(i)._2 + 1)
      }
    }

    nodeCounts.toList.sortBy(_._1)
  }
}
