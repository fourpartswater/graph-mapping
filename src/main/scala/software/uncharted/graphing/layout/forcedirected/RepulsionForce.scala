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

import software.uncharted.graphing.layout.{Circle, V2, QuadNode}

import scala.util.Random


trait RepulsionForce extends Force {
  var nodesOverlap: Boolean
  val overlappingNodesRepulsionFactor: Double
  val k2: Double
  val random: Random

  protected def calculateRepulsion (a: Circle, b: Circle): V2 = {
    val delta = a.center - b.center
    val distance = delta.length - a.radius - b.radius
    if (distance > 0.0) {
      delta * (k2 / (distance * distance))
    } else {
      nodesOverlap = true
      // Extra-strong repulsion force if nodes overlap!
      val repulsionForce = overlappingNodesRepulsionFactor * k2
      if (delta == V2.zero) {
        // perturbate a small amount in a random direction
        val perturbationDirection = random.nextDouble * 2.0 * math.Pi
        val perturbationDistance = 0.005 * (a.radius + b.radius)
        V2.unitVector(perturbationDirection) * perturbationDistance
      } else {
        delta * repulsionForce
      }
    }
  }
}
class QuadTreeRepulsionForce (val k2: Double,
                              qtTheta: Double,
                              val overlappingNodesRepulsionFactor: Double,
                              val random: Random,
                              var nodesOverlap: Boolean) extends RepulsionForce {
  def apply (nodes: Seq[LayoutNode], numNodes: Int,
             edges: Iterable[LayoutEdge], numEdges: Int,
             displacements: Array[V2]): Unit = {
    val qt = LayoutNode.createQuadTree(nodes, numNodes)
    for (i <- nodes.indices) {
      val node = nodes(i)
      val momentaryDelta = calculateRepulsion(i, node.geometry, qt.getRoot, k2, qtTheta)
      displacements(i) = displacements(i) + momentaryDelta
    }
  }

  private def calculateRepulsion (index: Int,
                                  geometry: Circle,
                                  qn: QuadNode,
                                  k2: Double,
                                  theta: Double): V2 = {
    assert(qn != null)

    qn.getNumChildren match {
      case 0 => V2.zero
      case 1 =>
        // Single sub-node - calculate repulsion directly
        val data = qn.getData
        if (data.getId == index) {
          V2.zero
        } else {
          calculateRepulsion(geometry, Circle(V2(data.getX, data.getY), data.getSize))
        }
      case _ =>
        if (useAsPseudoNode(qn, geometry, theta)) {
          // we have multiple children, but can act on them as one
          val com = V2(qn.getCenterOfMass)
          calculateRepulsion(geometry, Circle(com, qn.getSize)) * qn.getNumChildren
        } else {
          // We have multiple children, but have to act on them separately, and sum
          val ne = calculateRepulsion(index, geometry, qn.getNE, k2, theta)
          val se = calculateRepulsion(index, geometry, qn.getSE, k2, theta)
          val sw = calculateRepulsion(index, geometry, qn.getSW, k2, theta)
          val nw = calculateRepulsion(index, geometry, qn.getNW, k2, theta)

          ne + se + sw + nw
        }
    }
  }

  private def useAsPseudoNode (qn: QuadNode, geometry: Circle, theta: Double): Boolean = {
    // Minimum of width and height of cell
    val length = (qn.getBounds._3 - qn.getBounds._1) min (qn.getBounds._4 - qn.getBounds._2)
    val com = V2(qn.getCenterOfMass)
    val delta = geometry.center - com
    val distanceToCell = delta.length - qn.getSize
    // Notes:  -account for quadNode's radius too to minimize the chance of all pseudonode's children causing over-repulsion
    //   -technically, it would be more accurate to also subtract the target node's radius above too, but the trade-off would be less efficient QuadTree usage
    //    (due to extra recursion into child quadnodes)

    0 < distanceToCell && length <= distanceToCell * theta
  }
}

class ElementRepulsionForce (val k2: Double,
                             val overlappingNodesRepulsionFactor: Double,
                             val random: Random,
                             var nodesOverlap: Boolean
                            ) extends RepulsionForce {
  override def apply(nodes: Seq[LayoutNode], numNodes: Int,
                     edges: Iterable[LayoutEdge], numEdges: Int,
                     displacements: Array[V2]): Unit = {
    for (n1 <- nodes.indices) {
      var d = for (n2 <- nodes.indices) {
        if (n1 != n2) {
          displacements(n1) = displacements(n1) + calculateRepulsion(nodes(n1).geometry, nodes(n2).geometry)
        }
      }
    }
  }
}
