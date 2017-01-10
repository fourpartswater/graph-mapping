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


/**
  * A root trait from which to derive forces implementing repulsion between nodes.  This just contains the common code
  * that defines the matho of said repulsion
  */
private[forcedirected] trait RepulsionForce extends Force {
  val random: Random

  protected def calculateRepulsion (a: Circle, b: Circle, terms: ForceDirectedLayoutTerms): V2 = {
    val delta = a.center - b.center
    val distance = delta.length - a.radius - b.radius
    if (distance > 0.0) {
      delta * (terms.kSq / (distance * distance))
    } else {
      terms.overlappingNodes = true
      // Extra-strong repulsion force if nodes overlap!
      val repulsionForce = terms.nodeOverlapRepulsionFactor * terms.kSq
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

/**
  * A force implementing repulsion between nodes.  This implementation takes the possible shortcut of storing nodes in
  * a quad tree, and using that to approximate the repulsion between distant nodes
  * @param random
  */
class QuadTreeRepulsionForce (val random: Random) extends RepulsionForce {
  def apply (nodes: Seq[LayoutNode], numNodes: Int,
             edges: Iterable[LayoutEdge], numEdges: Int,
             displacements: Array[V2],
             terms: ForceDirectedLayoutTerms): Unit = {
    val qt = LayoutNode.createQuadTree(nodes, numNodes)
    for (i <- nodes.indices) {
      val node = nodes(i)
      val momentaryDelta = calculateRepulsion(i, node.geometry, qt.getRoot, terms)
      displacements(i) = displacements(i) + momentaryDelta
    }
  }

  private def calculateRepulsion (index: Int,
                                  geometry: Circle,
                                  qn: QuadNode,
                                  terms: ForceDirectedLayoutTerms): V2 = {
    assert(qn != null)

    qn.getNumChildren match {
      case 0 => V2.zero
      case 1 =>
        // Single sub-node - calculate repulsion directly
        val data = qn.getData
        if (data.getId == index) {
          V2.zero
        } else {
          calculateRepulsion(geometry, Circle(V2(data.getX, data.getY), data.getSize), terms)
        }
      case _ =>
        if (useAsPseudoNode(qn, geometry, terms)) {
          // we have multiple children, but can act on them as one
          val com = V2(qn.getCenterOfMass)

          calculateRepulsion(geometry, Circle(com, qn.getSize), terms) * qn.getNumChildren
        } else {
          // We have multiple children, but have to act on them separately, and sum
          val ne = calculateRepulsion(index, geometry, qn.getNE, terms)
          val se = calculateRepulsion(index, geometry, qn.getSE, terms)
          val sw = calculateRepulsion(index, geometry, qn.getSW, terms)
          val nw = calculateRepulsion(index, geometry, qn.getNW, terms)

          ne + se + sw + nw
        }
    }
  }

  private def useAsPseudoNode (qn: QuadNode, geometry: Circle, terms: ForceDirectedLayoutTerms): Boolean = {
    // Minimum of width and height of cell
    val length = (qn.getBounds._3 min qn.getBounds._4)
    val cOfM = V2(qn.getCenterOfMass)
    val delta = geometry.center - cOfM
    val distanceToCell = delta.length - qn.getSize
    // Notes:  -account for quadNode's radius too to minimize the chance of all pseudonode's children causing over-repulsion
    //   -technically, it would be more accurate to also subtract the target node's radius above too, but the trade-off would be less efficient QuadTree usage
    //    (due to extra recursion into child quadnodes)

    0 < distanceToCell && length <= distanceToCell * terms.parameters.quadTreeTheta
  }
}

class ElementRepulsionForce (val random: Random) extends RepulsionForce {
  override def apply(nodes: Seq[LayoutNode], numNodes: Int,
                     edges: Iterable[LayoutEdge], numEdges: Int,
                     displacements: Array[V2],
                     terms: ForceDirectedLayoutTerms): Unit = {
    for (n1 <- nodes.indices) {
      var d = for (n2 <- nodes.indices) {
        if (n1 != n2) {
          displacements(n1) = displacements(n1) + calculateRepulsion(nodes(n1).geometry, nodes(n2).geometry, terms)
        }
      }
    }
  }
}
