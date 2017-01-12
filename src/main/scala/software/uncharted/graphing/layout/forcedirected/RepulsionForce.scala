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
  * that defines the math behind said repulsion
  *
  * Base repulsion is defined purely on geometry, not on node weight.  Individual repulsion implementations may
  * choose to incorporate weight if they wish.
  *
  * Two of the general force-directed layout terms are used here:
  *
  * <dl>
  *   <dt>kSq</dt>
  *   <dd>The force scaling.</dd>
  *   <dt>nodeOverlapRepulsionFactor</dt>
  *   <dd>A extra scaling factor to use when nodes overlap.</dd>
  * </dl>
  */
private[forcedirected] trait RepulsionForce extends Force {
  /** A random number generator to be used by this force */
  val random: Random

  /**
    * Calculate the repulsion between two nodes (as represented by their geometry) a and b
    * @param a The geometry of the first node
    * @param b The geometry of the second node
    * @param terms The general terms governing the force-directed layout process
    * @return The force vector b exerts on a
    */
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
  * A force implementing repulsion between nodes.
  *
  * In this implementation, when nodes are far enough away from one another, they are considered in aggregate across
  * the largest quad wholely far enough away - i.e., all nodes in that quad are lumped together and considered as one.
  */
class QuadTreeRepulsionForce (val random: Random) extends RepulsionForce {
  /**
    * Calculate the repulsion force between nodes in a graph
    *
    * @param nodes The current layout of the nodes of the graph
    * @param edges The current layout of the edges of the graph
    * @param displacements The current displacement of each node, so far, in the current iteration of force
    *                      application. This is kept separate from the layout until all forces have had a chance to
    *                      act, so as to avoid confusing force interactions.
    * @param terms The parameters and terms describing the current force-directed layout
    */
  def apply (nodes: Seq[LayoutNode],
             edges: Iterable[LayoutEdge],
             displacements: Array[V2],
             terms: ForceDirectedLayoutTerms): Unit = {
    val qt = LayoutNode.createQuadTree(nodes)
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
      case 0 =>
        // No nodes in this quad => no force from this quad
        V2.zero
      case 1 =>
        // A single node in this quad - no need to approximate
        // Single sub-node - calculate repulsion directly
        val data = qn.getData
        if (data.getId == index) {
          V2.zero
        } else {
          calculateRepulsion(geometry, Circle(V2(data.getX, data.getY), data.getSize), terms)
        }
      case _ =>
        // See if we can consider the quad in aggregate
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

  // Determine if a quad qn is far enough away from the geometry of a given node to allow it to be considered in
  // aggregate, instead of going through each node individually.
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

/**
  * A force implementing repulsion between nodes.
  *
  * This implementation sums up the repulsive forces generated by every pair of nodes in the graph (so, for a graph of
  * N nodes, this is N (N-1) / 2 pairs
  */
class ElementRepulsionForce (val random: Random) extends RepulsionForce {
  /**
    * Calculate the repulsion force between nodes in a graph
    *
    * @param nodes The current layout of the nodes of the graph
    * @param edges The current layout of the edges of the graph
    * @param displacements The current displacement of each node, so far, in the current iteration of force
    *                      application. This is kept separate from the layout until all forces have had a chance to
    *                      act, so as to avoid confusing force interactions.
    * @param terms The parameters and terms describing the current force-directed layout
    */
  override def apply(nodes: Seq[LayoutNode],
                     edges: Iterable[LayoutEdge],
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
