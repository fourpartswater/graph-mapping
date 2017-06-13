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



import software.uncharted.graphing.layout.V2


/**
  * A force that impleents a r^2 constraint towards the center of a layout
  *
  * Two of the general force-directed layout terms are used here:
  *
  * <ul>
  *   <li>kInv</li>
  *   <li>proportionalConstraint</li>
  * </u>
  *
  * The force applied to a node is in the direction of the the defined center, with a magnitude that is the square of
  * the distance to the center, times the product of these two constants.
  *
  * @param center The center around which nodes are constrained
  */
class ProportionalConstrainingForce(center: V2) extends Force {
  /**
    * Apply our constraining force to all nodes in the graph
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
    for (n <- nodes.indices) {
      val delta = center - nodes(n).geometry.center
      val distance = delta.length - nodes(n).geometry.radius
      if (distance > 0) {
        displacements(n) = displacements(n) + delta * (distance * terms.kInv * terms.parameters.proportionalConstraint)
      }
    }
  }
}
