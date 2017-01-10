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



import software.uncharted.graphing.layout.V2


/**
  * A force that implements a 1/r^2 gravitational force towards the center of a layout
  *
  * Two of the general force-directed layout terms are used here:
  *
  * <ul>
  *   <li>kInv</li>
  *   <li>gravity</li>
  * </u>
  *
  * The product of these two is taken to be the gravitational constant of the universe.
  *
  * I think there may be something wrong with the math here - it looks like this is implementing an r^2 force, not a
  * 1/r^2 force
  *
  * @param center The center towards which gravity flows
  */
class GravitationalForce (center: V2) extends Force {
  override def apply(nodes: Seq[LayoutNode], numNodes: Int,
                     edges: Iterable[LayoutEdge], numEdges: Int,
                     displacements: Array[V2],
                     terms: ForceDirectedLayoutTerms): Unit = {
    for (n <- nodes.indices) {
      val delta = center - nodes(n).geometry.center
      val distance = delta.length - nodes(n).geometry.radius
      if (distance > 0) {
        displacements(n) = displacements(n) + delta * (distance * terms.kInv * terms.parameters.gravity)
      }
    }
  }
}
