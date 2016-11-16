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

class GravitationalForce (gravity: Double, center: V2, k_inv: Double) extends Force {
  override def apply(nodes: Seq[LayoutNode], numNodes: Int,
                     edges: Iterable[LayoutEdge], numEdges: Int,
                     displacements: Array[V2]): Unit = {
    for (n <- nodes.indices) {
      val delta = center - nodes(n).geometry.position
      val distance = delta.length - nodes(n).geometry.radius
      if (distance > 0) {
        displacements(n) = displacements(n) + delta * (distance * k_inv * gravity)
      }
    }
  }
}
