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

class EdgeAttractionForce (k_inv: Double, edgeWeightNormFactor: Option[Double]) extends Force {
  override def apply(nodes: Seq[LayoutNode], numNodes: Int,
                     edges: Iterable[LayoutEdge], numEdges: Int,
                     displacements: Array[V2]): Unit = {
    for (edge <- edges) {
      val src = nodes(edge.srcIndex)
      val dst = nodes(edge.dstIndex)
      val delta = dst.geometry.position - src.geometry.position
      val distance = delta.length - src.geometry.radius - dst.geometry.radius
      if (distance > 0) {
        // Only calculate attractive force if nodes don't overlap
        val force = edgeWeightNormFactor.map(_ * edge.weight).getOrElse(1.0) * distance * k_inv
        displacements(edge.srcIndex) = displacements(edge.srcIndex) + delta * force
        displacements(edge.dstIndex) = displacements(edge.dstIndex) - delta * force
      }
    }
  }
}

