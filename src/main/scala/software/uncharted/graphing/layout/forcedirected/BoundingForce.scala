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

import software.uncharted.graphing.layout.{Circle, V2}

// A force to keep objects inside a bounding box
class BoundingForce(bounds: Circle, thresholdRatio: Double = 0.9) extends Force {
  private val threshold = bounds.radius * thresholdRatio
  override def apply(nodes: Seq[LayoutNode], numNodes: Int,
                     edges: Iterable[LayoutEdge], numEdges: Int,
                     displacements: Array[V2],
                     terms: ForceDirectedLayoutTerms): Unit = {
    for (n <- nodes.indices) {
      val geo = nodes(n).geometry
      val delta = bounds.center - geo.center
      val distance = delta.length + geo.radius
      if (distance > threshold) {
        displacements(n) = displacements(n) + delta * ((distance - threshold) / distance)
      }
    }
  }
}
