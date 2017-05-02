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

import software.uncharted.graphing.layout.{Circle, V2}

/**
  * A force that keeps objects within a bounding circle, by pressing them inward severely when they reach beyond it.
  *
  * @param bounds The bounding circle which should ideally contain objects
  * @param thresholdRatio The ratio of the distance of an object from the center of our bounds, to the ideal radius of
  *                       those bounds, beyond which objects will be pushed back towards the center.  The farther
  *                       beyond this threshold, the harder the object will be pushed.
  */
class BoundingForce(bounds: Circle, thresholdRatio: Double = 0.9) extends Force {
  private val threshold = bounds.radius * thresholdRatio

  /**
    * Apply our bounding force to the nodes of a graph
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
      val geo = nodes(n).geometry
      val delta = bounds.center - geo.center
      val distance = delta.length + geo.radius
      if (distance > threshold) {
        displacements(n) = displacements(n) + delta * ((distance - threshold) / distance)
      }
    }
  }
}
