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
  * A force that pulls nodes together that are linked to each other.
  */
class EdgeAttractionForce extends Force {
  /**
    * Apply edge attraction to a graph
    *
    * @param nodes The current layout of the nodes of the graph
    * @param numNodes The number of nodes in the graph (included just to avoid having to recalculate it all the time,
    *                 since the list of nodes could be long)
    * @param edges The current layout of the edges of the graph
    * @param numEdges The number of edges in the graph (included similarly as for numNodes)
    * @param displacements The current displacement of each node, so far, in the current iteration of force
    *                      application. This is kept separate from the layout until all forces have had a chance to
    *                      act, so as to avoid confusing force interactions.
    * @param terms The parameters and terms describing the current force-directed layout
    */
  override def apply(nodes: Seq[LayoutNode], numNodes: Int,
                     edges: Iterable[LayoutEdge], numEdges: Int,
                     displacements: Array[V2],
                     terms: ForceDirectedLayoutTerms): Unit = {
    for (edge <- edges) {
      val src = nodes(edge.srcIndex)
      val dst = nodes(edge.dstIndex)
      val delta = dst.geometry.center - src.geometry.center
      val distance = delta.length - src.geometry.radius - dst.geometry.radius
      if (distance > 0) {
        // Only calculate attractive force if nodes don't overlap
        val force = terms.edgeWeightNormalizationFactor.map(_ * edge.weight).getOrElse(1.0) * distance * terms.kInv
        displacements(edge.srcIndex) = displacements(edge.srcIndex) + delta * force
        displacements(edge.dstIndex) = displacements(edge.dstIndex) - delta * force
      }
    }
  }
}

