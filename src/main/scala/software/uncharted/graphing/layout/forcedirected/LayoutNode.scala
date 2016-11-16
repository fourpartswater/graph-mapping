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

import software.uncharted.graphing.layout.{GraphNode, QuadTree}


/**
  *
  * @param geometry The layout geometry of this node
  * @param parentGeometry The layout geometry of this node's parent, if known
  */
class LayoutNode (_id: Long, _parentId: Long, _internalNodes: Long, _degree: Int, _metaData: String,
                  val geometry: LayoutGeometry, val parentGeometry: Option[LayoutGeometry])
  extends GraphNode(_id, _parentId, _internalNodes, _degree, _metaData) {
  def inParent (parentGeometry: LayoutGeometry): LayoutNode = {
    new LayoutNode(id, parentId, internalNodes, degree, metadata, geometry, Some(parentGeometry))
  }
}
object LayoutNode {
  def apply (node: GraphNode, x: Double, y: Double, radius: Double): LayoutNode =
    new LayoutNode(node.id, node.parentId, node.internalNodes, node.degree, node.metadata, LayoutGeometry(V2(x, y), radius), None)
  def apply (node: GraphNode, position: V2, radius: Double): LayoutNode =
    new LayoutNode(node.id, node.parentId, node.internalNodes, node.degree, node.metadata, LayoutGeometry(position, radius), None)
  def apply (node: GraphNode, geometry: LayoutGeometry): LayoutNode =
    new LayoutNode(node.id, node.parentId, node.internalNodes, node.degree, node.metadata, geometry, None)

  def createQuadTree (nodes: Iterable[LayoutNode], numNodes: Int): QuadTree = {
    val (minP, maxP) = nodes.map(n => (n.geometry.position, n.geometry.position)).reduce((a, b) =>
      (a._1 min b._1, a._1 max b._1)
    )

    // Create the quad tree
    val range = maxP - minP
    val qt = new QuadTree((minP.x, minP.y, range.x, range.y))

    // Insert all nodes into it
    nodes.foreach { node => qt.insert(node.geometry.position.x, node.geometry.position.y, node.id, node.geometry.radius) }

    qt
  }
}
