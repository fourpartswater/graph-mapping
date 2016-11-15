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


trait Node {
  val id: Long
  val internalNodes: Long
  val degree: Int
  val metaData: String
}
object Node {
  def apply (graphNode: GraphNode): Node =
    SimpleNode(graphNode.id, graphNode.internalNodes, graphNode.degree, graphNode.metadata)
}

/** A representation of a node
  *
  * @param id The id of that node (was _._1)
  * @param internalNodes The total number of leaf nodes under this node (was _._2)
  * @param degree The total degree of this node (was _._3)
  * @param metaData Any metadata associated with this node (was _._4)
  */
case class SimpleNode (id: Long, internalNodes: Long, degree: Int, metaData: String) extends Node

/**
  *
  * @param id
  * @param internalNodes
  * @param degree
  * @param metaData
  * @param geometry The layout geometry of this node
  */
case class LayoutNode (id: Long, internalNodes: Long, degree: Int, metaData: String,
                       geometry: LayoutGeometry) extends Node
object LayoutNode {
  def apply (node: Node, x: Double, y: Double, radius: Double): LayoutNode =
    LayoutNode(node.id, node.internalNodes, node.degree, node.metaData, LayoutGeometry(V2(x, y), radius))
  def apply (node: Node, position: V2, radius: Double): LayoutNode =
    LayoutNode(node.id, node.internalNodes, node.degree, node.metaData, LayoutGeometry(position, radius))
  def apply (node: Node, geometry: LayoutGeometry): LayoutNode =
    LayoutNode(node.id, node.internalNodes, node.degree, node.metaData, geometry)
  def apply (node: GraphNode, x: Double, y: Double, radius: Double): LayoutNode =
    LayoutNode(node.id, node.internalNodes, node.degree, node.metadata, LayoutGeometry(V2(x, y), radius))
  def apply (node: GraphNode, position: V2, radius: Double): LayoutNode =
    LayoutNode(node.id, node.internalNodes, node.degree, node.metadata, LayoutGeometry(position, radius))
  def apply (node: GraphNode, geometry: LayoutGeometry): LayoutNode =
    LayoutNode(node.id, node.internalNodes, node.degree, node.metadata, geometry)

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
