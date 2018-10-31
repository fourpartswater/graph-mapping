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
package software.uncharted.graphing.layout.openord

import org.gephi.graph.api._
import org.gephi.layout.plugin.noverlap.{NoverlapLayout, NoverlapLayoutBuilder}
import org.gephi.layout.plugin.openord.{OpenOrdLayout, OpenOrdLayoutBuilder}
import org.gephi.layout.plugin.scale.{Contract, ContractLayout}
import org.gephi.project.api.{ProjectController}
import org.openide.util.Lookup
import software.uncharted.graphing.layout._

import collection.JavaConversions._
import scala.collection.mutable
import scala.util.{Random, Try} //scalastyle:ignore


/**
  * A class that knows how to run an open ord layout on a graph. The heart of the algorithm is in
  * layoutConnectedNodes; all other routines are administration, or handle simple cases that don't require forces
  * in a deterministic manner.
  *
  * @param parameters The parameters governing how the force-directed layout algorithm is to run.
  */
class OpenOrdLayouter(parameters: OpenOrdLayoutParameters) extends Serializable {
  private var iterationCallback: Option[(Array[LayoutNode], Iterable[LayoutEdge], Int, Double) => Unit] = None
  private var isolatedNodeCallback: Option[Iterable[LayoutNode] => Unit] = None

  /**
    * Set a callback that will get called once on each iteration of the force-direct layout on connected nodes
    * @param fcnOpt The function to be called; parameters are the laid out connected nodes and edges, the number of
    *               iterations, and the current temperature.
    */
  def setIterationCallback (fcnOpt: Option[(Array[LayoutNode], Iterable[LayoutEdge], Int, Double) => Unit]): Unit =
    iterationCallback = fcnOpt

  /**
    * Set a callback that will get called once isolated nodes are laid out
    * @param fcnOpt The function to be called; parameters are the laid out isolated nodes
    */
  def setIsolatedNodeCallback (fcnOpt: Option[Iterable[LayoutNode] => Unit]): Unit =
    isolatedNodeCallback = fcnOpt

  /**
    * Run the force-directed layout algorithm on a "graph".
    *
    * "Graph" here should be taken to mean a set of nodes and edges; there may be other nodes and edges too that are
    * not laid out, and aren't passed into this method, so what is being laid out could really a sub-graph -
    * this method doesn't really care.
    *
    * @param nodes The nodes of the graph
    * @param edges The edges of the graph
    * @param parentId The ID of the common parent node of all nodes in the graph.
    * @param bounds The bounding circle in which the graph is to be laid out
    * @return The input nodes, augmented with their positions and sizes.
    */
  def run (nodes: Iterable[GraphNode],
           edges: Iterable[GraphEdge],
           parentId: Long,
           bounds: Circle): Iterable[LayoutNode] = {
    nodes.size match {
      case 0 =>
        throw new IllegalArgumentException("Attempt to layout 0 nodes")
      case 1 =>
        oneNodeLayout (nodes, parentId, bounds)
      case 2 | 3 | 4 =>
        smallNodeLayout(nodes, parentId, bounds)
      case _ =>
        generalLayout(nodes, edges, parentId, bounds)
    }
  }

  /* The radius of a circle with the given area */
  private def radiusFromArea (area: Double): Double = math.sqrt(area / math.Pi)
  /* The area of a circle with the given radius */
  private def areaFromRadius (radius: Double): Double = radius * radius * math.Pi

  private def degreeColumnLabel = "degree"
  private def parentIdColumnLabel = "parentId"
  private def internalNodesColumnLabel = "internalNodes"
  private def metadataColumnLabel = "metadata"

  /* simple function to encapsulate the common case of doing things differently if we are using node sizes or not
   * in one line.
   * We do this a lot, so it's not worth the line count and complexity measure (according to scalastyle) of a full
   * if/then/else. */
  private def ifUseNodeSizes[T] (thenDo: => T, elseDo: => T): T = {
    if (parameters.useNodeSizes) {
      thenDo
    } else {
      elseDo
    }
  }

  // Lay out a single node - obviously in the identical location as its parent
  // Note that this does, however, shrink the area of the node to nodeAreaFactor * the parent area - why do we
  // do this?
  private def oneNodeLayout (nodes: Iterable[GraphNode],
                             parentId: Long,
                             bounds: Circle): Iterable[LayoutNode] = {
    assert(1 == nodes.size)

    Array(LayoutNode(nodes.head, bounds.center, ifUseNodeSizes(bounds.radius, 1.0)))
  }

  // Precalculate relative locations of small layouts, so we don't have to do lots of parallel trig calculations
  private val smallNodeLayouts = Map(
    1 -> List(V2(1.0, 0.0)),
    2 -> List(V2(1.0, 0.0), V2(-1.0, 0.0)),
    3 -> List(V2(1.0, 0.0), V2(math.sqrt(3.0) / 2.0, -0.5), V2(-math.sqrt(3.0) / 2.0, -0.5)),
    4 -> List(V2(1.0, 0.0), V2(0.0, 1.0), V2(-1.0, 0.0), V2(0.0, -1.0))
  )

  // Lay out a small (<5) number of nodes, for which the layout is guaranteed (because of the small number of nodes)
  // to be a central primary node with satelites evenly spaced around it.
  private def smallNodeLayout (nodes: Iterable[GraphNode],
                               parentId: Long,
                               bounds: Circle): Iterable[LayoutNode] = {
    assert(1 < nodes.size && nodes.size <= 4)

    val epsilon = 1E-6
    val parentArea = areaFromRadius(bounds.radius)

    // Determine radii - proportional to the proportion of internal nodes in each node
    val totalInternalNodes = nodes.map(_.internalNodes).sum
    val radii = nodes.map { node =>
      if (parameters.useNodeSizes) {
        val nodeArea = parentArea * parameters.nodeAreaFactor * node.internalNodes / totalInternalNodes
        (node.id, radiusFromArea(nodeArea))
      } else {
        (node.id, 1.0)
      }
    }.toMap
    val primaryNodeRadius = radii.getOrElse(parentId, 0.0)
    val maxRadius = radii.filter(_._1 != parentId).values.max
    val nonPrimaryPoints = radii.count(_._1 != parentId)
    val scaleFactor = ifUseNodeSizes(bounds.radius / (primaryNodeRadius + 2.0 * maxRadius + 2 * epsilon), 1.0)

    // Map to each input node, putting the primary one (the one with the same id as the parent) in the center,
    // and locating the rest around it like satelites.
    var nonPrimaryPointIndex = 0
    nodes.map { node =>
      val radius = radii(node.id)
      val position =
        if (node.id == parentId) {
          // Primary node
          bounds.center
        } else {
          // Non-primary node. - use precalculated relative locations.
          val relativeLocation = smallNodeLayouts(nonPrimaryPoints)(nonPrimaryPointIndex)
          val distanceFromParent = radius + primaryNodeRadius + epsilon
          nonPrimaryPointIndex += 1

          bounds.center + relativeLocation * distanceFromParent * scaleFactor
        }

      // Combine node and location information.
      LayoutNode(node, position, radius * scaleFactor)
    }
  }

  /*
   * Get the external degree of each node - i.e., the degree of its connections to other nodes.
   */
  private def edgeBasedExternalNodeDegrees (edges: Iterable[GraphEdge]): scala.collection.Map[Long, Long] = {
    val weights = mutable.Map[Long, Long]()
    edges.foreach { edge =>
      if (edge.srcId != edge.dstId) {
        weights(edge.srcId) = weights.getOrElse(edge.srcId, 0L) + edge.weight
        weights(edge.dstId) = weights.getOrElse(edge.dstId, 0L) + edge.weight
      }
    }
    weights
  }

  // Lay out an arbitrarily large number of nodes
  private def generalLayout (nodes: Iterable[GraphNode],
                             edges: Iterable[GraphEdge],
                             parentId: Long,
                             bounds: Circle): Iterable[LayoutNode] = {
    // Manually layout isolated nodes
    val (connectedNodes, isolatedNodes) = ifUseNodeSizes(
      {
        val externalDegrees = edgeBasedExternalNodeDegrees(edges)
        nodes.partition { node =>
          externalDegrees.getOrElse(node.id, 0L) > parameters.isolatedDegreeThreshold
        }
      },
      (nodes, Iterable[GraphNode]())
    )

    val parentArea = areaFromRadius(bounds.radius)

    // Allocate area to isolated nodes and connected nodes according to the total number of contained internal nodes
    // To do this, we allocate the area for the connected nodes, and the remainder goes to isolated nodes
    val isolatedInternalNodes = isolatedNodes.map(_.internalNodes).sum
    val connectedInternalNodes = connectedNodes.map(_.internalNodes).sum
    val totalInternalNodes = isolatedInternalNodes + connectedInternalNodes
    val collectedRadius = radiusFromArea(parentArea * connectedInternalNodes / totalInternalNodes)

    val isolatedLayout = layoutIsolatedNodes(isolatedNodes, bounds, collectedRadius)
    isolatedNodeCallback.foreach(_(isolatedLayout))

    val connectedNodeSeq = connectedNodes.toSeq
    val connectedNodeBounds = new Circle(bounds.center, collectedRadius)
    val connectedLayout =
      connectedNodeSeq.length match {
        case 0 =>
          Iterable[LayoutNode]()
        case 1 =>
          oneNodeLayout(connectedNodeSeq, parentId, connectedNodeBounds)
        case 2 | 3 | 4 =>
          smallNodeLayout(connectedNodeSeq, parentId, connectedNodeBounds)
        case _ =>
          layoutConnectedNodes(connectedNodes.toSeq, edges, parentId,
            connectedNodeBounds,
            connectedInternalNodes)
      }

    isolatedLayout ++ connectedLayout
  }

  private def convertGraphNodesToGephiNodes (graphModel: GraphModel,
                                             nodes: Seq[GraphNode],
                                             parentId: Long,
                                             bounds: Circle,
                                             totalInternalNodes: Long,
                                             random: Random): Map[Long, Node] = {

    val border = parameters.borderPercent / 100.0 * bounds.radius
    val area = areaFromRadius(bounds.radius)

    val gephiNodes = new scala.collection.mutable.HashMap[Long, Node]

    val degreeColumn = graphModel.getNodeTable.getColumn(degreeColumnLabel)
    val parentIdColumn = graphModel.getNodeTable.getColumn(parentIdColumnLabel)
    val internalNodesColumn = graphModel.getNodeTable.getColumn(internalNodesColumnLabel)
    val metadataColumn = graphModel.getNodeTable.getColumn(metadataColumnLabel)

    for (i <- nodes.indices) {
      val node = nodes(i)
      val position = if (node.id == parentId) {
        bounds.center
      } else {
        // TODO: Should the random vector range over [-1, 1) instead of [-0.5, 0.5)?
        bounds.center + (V2.randomVector(random) - V2(0.5, 0.5)) * bounds.radius
      }
      val radius = ifUseNodeSizes(radiusFromArea((area * parameters.nodeAreaFactor) * (node.internalNodes.toDouble / totalInternalNodes.toDouble)), border)

      val label = node.id.toString
      val n = graphModel.factory.newNode(label)
      n.setLabel(label)
      n.setPosition(bounds.center.x.toFloat, bounds.center.y.toFloat)
      n.setSize(node.internalNodes)

      n.setAttribute(degreeColumn, node.degree)
      n.setAttribute(parentIdColumn, node.parentId)
      n.setAttribute(internalNodesColumn, node.internalNodes)
      n.setAttribute(metadataColumn, node.metadata)

      gephiNodes.put(node.id, n)
    }
    gephiNodes.toMap
  }

  private def convertGraphEdgesToGephiEdges(graphModel: GraphModel, layoutEdges: Iterable[GraphEdge], gephiNodes: Map[Long, Node]): Iterable[Edge] = {
    val gephiEdges = layoutEdges.flatMap(edge => {
      if (edge.srcId == edge.dstId) {
        None
      } else {
        val srcNode = gephiNodes.get(edge.srcId)
        val dstNode = gephiNodes.get(edge.dstId)
        if (srcNode != None && dstNode != None) {
          val thisEdge = graphModel.factory.newEdge(srcNode.get, dstNode.get)
          thisEdge.setWeight(edge.weight.doubleValue)
          Some(thisEdge)
        }
        else {
          None
        }
      }
    })
    gephiEdges
  }

  private def convertGephiNodesToLayoutNodes(nodes: Array[Node]): Array[LayoutNode] = {
    val layoutNodes = new Array[LayoutNode](nodes.length)

    for (i <- nodes.indices) {
      val node = nodes(i)

      val graphNode = new GraphNode(
        node.getLabel.toLong,
        node.getAttribute(parentIdColumnLabel).asInstanceOf[Long],
        node.getAttribute(internalNodesColumnLabel).asInstanceOf[Long],
        node.getAttribute(degreeColumnLabel).asInstanceOf[Int],
        node.getAttribute(metadataColumnLabel).asInstanceOf[String]
      )

      layoutNodes(i) = LayoutNode(graphNode, node.x, node.y, node.size)
    }

    layoutNodes
  }

  private def scaleLayoutToBounds(graphModel: GraphModel, bounds: Circle, terms: OpenOrdLayoutTerms) = {
    val nodes = graphModel.getDirectedGraph.getNodes
    // Find the largest distance from center currently
    Try {
      nodes.map { node =>
        (Math.sqrt(Math.pow((node.x - bounds.center.x), 2.0) + Math.pow((node.y - bounds.center.y), 2.0)), node.size)
      }.reduce((a, b) => if (a._1 + a._2 > b._1 + b._2) a else b)
    }.map { case (farthestDistance, radiusOfFarthestPoint) =>
      val borderScale = (100.0 - terms.parameters.borderPercent) / 100.0
      // target max radius is bounds.radius * borderScale
      val scale = Math.abs(bounds.radius * borderScale) / (Math.abs(farthestDistance) + Math.abs(radiusOfFarthestPoint))
      // Scale both the size of each node, and the vector defining its position relative to its parent node location.
      nodes.map { node =>
        node.setX((bounds.center.x + (node.x - bounds.center.x) * scale).toFloat)
        node.setY((bounds.center.y + (node.y - bounds.center.y) * scale).toFloat)
        node.setSize((node.size * scale).toFloat)
      }
    }
  }

  //noinspection ScalaStyle
  private def runLayout(graphModel: GraphModel, bounds: Circle, terms: OpenOrdLayoutTerms) : Array[LayoutNode] = {

    //Layout
    val oolb = new OpenOrdLayoutBuilder
    val ool = new OpenOrdLayout(oolb)
    ool.setGraphModel(graphModel)

    ool.resetPropertiesValues()
    ool.initAlgo()

    while ( {
      ool.canAlgo
    }) ool.goAlgo()

    ool.endAlgo()

    scaleLayoutToBounds(graphModel, bounds, terms)

    val nlb = new NoverlapLayoutBuilder
    val nl = new NoverlapLayout(nlb)
    nl.setGraphModel(graphModel)

    nl.resetPropertiesValues()
    nl.setRatio(1.0)
    nl.setMargin(0.0)
    nl.initAlgo()

    while ( {
      !nl.isConverged
    }) nl.goAlgo()

    nl.endAlgo()

    convertGephiNodesToLayoutNodes(graphModel.getDirectedGraph.getNodes.toArray)
  }

  // Lay out an arbitrarily large number of connected nodes (i.e., nodes that do have a sufficient connection to others
  // in their community).  This routine is the one that actually performs force-directed layout; all other layout
  // routines in this class are deterministic.
  private def layoutConnectedNodes (nodes: Seq[GraphNode],
                                    edges: Iterable[GraphEdge],
                                    parentId: Long,
                                    bounds: Circle,
                                    totalInternalNodes: Long): Iterable[LayoutNode] = {
    val numNodes = nodes.size
    val random = parameters.randomSeed.map(r => new Random(r)).getOrElse(new Random())

    // create new gephi project
    val pc =  Lookup.getDefault.lookup(classOf[ProjectController])
    pc.newProject
    val project = pc.getCurrentProject

    // create a fresh new workspace for the current project
    val workspace = pc.newWorkspace(project)

    // get the graphmodel for the workspace and add custom columns to the node table
    val graphModel = Lookup.getDefault.lookup(classOf[GraphController]).getGraphModel(workspace)
    if (!graphModel.getNodeTable.hasColumn(degreeColumnLabel)) graphModel.getNodeTable.addColumn(degreeColumnLabel, classOf[Int])
    if (!graphModel.getNodeTable.hasColumn(parentIdColumnLabel)) graphModel.getNodeTable.addColumn(parentIdColumnLabel, classOf[Long])
    if (!graphModel.getNodeTable.hasColumn(internalNodesColumnLabel)) graphModel.getNodeTable.addColumn(internalNodesColumnLabel, classOf[Long])
    if (!graphModel.getNodeTable.hasColumn(metadataColumnLabel)) graphModel.getNodeTable.addColumn(metadataColumnLabel, classOf[String])

    // Initialize output coordinates randomly
    val gephiNodes = convertGraphNodesToGephiNodes(graphModel, nodes, parentId, bounds, totalInternalNodes, random)
    val terms = new OpenOrdLayoutTerms(numNodes, bounds.radius, parameters, edges.map(_.weight).max)
    val gephiEdges = convertGraphEdgesToGephiEdges(graphModel, edges, gephiNodes)

    val graph = graphModel.getDirectedGraph

    graph.addAllNodes(gephiNodes.values)
    graph.addAllEdges(gephiEdges)

    // run open ord layout algorithm on nodes
    val layoutNodes = runLayout(graphModel, bounds, terms)

    // final scaling to make sure the nodes fit in the area
    scaleNodesToArea(layoutNodes, bounds, terms)

    // close
    pc.deleteWorkspace(workspace)

    pc.closeCurrentWorkspace
    pc.closeCurrentProject

    layoutNodes
  }

  // Scale final positions to fit within the prescribed area
  private def scaleNodesToArea (nodes: Array[LayoutNode], bounds: Circle, terms: OpenOrdLayoutTerms): Unit = {
    // Find the largest distance from center currently
    Try {
      nodes.map { node =>
        ((node.geometry.center - bounds.center).length, node.geometry.radius)
      }.reduce((a, b) => if (a._1 + a._2 > b._1 + b._2) a else b)
    }.map { case (farthestDistance, radiusOfFarthestPoint) =>
      val borderScale = (100.0 - terms.parameters.borderPercent) / 100.0
      // target max radius is bounds.radius * borderScale
      val scale = Math.abs(bounds.radius * borderScale) / (Math.abs(farthestDistance) + Math.abs(radiusOfFarthestPoint))
      // Scale both the size of each node, and the vector defining its position relative to its parent node location.
      for (i <- nodes.indices) {
        val node = nodes(i)
        val p = node.geometry.center
        val r = node.geometry.radius
        nodes(i) = LayoutNode(node, bounds.center + (p - bounds.center) * scale, r * scale)
      }
    }
  }

  /* If our math says we need up to N + isolatedNodeSquishFactor rows, squish isolated nodes to N rows. */
  private val isolatedNodeSquishFactor = 0.25
  // r0 is inner radius, r1 is outer radius, items is the total number of items to place
  private def determineIsolatedNodeRows (r0: Double, r1: Double, items: Int): Int = {
    // we lay out nodes in concentric rings, taking the space between items to be the same as the space between rings,
    // and letting there be as many items per ring as fit.
    // Given N rings, inner radius r0, and outer radius r1, we get:
    //     delta_r = (r1 - r0) / N          space between rings or between items
    //     items_ring_i = 2 pi r_ring_i / delta_r
    //     r_ring_i = r0 + (1 + 2i)/2N (r1 - r0)
    //     items = pi N^2 (r1 + r0) / (r1 - r0)
    // But we need N
    //     N = sqrt(items * (r1 - r0) / (pi (r1 + r0)))
    val N = math.sqrt(items * (r1 - r0) / (math.Pi * (r1 + r0)))

    // We now have to turn this into an integer.  But if it's one over fitting in N rows, we'd rather just squish a
    // little than have a second...
    // Let's just use a squish factor for now
    (N + (1.0 - isolatedNodeSquishFactor)).floor.toInt max 1
  }

  private def isolatedRowRadius (row: Int, rows: Int, r0: Double, r1: Double): Double =
    r0 + ((1.0 + 2.0 * row) / (2.0 * rows)) * (r1 - r0)

  private def isolatedRowCircumference (row: Int, rows: Int, r0: Double, r1: Double): Double =
    2.0 * math.Pi * isolatedRowRadius(row, rows, r0, r1)

  // row is the row current number, rows is the total number of rows,
  // r0 is inner radius, r1 is outer radius, items is the total number of items to place
  private def nodesPerIsolatedRow (row: Int, rows: Int, r0: Double, r1: Double, items: Int): Int = {
    // Row i centered at radius (r0 + (1 + 2 i)/2N (r1 - r0)
    val circumI = isolatedRowCircumference(row, rows, r0, r1)
    val circumTotal = 2.0 * math.Pi * rows * (r1 + r0) / 2.0
    (items * circumI / circumTotal).round.toInt
  }

  // Lay out an arbitrarily large number of isolated nodes (i.e. nodes that don't have a sufficient connect to other
  // nodes in their community)
  private[openord] def layoutIsolatedNodes (nodes: Iterable[GraphNode],
                                                  bounds: Circle,
                                                  minRadiusFromCenter: Double): Iterable[LayoutNode] = {
    val numNodes = nodes.size
    val rows = determineIsolatedNodeRows(minRadiusFromCenter, bounds.radius, numNodes)
    val avgOffset = (2.0 * math.Pi * rows * (minRadiusFromCenter + bounds.radius) / 2.0) / numNodes
    val maxSize = (bounds.radius - minRadiusFromCenter) / rows
    val nodeSize = (maxSize min avgOffset) * parameters.nodeAreaFactor * 0.5
    var row = 0 // The row currently being laid out
    var radius = isolatedRowRadius(row, rows, minRadiusFromCenter, bounds.radius)
    var rowNodes = nodesPerIsolatedRow(row, rows, minRadiusFromCenter, bounds.radius, numNodes)
    var i = 0   // Items in the current row already placed
    var curOffset = 0.0 // Radians offset for placement of current item
    var offsetPerItem = 2.0 * math.Pi / rowNodes

    nodes.map { node =>
      if (i >= rowNodes && row < rows - 1) {
        // next row
        i = 0
        row = row + 1
        radius = isolatedRowRadius(row, rows, minRadiusFromCenter, bounds.radius)
        rowNodes = nodesPerIsolatedRow(row, rows, minRadiusFromCenter, bounds.radius, numNodes)
        curOffset = 0.0
        offsetPerItem = 2.0 * math.Pi / rowNodes
      }

      val vOffset = V2.unitVector(curOffset)
      curOffset += offsetPerItem
      i += 1
      LayoutNode(node, bounds.center + vOffset * radius, nodeSize)
    }
  }
}
