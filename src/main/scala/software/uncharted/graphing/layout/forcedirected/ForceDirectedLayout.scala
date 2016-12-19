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


import scala.collection.mutable
import scala.util.{Random, Try}
import software.uncharted.graphing.layout._



class ForceDirectedLayout (parameters: ForceDirectedLayoutParameters) extends Serializable {
  private var iterationCallback: Option[(Array[LayoutNode], Iterable[LayoutEdge], Int, Double) => Unit] = None
  def setIterationCallback (fcnOpt: Option[(Array[LayoutNode], Iterable[LayoutEdge], Int, Double) => Unit]): Unit =
    iterationCallback = fcnOpt
  private var isolatedNodeCallback: Option[Iterable[LayoutNode] => Unit] = None
  def setIsolatedNodeCallback (fcnOpt: Option[Iterable[LayoutNode] => Unit]): Unit =
    isolatedNodeCallback = fcnOpt

  def run (nodes: Iterable[GraphNode],
           edges: Iterable[GraphEdge],
           parentId: Long,
           bounds: Circle,
           hierarchyLevel: Int): Iterable[LayoutNode] = {
    nodes.size match {
      case 0 =>
        throw new IllegalArgumentException("Attempt to layout 0 nodes")
      case 1 =>
        oneNodeLayout (nodes, parentId, bounds)
      case 2 | 3 | 4 =>
        smallNodeLayout(nodes, parentId, bounds)
      case _ =>
        generalLayout(nodes, edges, parentId, bounds, hierarchyLevel)
    }
  }

  /* The radius of a circle with the given area */
  private def radiusFromArea (area: Double): Double = math.sqrt(area / math.Pi)
  /* The area of a circle with the given radius */
  private def areaFromRadius (radius: Double): Double = radius * radius * math.Pi


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
  def oneNodeLayout (nodes: Iterable[GraphNode],
                     parentId: Long,
                     bounds: Circle): Iterable[LayoutNode] = {
    assert(1 == nodes.size)

    Array(LayoutNode(nodes.head, bounds.center, ifUseNodeSizes(bounds.radius, 0.0)))
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
  def smallNodeLayout (nodes: Iterable[GraphNode],
                       parentId: Long,
                       bounds: Circle): Iterable[LayoutNode] = {
    assert(1 < nodes.size && nodes.size <= 4)

    val parentArea = areaFromRadius(bounds.radius)

    // Determine radii - proportional to the proportion of internal nodes in each node
    val totalInternalNodes = nodes.map(_.internalNodes).sum
    val radii = nodes.map { node =>
      if (parameters.useNodeSizes) {
        val nodeArea = parentArea * parameters.nodeAreaFactor * node.internalNodes / totalInternalNodes
        (node.id, radiusFromArea(nodeArea))
      } else {
        (node.id, 0.0)
      }
    }.toMap
    val primaryNodeRadius = radii.getOrElse(parentId, 0.0)
    val maxRadius = radii.filter(_._1 != parentId).values.max
    val nonPrimaryPoints = radii.count(_._1 != parentId)
    val scaleFactor = ifUseNodeSizes(bounds.radius / (primaryNodeRadius + 2.0 * maxRadius), 1.0)

    // Map to each input node, putting the primary one (the one with the same id as the parent) in the center,
    // and locating the rest around it like satelites.
    var nonPrimaryPointIndex = 0
    nodes.map { node =>
      val radius = radii(node.id)
      val position =
        if (node.id == parentId) {
          // Primary node
          V2(0.0, 0.0)
        } else {
          // Non-primary node. - use precalculated relative locations.
          val relativeLocation = smallNodeLayouts(nonPrimaryPoints)(nonPrimaryPointIndex)
          val distanceFromParent = radius + primaryNodeRadius
          nonPrimaryPointIndex += 1

          bounds.center + relativeLocation * distanceFromParent * scaleFactor
        }

      // Combine node and location information.
      LayoutNode(node, position, radius)
    }
  }

  def generalLayout (nodes: Iterable[GraphNode],
                     edges: Iterable[GraphEdge],
                     parentId: Long,
                     bounds: Circle,
                     hierarchyLevel: Int): Iterable[LayoutNode] = {
    // Manually layout isolated nodes
    val (connectedNodes, isolatedNodes) = ifUseNodeSizes(
      nodes.partition(_.degree > parameters.isolatedDegreeThreshold),
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

    val connectedLayout = layoutConnectedNodes(connectedNodes.toSeq, edges, parentId,
      new Circle(bounds.center, collectedRadius),
      connectedInternalNodes, hierarchyLevel)

    isolatedLayout ++ connectedLayout
  }



  def layoutConnectedNodes (nodes: Seq[GraphNode], edges: Iterable[GraphEdge], parentId: Long, bounds: Circle,
                            totalInternalNodes: Long, hierarchyLevel: Int): Iterable[LayoutNode] = {
    val numNodes = nodes.size
    val random = parameters.randomSeed.map(r => new Random(r)).getOrElse(new Random())

    // Initialize output coordinates randomly
    val layoutNodes = convertGraphNodesToLayoutNodes(nodes, parentId, bounds, totalInternalNodes, random)
    val terms = new ForceDirectedLayoutTerms(numNodes, bounds.radius, parameters, edges.map(_.weight).max)
    val forces = getForces(terms, bounds, random)
    val layoutEdges = convertGraphEdgesToLayoutEdges(edges, nodes.map(_.id).zipWithIndex.toMap)
    val numEdges = layoutEdges.size

    // Actually run the force-directed algorithm
    var done = false
    var iterations = 1
    var overlappingNodes = false
    println(s"Starting force-directed layout on $numNodes nodes and $numEdges edges...")
    while (!done) {
      iterationCallback.foreach(_(layoutNodes, layoutEdges, iterations, terms.temperature))

      overlappingNodes = false

      // node displacements for this iteration
      val displacements = Array.fill(numNodes)(V2.zero)

      // Execute forces for this iteration
      forces.foreach { force =>
        force.apply(layoutNodes, numNodes, layoutEdges, numEdges, displacements, terms)
      }

      // Modify displacements as per current temperature, and store results for this iteration
      val initialTotalEnergy = terms.totalEnergy
      terms.totalEnergy = 0.0
      val largestSquaredStep = updatePositions(layoutNodes, displacements, parentId, terms)
      updateTemperature(terms, initialTotalEnergy)

      //---- Check if system has adequately converged (note: we allow iterations to go higher than maxIterations here, due to adaptive cooling routine)
      if ( ((iterations >= 1.5f * parameters.maxIterations) ||
        (!overlappingNodes && (iterations >= parameters.maxIterations))) ||
        (terms.temperature <= 0.0) ||
        (largestSquaredStep <= terms.squaredStepLimit) ) {
        println("Finished layout algorithm in " + iterations + " iterations.")
        done = true
      }

      iterations += 1
    }

    scaleNodesToArea(layoutNodes, bounds, terms)

    layoutNodes
  }

  private def getForces (terms: ForceDirectedLayoutTerms, bounds: Circle, random: Random): Seq[Force] = {
    Seq(
      if (terms.useQuadTree) {
        Some(new QuadTreeRepulsionForce(random))
      } else {
        Some(new ElementRepulsionForce(random))
      },
      Some(new EdgeAttractionForce()),
      if (parameters.gravity > 0.0) {
        Some(new GravitationalForce(bounds.center))
      } else if (parameters.useNodeSizes) {
        Some(new BoundingForce(bounds))
      } else {
        None
      }
    ).flatten
  }

  private def updatePositions (layoutNodes: Array[LayoutNode], displacements: Array[V2],
                               parentId: Long, terms: ForceDirectedLayoutTerms): Double = {
    var largestSquaredStep = Double.MinValue
    val numNodes = layoutNodes.length
    for (n <- 0 until numNodes) {
      val node = layoutNodes(n)
      // Never move our central node
      if (node.id != parentId) {
        var displacement = displacements(n)
        var displacementMagnitude = displacement.length
        if (displacementMagnitude > terms.temperature) {
          val normalizedTemperature = terms.temperature / displacementMagnitude
          displacement = displacement * normalizedTemperature
          displacementMagnitude = terms.temperature
        }
        val squaredStep = displacementMagnitude * displacementMagnitude
        largestSquaredStep = squaredStep max largestSquaredStep
        terms.totalEnergy = terms.totalEnergy + squaredStep

        layoutNodes(n) = LayoutNode(node, node.geometry.center + displacement, node.geometry.radius)
      }
    }
    largestSquaredStep
  }

  private def updateTemperature (terms: ForceDirectedLayoutTerms,
                                 initialTotalEnergy: Double): Unit = {
    //---- Adaptive cooling function (based on Yifan Hu "Efficient, High-Quality Force-Directed Graph Drawing", 2006)
    if (terms.totalEnergy < initialTotalEnergy) {
      // system energy (movement) is decreasing, so keep temperature constant
      // or increase slightly to prevent algorithm getting stuck in a local minimum
      terms.progressCount += 1
      if (terms.progressCount >= 5) {
        terms.progressCount   = 0
        terms.temperature = Math.min(terms.temperature / parameters.alphaCool, terms.initialTemperature)
      }
    } else {
      // system energy (movement) is increasing, so cool the temperature
      terms.progressCount = 0
      if (terms.overlappingNodes) {
        // cool slowly if nodes are overlapping
        terms.temperature = terms.temperature * parameters.alphaCoolSlow
      } else {
        // cool at the regular rate
        terms.temperature = terms.temperature * parameters.alphaCool
      }
    }
  }

  // Scale final positions to fit within the prescribed area
  private def scaleNodesToArea (nodes: Array[LayoutNode], bounds: Circle, terms: ForceDirectedLayoutTerms): Unit = {
    // Find the largest distance from center currently
    Try {
      nodes.map { node =>
        ((node.geometry.center - bounds.center).length, node.geometry.radius)
      }.reduce((a, b) => if (a._1 + a._2 > b._1 + b._2) a else b)
    }.map { case (farthestDistance, radiusOfFarthestPoint) =>
      val borderScale = (100.0 - terms.parameters.borderPercent) / 100.0
      // target max radius is bounds.radius * borderScale
      val scale = (bounds.radius * borderScale - radiusOfFarthestPoint) / farthestDistance
      for (i <- nodes.indices) {
        val node = nodes(i)
        val p = node.geometry.center
        val r = node.geometry.radius
        nodes(i) = LayoutNode(node, bounds.center + (p - bounds.center) * scale, r)
      }
    }
  }

  private def convertGraphNodesToLayoutNodes (nodes: Seq[GraphNode],
                                              parentId: Long,
                                              bounds: Circle,
                                              totalInternalNodes: Long,
                                              random: Random): Array[LayoutNode] = {
    val border = parameters.borderPercent / 100.0 * bounds.radius
    val area = areaFromRadius(bounds.radius)

    val layoutNodes = new Array[LayoutNode](nodes.size)
    for (i <- nodes.indices) {
      val node = nodes(i)
      val position = if (node.id == parentId) {
        bounds.center
      } else {
        // TODO: Should the random vector range over [-1, 1) instead of [-0.5, 0.5)?
        bounds.center + (V2.randomVector(random) - V2(0.5, 0.5)) * bounds.radius
      }
      val radius = ifUseNodeSizes(radiusFromArea(area * parameters.nodeAreaFactor * node.internalNodes / totalInternalNodes), border)
      layoutNodes(i) = LayoutNode(node, position, radius)
    }
    layoutNodes
  }

  private def convertGraphEdgesToLayoutEdges (edges: Iterable[GraphEdge], nodeIds: Map[Long, Int]): Iterable[LayoutEdge] = {
    edges.flatMap { edge =>
      for (srcIndex <- nodeIds.get(edge.srcId);
           dstIndex <- nodeIds.get(edge.dstId)) yield {
        LayoutEdge(srcIndex, dstIndex, edge.weight)
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

  def layoutIsolatedNodes (nodes: Iterable[GraphNode],
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
