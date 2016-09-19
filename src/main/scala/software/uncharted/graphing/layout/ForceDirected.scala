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
package software.uncharted.graphing.layout


import org.apache.spark.{Accumulable, Accumulator}

import scala.util.Random
import scala.collection.mutable.ArrayBuffer


/**
 *  A Force-Directed graph layout algorithm (ie Fruchterman-Reingold)
 *
 *  Converted from the 'FFDLayouter' code in ApertureJS
 *
 *  notes:
 *  - nodes = List of (node ID, numInternalNodes, degree, metadata) -- numInternalNodes and degree are only applicable
 *  	for hierarchical force-directed applications (ie if each 'node' represents a community). In this
 *   	case it is recommended to set bUseNodeSizes = true
 *  - boundingBox = bottem-left corner, width, height of bounding region for layout of nodes
 *  - borderPercent = Percent of parent bounding box to leave as whitespace between neighbouring communities during layout.  Default = 2 %.
 *  				  In particular, this parameter is used when a) manually laying out isolated communities, or
 *        			  b) when laying out raw nodes without any size (ie bUseNodeSizes = false)
 *  - maxIterations = max number of iterations to use for force-directed algorithm
 *  - bUseEdgeWeights = uses edge weights to scale the attraction forces between connected nodes
 *  - bUseNodeSizes = uses 'number of internal nodes' attribute to size each node as a circle
 *  - nodeAreaPercent = if bUseNodeSizes = true, then this parameter is used to determine the area of all node
 *  	'circles' within the boundingBox vs whitespace
 *  - gravity = strength gravity force to use to prevent outer nodes from spreading out too far.  Default = 0.0 (no gravity),
 *  			whereas gravity = 1.0 gives gravitational force on a similar scale to edge attraction forces
 *  - isolatedDegreeThres = threshold to determine whether or not a community/node is considered 'isolated'.  Isolated nodes are laid out in an outer radial/spiral pattern
 *
 *  - Format of output array is (node ID, x, y, radius, numInternalNodes, metaData)
 **/

object ForceDirected {
	//----- Static members of ForceDirected

	/**
	 * Used to create a new Quad Tree object and insert all nodes into it
	 *
	 * @param nodes = Array of nodes data to be inserted. Format is (node ID, x, y, radius, numInternalNodes, metaData).
	 *
	 **/
	def createQuadTree(nodes: Array[LayoutNode], numNodes: Int): QuadTree = {
		// find bounding box for the current node positions
		var minX = Double.MaxValue
		var maxX = Double.MinValue
		var minY = Double.MaxValue
		var maxY = Double.MinValue
    nodes.foreach{node =>
			minX = Math.min(node.geometry.x, minX)
			maxX = Math.max(node.geometry.x, maxX)
			minY = Math.min(node.geometry.y, minY)
			maxY = Math.max(node.geometry.y, maxY)
		}

		//insert all nodes into quadtree
		val qt = new QuadTree((minX, minY, maxX-minX, maxY-minY))
		for (n <- 0 until numNodes) {
      val node = nodes(n)
			qt.insert(node.geometry.x, node.geometry.y, n, node.geometry.radius)	//note: use 'nodes' array index as quadNode 'ID', and node radius as quadNode 'size'
		}

		qt
	}

	/**
	 * Used to decide whether to recurse inside this quad node or just use its center of mass as
	 * an approximate 'pseudo node'
	 *
	 * If qnLen/dist <= theta, then dist is big compared to qnLen and we should just consider this a 'pseudo node'
	 **/
	def useAsPseudoNode(qn: QuadNode, geometry: LayoutGeometry, theta: Double): Boolean = {

		val qnLen = Math.min(qn.getBounds._3, qn.getBounds._4) // min of quadNode's width, height
		val (xC,yC) = qn.getCenterOfMass
		val rC = qn.getSize
		val (deltaX,deltaY) = (geometry.x - xC, geometry.y - yC)

		val dist = Math.sqrt(deltaX*deltaX + deltaY*deltaY) - rC	// distance between current node and quadnode's CofMass
		// Notes: 	-account for quadNode's radius too to minimize the chance of all pseudonode's children causing over-repulsion
		//			-technically, it would be more accurate to also subtract the target node's radius above too, but the trade-off would be less efficient QuadTree usage
		//			 (due to extra recursion into child quadnodes)

		if (dist > 0) qnLen <= theta*dist
		else false
	}
}

class ForceDirected extends Serializable {

	val QT_NODE_THRES = 20		// num of nodes threshold for whether or not to use quadtree decomposition
	val QT_THETA = 1.0			// theta value for quadtree decomposition
								// (>= 0; lower value gives more accurate repulsion force results, but is less efficient)
	var _bNodesOverlapping = false	// boolean for whether community circles overlap or not
	var _nodeOverlapRepulsionFactor = Math.pow(1000.0/256, 2.0)	// constant used for extra strong repulsion if node 'circles' overlap

  /**
    * Run a force-directed layout on all the direct children of a single parent community
    *
    * @param nodes A 4-tuple of (VertexId, numInternalNodes, degree, metadata) for each node in our parent community
    * @param edges A 3-tuple of (srcId, dstId, weight) of each edge between two nodes in our parent community
    * @param parentID The ID of the parent community.  This should be -1 for the root community.
    * @param boundingBox The bounding box of the parent community, as determined by the last level.
    *                    This should be (0, 0, maxX, maxY) for the top level.
    * @param hierLevel The hierarchy level of the (parent?) community
    * @param borderPercent
    * @param maxIterations
    * @param bUseEdgeWeights
    * @param bUseNodeSizes
    * @param nodeAreaPercent
    * @param gravity
    * @param isolatedDegreeThres
    * @return
    */
	def run(nodes: Iterable[GraphNode],
	        edges: Iterable[GraphEdge],
	        parentID: Long,
	        boundingBox: (Double, Double, Double, Double),
	        hierLevel: Int,
	        borderPercent: Double = 2.0,
	        maxIterations: Int = 1000,
	        bUseEdgeWeights: Boolean = false,
	        bUseNodeSizes: Boolean = false,
	        nodeAreaPercent: Int = 30,
	        gravity: Double = 0.0,
	        isolatedDegreeThres: Int = 0,
          scaleFactors: Accumulable[ArrayBuffer[Double], Double]): Array[LayoutNode] = {

		var numNodes = nodes.size
		if (numNodes == 0) throw new IllegalArgumentException("number of nodes must be > 0")

		var nodeData = nodes
		var invTotalInternalNodes = if (bUseNodeSizes)	// inverse of total number of internal nodes in all groups
			1.0 / nodeData.map(_.internalNodes).sum
		else
			0L

		val xC = 0.0 //0.5*boundingBoxFinal._3	// centre of bounding area (use as gravitational centre)
		val yC = 0.0 //0.5*boundingBoxFinal._4
		var boundingBoxFinal = boundingBox
		var boundingBoxArea = boundingBoxFinal._3 * boundingBoxFinal._4
		val nodeAreaFactor = nodeAreaPercent*0.01

		if (numNodes == 1) {
			//---- Special case: <= 4 nodes, so do simple manual layout
			val nodeResults = doManualLayout(nodeData, boundingBoxFinal, numNodes, nodeAreaFactor*invTotalInternalNodes, bUseNodeSizes, parentID)
			return nodeResults
		}

		//---- Manually layout any isolated communities (if some communities have degree <= isolatedDegreeThres)	//TODO -- ideally, we should check degree of intra-community edges here, instead of total degree
		val isolatedNodeCoords = if (bUseNodeSizes && (nodeData.map(n => n.degree).min <= isolatedDegreeThres)) {
			val isolatedNodeData = nodeData.filter(n => n.degree <= isolatedDegreeThres)	// list of isolated communities (degree<=isolatedDegreeThres)
			nodeData = nodeData.filter(n => n.degree > isolatedDegreeThres)	// list of connected communities (degree>isolatedDegreeThres)
			numNodes = nodeData.size	// and re-calc numNodes and invTotalInternalNodes too

			val totalConnectedNodes = if (numNodes == 0) 0 else nodeData.map(n => n.internalNodes).sum	// sum of internal nodes for all connected communities

			val connectedArea = nodeAreaFactor * boundingBoxArea * totalConnectedNodes * invTotalInternalNodes	// area for layout of connected communities

			// layout isolated communities in a spiral shape
			val isolatedNodeLayouter = new IsolatedNodeLayout()
			val (spiralCoords, connectedAreaOut) = isolatedNodeLayouter.calcSpiralCoords(isolatedNodeData, boundingBoxFinal, nodeAreaFactor*invTotalInternalNodes, connectedArea, /* borderPercent/4.0 */ 0.0, hierLevel==0)
      val scaleFactor = math.sqrt(connectedAreaOut / connectedArea)
      scaleFactors += scaleFactor

			// re-calc coords of bounding box to correspond to only the central connected communities (width = height = sqrt(2)*r)
			val rSqrt2 = Math.sqrt(connectedAreaOut * 0.31831)*0.70711		//0.31831 = 1/pi; 0.70711 = 1/sqrt(2)
				boundingBoxFinal = (boundingBoxFinal._1 + boundingBoxFinal._3/2 - rSqrt2, boundingBoxFinal._2 + boundingBoxFinal._4/2 - rSqrt2, 2*rSqrt2, 2*rSqrt2)
			boundingBoxArea = boundingBoxFinal._3 * boundingBoxFinal._4

			if (numNodes == 0) {
				// number of remaining 'connected' nodes == 0, so simply return isolated nodes' layout results
				return spiralCoords
			}
			else {
				// recalc invTotalInternalNodes and continue with FD layout
				invTotalInternalNodes = 1.0 / nodeData.map(_.internalNodes).sum
			}

			spiralCoords
		}
		else {
			Array[LayoutNode]()
		}

		//Init scale factors for edge weighting (squash raw edge weights into an appropriate range for the number of nodes)
		//(ie range of sqrt(numNodes) to 1) <-- No, now using weighting from 0 to 1 (see below)
		//var eWeightSlope = 0.0
		//var eWeightOffset = 1.0
		var eWeightNormFactor = 1.0	// edge weight normalization factor (converts weights from 0 to 1 range)
		if (bUseEdgeWeights) {
			val maxW = edges.map(e => e.weight).max
			if (maxW > 0)
				eWeightNormFactor = 1.0 / maxW
			//			// find the min and max edge weights
			//			val (maxW, minW) = edges.map(e => (e._3, e._3)).reduce((a, b) => (a._1 max b._1, a._2 min b._2))
			//
			//			if (maxW > minW) {
			//				val maxWeight = Math.max(Math.sqrt(numNodes), 10.0)
			//				eWeightSlope = (maxWeight - 1.0) / (maxW - minW)
			//				eWeightOffset = 1.0 - eWeightSlope*minW
			//			}
		}

		//---- Initially assign random co-ordinates for all nodes (random number between -0.5 and 0.5)
		// and also assign radii for each node based on internal community size (if applicable)

		// TODO -- try other initial layout schemes to try and reduce number of needed iterations?
		//Eg, place nodes with highest degree in centre, and others farther away ... perhaps in a spiral-like layout similar to Group-in-Box
		val border = if (bUseNodeSizes) 0.0
		else borderPercent*0.01*Math.min(boundingBoxFinal._3, boundingBoxFinal._4)
		val randSeed = 911
		val random = new Random(randSeed)
		var nodeCoords = nodeData.map(n =>
			{
        val geometry = LayoutGeometry(
          if (n.id == parentID) xC else random.nextDouble() - 0.5,
          if (n.id == parentID) yC else random.nextDouble() - 0.5,
          if (bUseNodeSizes) {
            val nodeArea = nodeAreaFactor * boundingBoxArea * n.internalNodes * invTotalInternalNodes
            Math.sqrt(nodeArea * 0.31831)	//0.31831 = 1/pi
          } else {
            //else init all radii = border (to ensure a bit of whitespace between nodes)
            border
          }
        )
				LayoutNode(n, geometry)
			}
		).toArray

		// normalize starting coords so they are within the width and height of the bounding box
		for (n <- 0 until numNodes) {
      val node = nodeCoords(n)
			nodeCoords(n) = LayoutNode(node.node, node.geometry.scalePosition(boundingBoxFinal._3, boundingBoxFinal._4))
		}

		//----- Init variables for controlling force-directed step-size
		val bUseQTDecomp = numNodes > QT_NODE_THRES
		val k2 = boundingBoxArea/numNodes
		val k_inv = 1.0/Math.sqrt(k2)
		val temperature0 = 0.5*Math.min(boundingBoxFinal._3, boundingBoxFinal._4)
		var temperature = temperature0
		val stepLimitFactor = 0.001
		val alphaCool = Math.max(Math.min(1.0 + Math.log(stepLimitFactor)*4.0/maxIterations, 0.99), 0.8)	// set temperature cooling factor with respect to maxIterations (lower value == faster algorithm cooling)
		val alphaCoolSlow = Math.max(Math.min(1.0 + Math.log(stepLimitFactor)*2.0/maxIterations, 0.99), 0.8)	// for cooling half as fast
		val stepLimitSq = Math.pow(Math.min(boundingBoxFinal._3, boundingBoxFinal._4)*stepLimitFactor, 2.0)	// square of stepLimit
		var energySum = Double.MaxValue		// init high
		var progressCount = 0
		_nodeOverlapRepulsionFactor = Math.pow(1000.0/Math.min(boundingBoxFinal._3, boundingBoxFinal._4), 2.0)	// constant used for extra strong repulsion if node 'circles' overlap

		//----- Re-format edge data to reference node array indices instead of actual node ID labels (for faster array look-ups below)
		val edgesArray = reformatEdges(edges, nodeCoords.map(n => n.node.id))

		val numEdges = edgesArray.length

		//----- Main Force-directed algorithm...
		var bDone = false
		var iterations = 1
		println("Starting Force Directed layout on " + numNodes + " nodes and " + numEdges + " edges...")

		while (!bDone) {

			_bNodesOverlapping = false

			// init array of node displacements for this iteration
			val deltaXY = Array.fill[Delta](numNodes)(Delta(0.0,0.0))

			//---- Calc repulsion forces between all nodes
			// (Also, account for node sizes, by adjusting distance between nodes by node radii)
			if (bUseQTDecomp) {
				// Use Quadtree Decomposition for repulsion force calculation
				val qt = ForceDirected.createQuadTree(nodeCoords, numNodes)

				for (n1 <- 0 until numNodes) {
					deltaXY(n1) = deltaXY(n1) + calcQTRepulsion(n1, nodeCoords(n1).geometry, qt.getRoot, k2, QT_THETA)
				}
			}
			else {
				// Use regular repulsion force calculation instead
				for (n1 <- 0 until numNodes) {
					for (n2 <- 0 until numNodes) {
						if (n1 != n2) {
							deltaXY(n1) = deltaXY(n1) + calcRepulsionDelta(nodeCoords(n1).geometry, nodeCoords(n2).geometry, k2)
						}
					}
				}
			}

			//---- Calc attraction forces due to all edges
			// Also, account for node sizes, by adjusting distance between nodes by node radii
			for (e1 <- 0 until numEdges) {
        val edge = edgesArray(e1)
        val src = nodeCoords(edge.srcId).geometry
        val dst = nodeCoords(edge.dstId).geometry

				val xDist = dst.x - src.x
				val yDist = dst.y - src.y
				val dist = Math.sqrt(xDist*xDist + yDist*yDist) - dst.radius - src.radius	// distance minus node radii
				if (dist > 0) {	// only calc attraction force if node circles don't overlap
					val attractForce = if (bUseEdgeWeights) {
						val w = eWeightNormFactor*edge.weight //eWeightSlope*edgeWeight + eWeightOffset
						dist * k_inv * w
					}
					else
						dist * k_inv

          val attraction = Delta(xDist * attractForce, yDist * attractForce)
          deltaXY(edge.srcId) = deltaXY(edge.srcId) + attraction
          deltaXY(edge.dstId) = deltaXY(edge.dstId) - attraction
				}
			}

			//---- Calc gravitational force for all nodes

			if (gravity > 0.0) {
				//Also, account for node sizes using node radii
				for (n <- 0 until numNodes) {
          val node = nodeCoords(n).geometry
					val xDist = xC - node.x	// node distance to centre
					val yDist = yC - node.y
					val dist = Math.sqrt(xDist*xDist + yDist*yDist) - node.radius	// distance minus node radius
					if (dist > 0) {
						val gForce = dist * k_inv * gravity	// gravitational force for this node
            deltaXY(n) = deltaXY(n) + Delta(xDist * gForce, yDist * gForce)
					}
				}
			} else if (bUseNodeSizes) {
				// if using community sizes, but gravity = 0, then do an extra check to see
				// if nodes are outside the bounding box
				val rC = 0.5*Math.min(boundingBoxFinal._3, boundingBoxFinal._4)	// radius thres (smaller value == tighter layout boundary)
				for (n <- 0 until numNodes) {
          val node = nodeCoords(n).geometry
					val xDist = xC - node.x // node distance to centre
					val yDist = yC - node.y
					val dist = Math.sqrt(xDist*xDist + yDist*yDist)
					if (dist > rC) {
						val displRatio = (dist - rC)/dist
            deltaXY(n) = deltaXY(n) + Delta(xDist * displRatio, yDist * displRatio)
					}
				}
			}

			//---- Calc final displacements and save results for this iteration
			var largestStepSq = Double.MinValue // init low
			val energySum0 = energySum
			energySum = 0.0
			for (n <- 0 until numNodes) {
        val node = nodeCoords(n)
				if (node.node.id != parentID) {	// leave 'primary node' at fixed position at centre of bounding area
          var deltaDist = deltaXY(n).length
					if (deltaDist > temperature) {
						val normalizedTemp = temperature/deltaDist
            deltaXY(n) = deltaXY(n) * normalizedTemp
            deltaDist = temperature
					}
          val finalStepSq = deltaDist * deltaDist
					largestStepSq = Math.max(largestStepSq, finalStepSq);	// save largest step for this iteration
					energySum += finalStepSq

					// save new node coord locations
					nodeCoords(n) = LayoutNode(node.node, node.geometry.offset(deltaXY(n)))
				}
			}

			//---- Adaptive cooling function (based on Yifan Hu "Efficient, High-Quality Force-Directed Graph Drawing", 2006)
			if (energySum < energySum0) {
				// system energy (movement) is decreasing, so keep temperature constant
				// or increase slightly to prevent algorithm getting stuck in a local minimum
				progressCount += 1
				if (progressCount >= 5) {
					progressCount = 0
					temperature = Math.min(temperature / alphaCool, temperature0)
				}
			}
			else {
				// system energy (movement) is increasing, so cool the temperature
				progressCount = 0
				if (_bNodesOverlapping)
					temperature *= alphaCoolSlow	// cool slowly if nodes are overlapping
				else
					temperature *= alphaCool		// cool at the regular rate
			}

			//---- Check if system has adequately converged (note: we allow iterations to go higher than maxIterations here, due to adaptive cooling routine)
			if ( ((iterations >= 1.5f*maxIterations) || (!_bNodesOverlapping && (iterations >= maxIterations))) ||
				    (temperature <= 0.0) ||
				    (largestStepSq <= stepLimitSq) ) {
				println("Finished layout algorithm in " + iterations + " iterations.")
				bDone = true
			}

			iterations += 1
		}

		//---- Use Anti-Overlap algo to tune layout if needed
		if (_bNodesOverlapping) {
			println("...but communities still overlapping so using Anti-Overlap algorithm to fine-tune layout")
			val antiOverlap = new AntiOverlap
			val dampenFactor = 0.5f	//0 to 1; lower value == slower, but more precise anti-overlap calc
			val maxDelta = boundingBoxFinal._3*(0.001/100.0) //use 0.001% of parent width as maxDelta
			nodeCoords = antiOverlap.run(nodeCoords, parentID, maxIterations/2, dampenFactor, maxDelta)	// run anti-overlap algorithm (use half of maxIterations)
		}

		//---- Do final scaling of XY co-ordinates to fit within bounding area
		var maxDist = Double.MinValue
		for (n <- 0 until numNodes) {
      val node = nodeCoords(n)
			val xDist = xC - node.geometry.x	// node distance to centre
			val yDist = yC - node.geometry.y
			// calc distance plus node radius (to ensure all of a given node's circle fits into the bounding area)
			val dist = Math.sqrt(xDist*xDist + yDist*yDist) + node.geometry.radius
			maxDist = Math.max(maxDist, dist)
		}

		val parentR = Math.sqrt(0.25*(Math.pow(boundingBoxFinal._3, 2.0) + Math.pow(boundingBoxFinal._4, 2.0)))	// radius of parent circle
		val scaleFactor = if (maxDist > 0) parentR / maxDist  //0.5*Math.min(boundingBoxFinal._3, boundingBoxFinal._4) / maxDist
		else 1.0

		for (n <- 0 until numNodes) {
      val node = nodeCoords(n)
			// scale community radii too if scaleFactor < 1, so scaling doesn't cause communities to overlap
			val finalRadius =
        if (!bUseNodeSizes) 0.0
        else if (scaleFactor < 1.0) node.geometry.radius*scaleFactor
        else node.geometry.radius
      val geometry = LayoutGeometry(
        node.geometry.x*scaleFactor + boundingBoxFinal._1 + 0.5*boundingBoxFinal._3,
        node.geometry.y*scaleFactor + boundingBoxFinal._2 + 0.5*boundingBoxFinal._4,
        finalRadius
      )
			nodeCoords(n) = LayoutNode(node.node, geometry)
		}

		Array.concat(nodeCoords, isolatedNodeCoords)	// return final node coordinates (all node coords concatenated together)
	}

	private def reformatEdges(edges: Iterable[GraphEdge], nodeIds: Array[Long]): Array[LocalGraphEdge] = {

		edges.flatMap(e =>
			{
				val srcIndx = nodeIds.indexOf(e.srcId)
				val dstIndx = nodeIds.indexOf(e.dstId)
				//val srcIndx = nodeCoords.indexOf((e._1,_:Double,_:Double))
				//val dstIndx = nodeCoords.indexOf((e._2,_:Double,_:Double))

				if (srcIndx == -1 || dstIndx == -1) {
          Iterator.empty // not a valid edge
        } else {
          Iterator(LocalGraphEdge(srcIndx, dstIndx, e.weight))
        }
      }
		).toArray
	}

	// Function to manually layout very small communities of <= 4 nodes
	private def doManualLayout(nodeData: Iterable[GraphNode],
	                           boundingBox: (Double, Double, Double, Double),
	                           numNodes: Int,
	                           nodeAreaNorm: Double,
	                           bUseNodeSizes: Boolean,
	                           parentID: Long
	): Array[LayoutNode] = {

		val boundingBoxArea = boundingBox._3 * boundingBox._4

		val nodeResults = if (numNodes == 1) {
			//---- Special case: only one node to layout, so place in centre of boundingBox
			val x = boundingBox._1 + boundingBox._3*0.5
			val y = boundingBox._2 + boundingBox._4*0.5
			val numInternalNodes = nodeData.last.internalNodes
      val radius = if (false) {
        val nodeArea = nodeAreaNorm * boundingBoxArea * numInternalNodes
        if (bUseNodeSizes) Math.sqrt(nodeArea /* * 0.31831*/) else 0.0 //0.31831 = 1/pi
      } else {
        Math.sqrt(boundingBoxArea / 2)
      }

			Array( LayoutNode(nodeData.last, LayoutGeometry(x, y, radius) ) )
		} else {
			if (numNodes > 4) throw new IllegalArgumentException("numNodes <= 4")

			//---- Special case:  <= 4 nodes, so layout in a 'cross' configuration
			val parentR = Math.sqrt(0.25*(Math.pow(boundingBox._3, 2.0) + Math.pow(boundingBox._4, 2.0)))	// radius of parent circle
			val xPts = Array(0.0, 0.0, parentR, -parentR)
			val yPts = Array(parentR, -parentR, 0.0, 0.0)
			val direction = Array((0.0,1.0),(0.0,-1.0),(1.0,0.0),(-1.0,0.0))	// unit direction vector used to shift commmunity positions by it's radius so, it's not outside the bounding area
			var ptCount = -1
			val coords = new Array[LayoutNode](numNodes)
			val radii = new Array[Double](numNodes)
			var primaryNodeRadius = 0.0
			val nodeArray = nodeData.toArray
			var maxRadius = Double.MinValue
			for (n <- 0 until numNodes) {	// calc radii of all communities
				val node  = nodeArray(n)
				val nodeArea = nodeAreaNorm * boundingBoxArea * node.internalNodes
				radii(n) = if (bUseNodeSizes) Math.sqrt(nodeArea * 0.31831) else 0.0	//0.31831 = 1/pi
				if (node.id == parentID) primaryNodeRadius = radii(n)
				else maxRadius = Math.max(radii(n), maxRadius)
			}

			val scaleFactor = if ((primaryNodeRadius + 2*maxRadius > parentR) && bUseNodeSizes)  {
				// current radii will result in community overlap, so scale appropriately
				parentR / (primaryNodeRadius + 2*maxRadius)
			}
			else 1.0

			for (n <- 0 until numNodes) {
        val node = nodeArray(n)
				val radius = radii(n)*scaleFactor
				val (x,y) =
          if (node.id == parentID) (0.0, 0.0)
          else {
            ptCount += 1
            (xPts(ptCount) - direction(ptCount)._1 * radius, yPts(ptCount) - direction(ptCount)._2 * radius)
          }

				coords(n) = LayoutNode(node, LayoutGeometry(x + boundingBox._1 + 0.5*boundingBox._3, y + boundingBox._2 + 0.5*boundingBox._4, radius))
			}
			coords
		}

		nodeResults
	}

	// Calculate Node to Node repulsion force
	def calcRepulsionDelta(target: LayoutGeometry, repulsor: LayoutGeometry, k2: Double): Delta = {

		//format for 'point' is assumed to be (x,y,radius)
		var xDist = target.x - repulsor.x
		var yDist = target.y - repulsor.y
		val r1 = target.radius
		val r2 = repulsor.radius
		// calc distance between two nodes (corrected for node radii)
    // distance minus node radii
		val dist = Math.sqrt(xDist*xDist + yDist*yDist) - r1 - r2
		if (dist > 0.0) {
      // repulsion force
			val repulseForce = k2/(dist*dist)
      Delta(xDist*repulseForce, yDist*repulseForce)
		} else {
			_bNodesOverlapping = true
      // extra strong repulsion force if node circles overlap!
			val repulseForce = _nodeOverlapRepulsionFactor*k2
			if ((xDist == 0) && (yDist == 0)) {
        // force xDist and yDist to be 1% of radius so repulse calc below doesn't == 0
				xDist = r1*0.01
				yDist = r2*0.01	// TODO need random directions here!
			}
      Delta(xDist*repulseForce, yDist*repulseForce)
		}
	}

	// Calculate QuadTree repulsion force
	def calcQTRepulsion(index: Int, g: LayoutGeometry,
                      qn: QuadNode, k2: Double, theta: Double): Delta = {

		if (qn == null) {
			throw new IllegalArgumentException("quadNode == null") //return (0.0, 0.0)
		}
		if (qn.getNumChildren == 0) {
      // nothing to compute
			Delta(0.0, 0.0)
		} else if (qn.getNumChildren == 1) { // leaf
			val qnData = qn.getData
			if (qnData.getId == index) {
        // no repulsion for node compared to itself
				Delta(0.0, 0.0)
			} else {
        calcRepulsionDelta(g, LayoutGeometry(qnData.getX, qnData.getY, qnData.getSize), k2)
      }
		} else if (ForceDirected.useAsPseudoNode(qn, g, theta)) {
      // consider current quadnode as a 'pseudo node'?
      // use quadnode's Centre of Mass as repulsor's coords
			val (xC, yC) = qn.getCenterOfMass
      // and use average radius of all underlying nodes, for this pseudo node
			val rC = qn.getSize
			val xyDelta = calcRepulsionDelta(g, LayoutGeometry(xC, yC, rC), k2)
      // multiply repulsion results by number of child nodes
      xyDelta * qn.getNumChildren
		} else {
      // failed to resolve a repulsion, so recurse into all four child quad nodes, and sum results
      calcQTRepulsion(index, g, qn.getNW, k2, theta) +
        calcQTRepulsion(index, g, qn.getNE, k2, theta) +
        calcQTRepulsion(index, g, qn.getSW, k2, theta) +
        calcQTRepulsion(index, g, qn.getSE, k2, theta)
    }
	}
}

case class LayoutGeometry (x: Double, y: Double, radius: Double) {
  def scalePosition (xScale: Double, yScale: Double): LayoutGeometry =
    LayoutGeometry(x * xScale, y * yScale, radius)
  def offset (xOffset: Double, yOffset: Double): LayoutGeometry =
    LayoutGeometry(x + xOffset, y + yOffset, radius)
  def offset (delta: Delta): LayoutGeometry =
    offset(delta.deltaX, delta.deltaY)
}
case class LayoutNode (node: GraphNode, geometry: LayoutGeometry)
case class Delta (deltaX: Double, deltaY: Double) {
  def * (scale: Double): Delta =
    Delta(deltaX * scale, deltaY * scale)
  def * (xScale: Double, yScale: Double): Delta =
    Delta(deltaX * xScale, deltaY * yScale)
  def + (that: Delta): Delta =
    Delta(this.deltaX + that.deltaX, this.deltaY + that.deltaY)
  def - (that: Delta): Delta =
    Delta(this.deltaX - that.deltaX, this.deltaY - that.deltaY)
  def length: Double = math.sqrt(deltaX * deltaX + deltaY * deltaY)
}
case class LocalGraphEdge (srcId: Int, dstId: Int, weight: Long)
