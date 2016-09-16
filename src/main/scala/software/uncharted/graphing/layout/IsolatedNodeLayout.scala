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



/**
 *  Functions to layout isolated nodes/communities in a graph (i.e. nodes with degree = 0)
 *
 **/
class IsolatedNodeLayout {

	/**
	 * 	calcSpiralCoords
	 *
	 *  Function to layout isolated and/or leaf communities in a spiral pattern
	 *
	 *  - nodes = List of isolated nodes to layout (node ID, numInternalNodes, degree, metadata)
	 *  - boundingBox = bottem-left corner, width, height of bounding region for layout of nodes
	 *  - nodeAreaNorm = normalization factor used to determine areas of communities within bounding box
	 *  - centralCommunityArea = area of large central 'connected' community (ie area to leave empty in centre of spiral)
	 *  - borderPercent = Percent of parent bounding box to leave as whitespace between neighbouring communities.  Default = 2 %
	 *
	 *  - Format of output array is (node ID, x, y, radius, numInternalNodes, degree, metaData)
	 **/
	def calcSpiralCoords(nodes: Iterable[GraphNode],
	                     boundingBox: (Double, Double, Double, Double),
	                     nodeAreaNorm: Double,
	                     centralCommunityArea: Double,
	                     borderPercent: Double = 2.0,
	                     bSortByDegree: Boolean = false): (Array[LayoutNode], Double) = {

		val (xC, yC) = (0.0, 0.0) //(boundingBox._1 + boundingBox._3/2, boundingBox._2 + boundingBox._4/2)	// centre of bounding box
		val boundingBoxArea = boundingBox._3 * boundingBox._4

		if (borderPercent < 0.0 || borderPercent > 10.0) throw new IllegalArgumentException("borderPercent must be between 0 and 10")
		val border = borderPercent*0.01*Math.min(boundingBox._3, boundingBox._4)

		// Store community results in array with initial coords as xc,yc for now.
		// Array is sorted by community radius (or degree) -- smallest to largest
		var nodeCoords = nodes.map(n =>
			{
				val nodeArea = nodeAreaNorm * boundingBoxArea * n.internalNodes
				val nodeRadius = Math.sqrt(nodeArea * 0.31831)	//0.31831 = 1/pi
        LayoutNode(n, LayoutGeometry(xC, yC, nodeRadius))
			}
		).toList.sortBy(row => if (bSortByDegree) row.node.degree else row.geometry.radius).toArray

		val numNodes = nodeCoords.length

		//---- layout centre of spiral
		var n = numNodes-1
		val r0 =
      if (centralCommunityArea > 0.0) {
        // use central connected community as centre of spiral
        Math.sqrt(centralCommunityArea * 0.31831) //0.31831 = 1/pi
      } else {
        // no central connected community here, so use largest isolated community as centre of spiral instead
        n -= 1
        nodeCoords(n + 1).geometry.radius
      }

		//init spiral layout variables
		var r_prev = 0.0
		var r_delta = 0.0
		var Q_now_sum = 0.0
		var Q = 0.0		// current spiral angle	(in radians)
		var rQ = r0		// current spiral radius

		//---- layout 2nd community (to right of spiral centre)
		if (n >= 0) {
			val r_curr = nodeCoords(n).geometry.radius	// radius of current community

			rQ = rQ + r_curr + border   // update spiral radius
			val x = rQ * Math.cos(Q)    // get centre coords of current community and save results
			val y = rQ * Math.sin(Q)
			nodeCoords(n) = LayoutNode(nodeCoords(n).node, LayoutGeometry(x + xC, y + yC, r_curr))

			r_prev = r_curr             //save current community radius for next iteration
			r_delta = 2*r_curr + border //rate of r change per 2*pi radians (determines how tight or wide the spiral is)
			n -= 1
		}

		//---- layout rest of isolated communities
		while (n >= 0) {
			val r_curr = nodeCoords(n).geometry.radius     // radius of current community
			val d_arc = r_prev + r_curr + border  // distance between neighouring nodes in spiral
			val Q_curr = Math.acos((2*rQ*rQ - d_arc*d_arc)/(2*rQ*rQ)) 	// use cosine law to get spiral angle between neighbouring nodes
			Q += Q_curr
			Q_now_sum += Q_curr

			//rQ += r_delta*Q_curr/(2.0*Math.PI)	// increase r_delta over 2*pi rads
			if (Q_now_sum >= Math.PI) {
				rQ += r_delta*Q_curr/Math.PI		// increase r_delta over pi rads (produces a slightly tighter spiral)
			}

			val x = rQ * Math.cos(Q)	// get centre coords of current community and save results
			val y = rQ * Math.sin(Q)
			nodeCoords(n) = LayoutNode(nodeCoords(n).node, LayoutGeometry(x + xC, y + yC, r_curr))

			if (Q_now_sum > 2.0*Math.PI) {   // reset r_delta every 2*pi radians (for next level of spiral)
				Q_now_sum = 0.0
				r_delta = 2.0*r_curr + border	//rate of r change per 2*pi radians
			}

			r_prev = r_curr			//save current community radius for next iteration
			n -= 1
		}

		//---- Do final scaling of XY co-ordinates to fit within bounding area
		var maxDist = Double.MinValue
		for (n <- 0 until numNodes) {
			val xDist = xC - nodeCoords(n).geometry.x	// node distance to centre
			val yDist = yC - nodeCoords(n).geometry.y
			// calc distance plus node radius (to ensure all of a given node's circle fits into the bounding area)
			val dist = Math.sqrt(xDist*xDist + yDist*yDist) + nodeCoords(n).geometry.radius
			maxDist = Math.max(maxDist, dist)
		}

		val scaleFactor = 0.5*Math.min(boundingBox._3, boundingBox._4) / maxDist

		for (n <- 0 until numNodes) {
      val node = nodeCoords(n)
			// scale community radii too if scaleFactor < 1, so scaling doesn't cause communities to overlap
			val scaledRadius = if (scaleFactor < 1.0) node.geometry.radius*scaleFactor else node.geometry.radius
			nodeCoords(n) = LayoutNode(
        node.node,
        LayoutGeometry(
          node.geometry.x*scaleFactor + boundingBox._1 + 0.5*boundingBox._3,
          node.geometry.y*scaleFactor + boundingBox._2 + 0.5*boundingBox._4,
          scaledRadius)
      )
		}

		val scaledCentralArea = centralCommunityArea*scaleFactor*scaleFactor	// also scale centralCommunityArea accordingly (ie x square of scaleFactor)

		(nodeCoords, scaledCentralArea)
	}
}
