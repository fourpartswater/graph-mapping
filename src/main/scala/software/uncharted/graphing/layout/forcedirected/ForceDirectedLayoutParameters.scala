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

import scala.util.Random

/**
  * General parameters on how force-directed layout should be run
  */
class ForceDirectedLayoutParameters {
  // boolean for whether community circles overlap or not
  var nodesAreOverlapping = false
  // constant used for extra strong repulsion if node 'circles' overlap
  var overlappingNodesRepulsionFactor = Math.pow(1000.0/256, 2.0)

  // num of nodes threshold for whether or not to use quadtree decomposition
  var quadTreeNodeThreshold = 20
  // theta value for quad tree decomposition
  // must be >= 0.  Lower values give more accurate repulsion force results, but are less efficient
  var quadTreeTheta = 1.0
  var borderPercent: Double = 2.0
  var maxIterations: Int = 1000
  var useEdgeWeights: Boolean = false
  var useNodeSizes: Boolean = false
  var nodeAreaFactor: Double = 0.3
  val stepLimitFactor = 0.001
  var gravity: Double = 0.0
  var isolatedDegreeThreshold: Int = 0

  lazy val alphaCool = capToBounds(1.0 + math.log(stepLimitFactor) * 4.0 / maxIterations, 0.8, 0.99)
  lazy val alphaCoolSlow = capToBounds(1.0 + math.log(stepLimitFactor) * 2.0 / maxIterations, 0.8, 0.99)

  private def capToBounds (value: Double, minValue: Double, maxValue: Double): Double =
    ((value min maxValue) max minValue)

  var randomSeed = Some(911)
}

/**
  * Global parameters
  */
object ForceDirectedLayoutParameters {
  // num of nodes threshold for whether or not to use quadtree decomposition
  val QT_NODE_THRES = 20
  // theta value for quadtree decomposition
  // (>= 0; lower value gives more accurate repulsion force results, but is less efficient)
  val QT_THETA = 1.0
}

/**
  * Specific terms describing how a particular set of nodes should be laid out
  * @param numNodes The number of nodes being laid out
  * @param maxRadius The maximum radius within which to lay out nodes
  * @param parameters The global layout parameters constraining all distributions of nodes
  * @param getMaxEdgeWeight A function to get the maximum edge weight, if it is needed.
  */
class ForceDirecedLayoutTerms (numNodes: Int, maxRadius: Double,
                               parameters: ForceDirectedLayoutParameters,
                               getMaxEdgeWeight: => Double) {
  var useQuadTree = numNodes > parameters.quadTreeNodeThreshold
  var kSq: Double = math.Pi * maxRadius * maxRadius / numNodes
  var kInv: Double = 1.0 / math.sqrt(kSq)
  val squaredStepLimit = maxRadius*maxRadius * parameters.stepLimitFactor
  val initialTemperature: Double = 0.5 * maxRadius
  var temperature: Double = initialTemperature
  var totalEnergy: Double = Double.MaxValue
  var edgeWeightNormalizationFactor: Option[Double] = if (parameters.useEdgeWeights) {
    val maxWeight = getMaxEdgeWeight
    if (maxWeight > 0) {
      Some(1.0 / maxWeight)
    } else {
      None
    }
  } else {
    None
  }

  var overlappingNodes: Boolean = false
  // constant used for extra strong repulsion force if node regions overlap
  val nodeOverlapRepulsionFactor = maxRadius * maxRadius / (parameters.stepLimitFactor * parameters.stepLimitFactor)
  // Used to update some parameters every nth iteration
  var progressCount = 0
}
