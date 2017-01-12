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

import com.typesafe.config.Config
import software.uncharted.xdata.sparkpipe.config.ConfigParser

import scala.util.{Try, Random}



/**
  * General parameters on how force-directed layout should be run
  *
  * @param overlappingNodeRepulsionFactor A factor affecting how much overlapping nodes push each other away.
  * @param nodeAreaFactor The proportion of a node that children should expect to cover.  Default is 0.3
  * @param stepLimitFactor A factor affecting how fast the layout is allowed to converge
  * @param borderPercent Percent of parent bounding box to leave as whitespace between neighbouring communities
  *                      during initial layout.  Default = 2%
  * @param isolatedDegreeThreshold Nodes with external degree <= this number will be considered "isolated", and will
  *                                not be laid out in the central area.  Default is 0.
  * @param quadTreeNodeThreshold If there are more nodes than this threshold, use a quad tree when calculating
  *                              repulsion forces
  * @param quadTreeTheta When a quad tree cell is smaller than this proportion times the distance to the node in
  *                      question, treat the whole cell as a point source with respect to repulsion forces.
  * @param proportionalConstraint A constant governing how strong a proportional constraint towards the center is.
  *                               If non-0, a proportional constraint will be used that pushes nodes towards the
  *                               center, pushing harder the farther they are from the center.  If 0, a bounding
  *                               force will be used instead that doesn't push nodes until they cross a boundary,
  *                               but pushes them strongly towards the center once that boundary is crossed.
  * @param maxIterations The maximum number of iterations to take to achieve layout convergence.  Default is 500.
  * @param useEdgeWeights True to use edge weights when determining layout; false to assume all edges have a
  *                       strength of 1.  Default is false.
  * @param useNodeSizes True to use node sizes when determining layout.  Not sure what the results of leaving this
  *                     false would be
  * @param randomHeatingDeceleration A parameter that determines how quickly the insertion random intermittent heating
  *                                  events (so as to prevent getting trapped in local minima) decelerates.  The higher
  *                                  the number, the more quickly the random heating decelerates (i.e, the less often
  *                                  the random heating occurs).  A value of 1.0, the default, means the chance of
  *                                  random heating when it might be called for decreases linearly over the course of
  *                                  a layout.
  * @param randomSeed A potential random seed to allow consistency when repeating layouts.
  */
case class ForceDirectedLayoutParameters (
                                           overlappingNodeRepulsionFactor: Double,
                                           nodeAreaFactor: Double,
                                           stepLimitFactor: Double,
                                           borderPercent: Double,
                                           isolatedDegreeThreshold: Int,
                                           quadTreeNodeThreshold: Int,
                                           quadTreeTheta: Double,
                                           proportionalConstraint: Double,
                                           maxIterations: Int,
                                           useEdgeWeights: Boolean,
                                           useNodeSizes: Boolean,
                                           randomHeatingDeceleration: Double,
                                           randomSeed: Option[Long]
                                         ) {
  /** The amount by which to multiply the temperature when cooling when all is going well */
  lazy val alphaCool = capToBounds(1.0 + math.log(stepLimitFactor) * 4.0 / maxIterations, 0.8, 0.99)
  /** The amount by which to multiply the temperature when cooling when there are overlapping nodes */
  lazy val alphaCoolSlow = capToBounds(1.0 + math.log(stepLimitFactor) * 2.0 / maxIterations, 0.8, 0.99)

  private def capToBounds (value: Double, minValue: Double, maxValue: Double): Double =
    ((value min maxValue) max minValue)
}
/**
  * A parser for reading force-directed layout parameters
  */
object ForceDirectedLayoutParametersParser extends ConfigParser {

  private val SECTION_KEY = "layout.force-directed"
  private val OVERLAPPING_NODES_REPULSION_FACTOR_KEY = "overlapping-nodes-repulsion-factor"
  private val NODE_AREA_FACTOR_KEY = "node-area-factor"
  private val STEP_LIMIT_FACTOR_KEY = "step-limit-factor"
  private val BORDER_PERCENT_KEY = "border-percent"
  private val ISOLATED_DEGREE_THRESHOLD_KEY = "isolated-degree-threshold"
  private val QUAD_TREE_NODE_THRESHOLD_KEY = "quad-tree-node-threshold"
  private val QUAD_TREE_TEHTA_KEY = "quad-tree-theta"
  private val PROPORTIONAL_CONSTRAINT_KEY = "proportional-constraint"
  private val MAX_ITERATIONS_KEY = "max-iterations"
  private val USE_EDGE_WEIGHTS_KEY = "use-edge-weights"
  private val USE_NODE_SIZES_KEY = "use-node-sizes"
  private val RANDOM_HEATING_KEY = "random-heating-deceleration"
  private val RANDOM_SEED_KEY = "random-seed"

  private[forcedirected] val defaultOverlappingNodesRepulsionFactor = (1000.0 * 1000.0) / (256.0 * 256.0)
  private[forcedirected] val defaultNodeAreaFactor = 0.3
  private[forcedirected] val defaultStepLimitFactor = 0.001
  private[forcedirected] val defaultBorderPercent = 2.0
  private[forcedirected] val defaultIsolatedDegreeThreshold = 0
  private[forcedirected] val defaultQuadTreeNodeThreshold = 20
  private[forcedirected] val defaultQuadTreeTheta = 1.0
  private[forcedirected] val defaultProportionalConstraint = 0.0
  private[forcedirected] val defaultMaxIterations = 500
  private[forcedirected] val defaultUseEdgeWeights = false
  private[forcedirected] val defaultUseNodeSizes = false
  private[forcedirected] val defaultRandomHeating = 1.0
  private[forcedirected] val defaultRandomSeed = 911L

  /**
    * Extract force-directed layout configuration details from a more general configuration set
    *
    * @param config The general configuration set
    * @return Those portions of the configuration directly relevant to force-directed layout
    */
  def parse(config: Config): Try[ForceDirectedLayoutParameters] = {
    Try {
      val section = config.getConfig(SECTION_KEY)

      ForceDirectedLayoutParameters(
        getDouble(section, OVERLAPPING_NODES_REPULSION_FACTOR_KEY, defaultOverlappingNodesRepulsionFactor),
        getDouble(section, NODE_AREA_FACTOR_KEY, defaultNodeAreaFactor),
        getDouble(section, STEP_LIMIT_FACTOR_KEY, defaultStepLimitFactor),
        getDouble(section, BORDER_PERCENT_KEY, defaultBorderPercent),
        getInt(section, ISOLATED_DEGREE_THRESHOLD_KEY, defaultIsolatedDegreeThreshold),
        getInt(section, QUAD_TREE_NODE_THRESHOLD_KEY, defaultQuadTreeNodeThreshold),
        getDouble(section, QUAD_TREE_TEHTA_KEY, defaultQuadTreeTheta),
        getDouble(section, PROPORTIONAL_CONSTRAINT_KEY, defaultProportionalConstraint),
        getInt(section, MAX_ITERATIONS_KEY, defaultMaxIterations),
        getBoolean(section, USE_EDGE_WEIGHTS_KEY, defaultUseEdgeWeights),
        getBoolean(section, USE_NODE_SIZES_KEY, defaultUseNodeSizes),
        getDouble(section, RANDOM_HEATING_KEY, defaultRandomHeating),
        getRandomSeed(section)
      )
    }
  }

  private def getRandomSeed(config: Config): Option[Long] = {
    if (!config.hasPath(RANDOM_SEED_KEY)) {
      Some(defaultRandomSeed)
    } else {
      config.getString(RANDOM_SEED_KEY).toLowerCase.trim match {
        case "time" => Some(System.currentTimeMillis())
        case "none" => None
        case _ => Some(config.getLong(RANDOM_SEED_KEY))
      }
    }
  }
}

/**
  * Specific terms describing how a particular set of nodes should be laid out
  *
  * @param numNodes The number of nodes being laid out
  * @param maxRadius The maximum radius within which to lay out nodes
  * @param parameters The global layout parameters constraining all distributions of nodes
  * @param getMaxEdgeWeight A function to get the maximum edge weight, if it is needed.
  */
class ForceDirectedLayoutTerms (numNodes: Int, maxRadius: Double,
                                val parameters: ForceDirectedLayoutParameters,
                                getMaxEdgeWeight: => Double) {
  var useQuadTree = numNodes > parameters.quadTreeNodeThreshold
  var kSq: Double = math.Pi * maxRadius * maxRadius / numNodes
  var kInv: Double = 1.0 / math.sqrt(kSq)
  val squaredStepLimit = maxRadius*maxRadius * parameters.stepLimitFactor
  // Initial temperature determining how fast the layout cools.  This used to be half maxRadius; I've lowered it
  // significantly because the the layout was bouncing around a lot under the old number.
  val initialTemperature: Double = 0.1 * maxRadius
  var temperature: Double = initialTemperature
  var totalEnergy: Double = Double.MinValue
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

  // Variable sed to track whether there are overlapping nodes left in the layout
  var overlappingNodes: Boolean = false
  // Constant used for extra strong repulsion force if node regions overlap.
  val nodeOverlapRepulsionFactor = 64.0
  // Variable used to update some parameters every nth iteration
  var progressCount = 0
}
