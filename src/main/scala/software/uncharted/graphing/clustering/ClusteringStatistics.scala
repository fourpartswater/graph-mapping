/**
 * Copyright © 2014-2015 Uncharted Software Inc. All rights reserved.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 * http://uncharted.software/
 *
 * This software is the confidential and proprietary information of
 * Uncharted Software Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Uncharted Software Inc.
 */
package software.uncharted.graphing.clustering

/**
 * A class to track statistics about a Louvain-based clustering job
 */
case class ClusteringStatistics (level: Int,
                                 partition: Int,
                                 iterations: Int,
                                 startModularity: Double,
                                 startNodes: Int,
                                 startLinks: Long,
                                 endModularity: Double,
                                 endNodes: Int,
                                 endLinks: Long,
                                 timeToCluster: Long) {
  def addLevelAndPartition (newLevel: Int, newPartition: Int) =
    ClusteringStatistics(
      newLevel, newPartition, iterations,
      startModularity, startNodes, startLinks,
      endModularity, endNodes, endLinks,
      timeToCluster
    )

  override def toString: String = {
     """{"level": %d, "partition": %d, "iterations": %d,
       | "start": {"modularity": %.6f, "nodes": %d, "links": %d},
       | "end": {"modularity": %.6f, "nodes": %d, "links": %d},
       | "time": %.3f}""".stripMargin.format(
      level, partition, iterations, startModularity, startNodes, startLinks, endModularity, endNodes, endLinks, (timeToCluster/1000.0))
  }
}
