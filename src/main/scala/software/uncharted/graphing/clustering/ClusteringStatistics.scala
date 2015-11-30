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
