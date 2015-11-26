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
}
