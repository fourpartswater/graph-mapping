package software.uncharted.graphing.clustering.sotera

import java.util.Date

/**
 * Created by nkronenfeld on 11/10/15.
 */
case class IterationStats (level: Int,
                           startTime: Long,
                           endTime: Long,
                           nodes: Long,
                           links: Long,
                           weight: Double,
                           startModularity: Double,
                           endModularity: Double) {
  def print: Unit = {
    println("level "+level)
    println("\tStart computation: "+new Date(startTime))
    println("\tnetwork size: %d nodes, %d links, %f weight".format(nodes, links, weight))
    println("\tmodularity increased from %f to %f".format(startModularity, endModularity))
    println("\tEnd computation: "+new Date(endTime))
    println("\t\tTotal time to cluster level %d: %.3f seconds".format(level, (endTime-startTime)/1000.0))
  }
}
