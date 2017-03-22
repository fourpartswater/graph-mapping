//scalastyle:off
package software.uncharted.graphing.clustering.usc

/**
 * This package is an attempt to translate the map/reduce version of the USC louvain clustering algorithm (found at
 * https://github.com/usc-cloud/hadoop-louvain-community/tree/master/src/main/java/edu/usc/pgroup/louvain/hadoop) to
 * scala/spark code
 *
 * For original code works as follows:
 *  * The main class (LouvainMR) sets up the map/reduce job.
 *    * Mapper: MapCommunity
 *      - reads in a community (from an input stream formed from the bytes of the input key)
 *      - calls one_level
 *      - converts the results to a graph
 *      - creates a GraphMessage - essentially a case class with
 *        - number of links
 *        - number of nodes
 *        - total weight
 *        - links
 *        - degrees
 *        - weights
 *        - remoteMap
 *        - node-to-community mapping
 *        - current partition
 *        Essentially, in total, this is just
 *        - the resultant sub-graph
 *        - the list of changes made to get to it
 *        - the partition from which it came
 *      - sends the GraphMessage to the reducer
 *    * Reducer: ReduceCommunity
 *      - Reconstruct the graph
 *        - Read the graph messages from each mapper
 *          - put into a map from partition to graph message
 *        - Transform graph messages to graphs
 *        - Renumber graphs
 *        - Merge local portions of graphs
 *        - Merge remote portions of graphs
 *      - Run standard Louvain on merged result
 *
 * It looks to me like the algorithm is doing the distributed clustering ignoring remote nodes.  The graph object
 * there has a list of links to truely remote nodes (called 'remoteMaps'), and lists of links to formerly remote
 * nodes (called 'links' and 'weights')
 */
package object reference {

}
//scalastyle:on
