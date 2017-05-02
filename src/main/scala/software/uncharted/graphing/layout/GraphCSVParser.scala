/**
  * Copyright (c) 2014-2017 Uncharted Software Inc. All rights reserved.
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



import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._ //scalastyle:ignore


/**
  * This class handles parsing the output of the clustering algorithm for the layout routines.
  *
  * This code is copied from previous iterations; it is a bit wierd in that it assumes part of the output format
  * of the clustering (namely, that each edge line begins with the string "edge" and each node line begins with the
  * string "node", and that node metadata consists of everything but the four listed attributes), but doesn't assume
  * other parts (like the column numbers).
  */
class GraphCSVParser {
  /**
    * Parse the edge data for a single level of the hierarchically clustered graph.
    *
    * @param rawData A distributed dataset containing this level's edge data
    * @param delimiter The delimiter used between fields of the data
    * @param edgeSrcIDindex The index of the delimited column containing the source ID of each edge
    * @param edgeDstIDindex The index of the delimited column containing the destinatin ID of each edge
    * @param edgeWeightIndex The index of the delimited column containing the weight of each edge
    * @return A distributed dataset of all the readable edges in the input dataset
    */
  //----------------------
  // Parse edge data for a given hierarchical level
  //(assumes graph data has been louvain clustered using the spark-based graph clustering utility)
  def parseEdgeData(rawData: RDD[String],
                    delimiter: String,
                    edgeSrcIDindex: Int=1,
                    edgeDstIDindex: Int=2,
                    edgeWeightIndex: Int=3): RDD[Edge[Long]] = {

    rawData.mapPartitionsWithIndex { case (partition, iterator) =>
        iterator
    }.flatMap(row =>
      {
        val tokens = row.split(delimiter).map(_.trim())
        if (tokens(0) == "edge") {
          val srcID = tokens(edgeSrcIDindex).toLong
          val dstID = tokens(edgeDstIDindex).toLong
          val weight = if (edgeWeightIndex == -1) 1L else tokens(edgeWeightIndex).toLong
          Some(new Edge(srcID, dstID, weight))
        }
        else {
          None
        }
      }
    )
  }

  /**
    * Parse the edge data for a single level of the hierarchically clustered graph.
    *
    * @param rawData A distributed dataset containing this level's node data
    * @param delimiter The delimiter used between fields of the data
    * @param nodeIDindex The index of the delimited column containing the ID of each node
    * @param parentIDindex The index of the delimited column containing the parent ID of each node
    * @param internalNodesX The index of the delimited column containing the number of internal nodes contained in
    *                       each node
    * @param degreeX The index of the delimited column containing the degree of each node
    * @param bKeepExtraAttributes Whether or not to look for, and store, extra node attributes (found at the end of
    *                             each record, after the above 4 columns)
    * @return A distributed dataset of all the readable nodes found in input dataset
    */
  def parseNodeData(rawData: RDD[String],
                    delimiter: String,
                    nodeIDindex: Int=1,
                    parentIDindex: Int=2,
                    internalNodesX: Int=3,
                    degreeX: Int=4,
                    bKeepExtraAttributes: Boolean=true): RDD[GraphNode] = {

    val nAttrX = Math.max(Math.max(Math.max(nodeIDindex, parentIDindex), internalNodesX), degreeX) + 1

    rawData.flatMap(row =>
      {
        val tokens = row.split(delimiter).map(_.trim())
        if (tokens(0) == "node") {
          val id = tokens(nodeIDindex).toLong
          val parentID = tokens(parentIDindex).toLong
          val internalNodes = tokens(internalNodesX).toLong
          val degree = tokens(degreeX).toInt
          val metaData = if (bKeepExtraAttributes) {
            // Preserve everything after our important attributes
            (nAttrX until tokens.length).map(n => tokens(n)).mkString(delimiter)
          } else {
            ""
          }
          Some(GraphNode(id, parentID, internalNodes, degree, metaData))
        }
        else {
          None
        }
      }
    )
  }
}

/**
  * Wrapper class for a graph node
  * @param id Node id
  * @param parentId Node's parent id
  * @param internalNodes Number of nodes within the community
  * @param degree Number of edges of the node
  * @param metadata Metadata associated with the node
  */
case class GraphNode (id: Long, parentId: Long, internalNodes: Long, degree: Int, metadata: String)

/**
  * Wrapper class for a graph edge
  * @param srcId Source node id
  * @param dstId Destination node id
  * @param weight Edge weight
  */
case class GraphEdge (srcId: Long, dstId: Long, weight: Long)
