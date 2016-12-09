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



import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._



class GraphCSVParser {

  //  // loads CSV graph dataset and stores rows in an RDD with "edge" or "node" string as the key, and rest of row data as the value
  //  def loadCSVgraphData(sc: SparkContext, sourceDir: String, partitions: Int, delimiter: String): RDD[(String, String)] = {
  //
  //    val rawData = if (partitions <= 0) {
  //      sc.textFile(sourceDir)
  //    } else {
  //      sc.textFile(sourceDir, partitions)
  //    }
  //
  //    rawData.flatMap(row => {
  //          //val tokens = row.split(delimiter).map(_.trim())
  //          val firstDelim = row.indexOf(delimiter)
  //          var objType = ""
  //          if (firstDelim > 0) {
  //            var objType = row.substring(0, firstDelim-1)
  //          }
  //          if ((objType=="edge") || (objType=="node")) {
  //             val rowData = row.substring(firstDelim+1)
  //
  //
  //             Some((objType, rowData))
  //          }
  //          else {
  //             None
  //          }
  //    })
  //  }

  //----------------------
  // Parse edge data for a given hierarchical level
  //(assumes graph data has been louvain clustered using the spark-based graph clustering utility)
  def parseEdgeData(sc: SparkContext,
                    rawData: RDD[String],
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

  //----------------------
  // Parse node/community data for a given hierarchical level
  //(assumes graph data has been louvain clustered using the spark-based graph clustering utility)
  //nodeIDindex = column number of nodeID
  //parentIDindex = column number of parentID
  //internalNodesX = column number of num internal node in current community
  //degreeX = column number of community degree
  //bKeepExtraAttributes = bool to look for, and store extra node attributes (at the end of each record line)
  def parseNodeData(sc: SparkContext,
                    rawData: RDD[String],
                    delimiter: String,
                    nodeIDindex: Int=1,
                    parentIDindex: Int=2,
                    internalNodesX: Int=3,
                    degreeX: Int=4,
                    bKeepExtraAttributes: Boolean=true): RDD[GraphNode] = {

    val nAttrX = Math.max(Math.max(Math.max(nodeIDindex, parentIDindex), internalNodesX), degreeX)+1

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

case class GraphNode (id: Long, parentId: Long, internalNodes: Long, degree: Int, metadata: String)
case class GraphEdge (srcId: Long, dstId: Long, weight: Long)
