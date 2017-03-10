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
package software.uncharted.graphing.export

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession

class Exporter {
  private var sc:SparkContext = null

  def exportData(session: SparkSession, sourceLayoutDir:String, outputDir:String, dataDelimiter:String, maxLevel: Int) = {
    this.sc = session.sparkContext

    var allNodes: RDD[ClusteredNode] = sc.emptyRDD[ClusteredNode]
    var allEdges: RDD[ClusteredEdge] = sc.emptyRDD[ClusteredEdge]
    var communityRDD: RDD[(String, String)] = sc.emptyRDD[(String, String)]

    //Work from top to bottom to generate the data extract.
    for(level <- maxLevel to 0 by -1) {
      //Get the data for the level.
      val (levelNode, levelEdge) = extractLevel(sourceLayoutDir, dataDelimiter, level)

      //Join the node data to the previous level's community subset.
      val levelDataCommunity = levelNode.map(node => (node.parentId, node))
      val levelDataJoined = levelDataCommunity.leftOuterJoin(communityRDD)

      //Create the community hierarchy field.
      val levelDataCommunityHierarchy = levelDataJoined.map(e => {
        val (node, levelPath) = e._2
        new ClusteredNode(
          node.nodeId,
          node.xCoord,
          node.yCoord,
          node.radius,
          node.parentId,
          node.parentXCoord,
          node.parentYCoord,
          node.parentRadius,
          node.numInternalNodes,
          node.degree,
          node.level,
          levelPath.getOrElse(ClusteredObject.levelId(node.parentId, node.level)),  //Top level does not have a path defined, so it is initialized here.
          node.metaData)
      })

      //Add the level's data to the final set.
      allNodes = allNodes.union(levelDataCommunityHierarchy)
      allEdges = allEdges.union(levelEdge)

      //Create the community subset, which is used to generate the hierarchy field.
      communityRDD = levelDataCommunityHierarchy.map(node => (node.nodeId, node.inclusiveHierarchy()))
    }

    //Save the complete datasets.
    allNodes.saveAsTextFile(outputDir + "/nodes")
    allEdges.saveAsTextFile(outputDir + "/edges")
  }

  private def extractLevel(sourceLayoutDir:String, delimiter:String, level:Int): (RDD[ClusteredNode], RDD[ClusteredEdge]) = {

    val layoutData = sc.textFile(sourceLayoutDir + "/level_" + level)

    val nodeBuilder = (node: Array[String]) => new ClusteredNode(
      node(1),
      node(2),
      node(3),
      node(4),
      node(5),
      node(6),
      node(7),
      node(8),
      node(9),
      node(10),
      level,  //Data also contains level.
      "",
      node.drop(12) //Treat all other columns as metadata.
    )

    val edgeBuilder = (edge: Array[String]) => new ClusteredEdge(
      edge(1),
      edge(2),
      edge(3),
      edge(4),
      edge(5),
      edge(6),
      edge(7),
      edge(8),
      level
    )

    //Split into node & edge sets.
    val layoutNode = layoutData.filter(line => line.startsWith("node"))
    val layoutEdge = layoutData.filter(line => line.startsWith("edge"))

    //Map the data to columns.
    val nodeSplit = layoutNode.map(line => line.split(delimiter))
    val edgeSplit = layoutEdge.map(line => line.split(delimiter))

    //Wrap the data in clustered instances.
    val combinedNode = nodeSplit.map(line => nodeBuilder(line))
    val combinedEdge = edgeSplit.map(line => edgeBuilder(line))

    (combinedNode, combinedEdge)
  }
}

object ClusteredObject {
  private[export] def levelId (id: String, level: Int): String = {
    if(level >= 0) id + "_c_" + level else id
  }
}
