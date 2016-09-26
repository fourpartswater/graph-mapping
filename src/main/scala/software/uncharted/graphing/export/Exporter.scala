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

class Exporter {
  private var sc:SparkContext = null

  def exportData(sc: SparkContext, sourceLayoutDir:String, outputDir:String, dataDelimiter:String, maxLevel: Int) = {
    this.sc = sc

    var allNodes: RDD[ClusteredNode] = sc.emptyRDD[ClusteredNode]
    var communityRDD: RDD[(String, String)] = sc.emptyRDD[(String, String)]
    //Work from top to bottom to generate the data extract.
    for(level <- maxLevel to 0 by -1) {
      //Get the data for the level.
      val levelData = extractLevel(sourceLayoutDir, dataDelimiter, level)

      //Join it to the previous level's community subset.
      val levelDataCommunity = levelData.map(node => (node.parentId, node))
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
          levelPath.getOrElse(node.levelId(node.parentId, node.level)),  //Top level does not have a path defined, so it is initialized here.
          node.metaData)
      })

      //Add the level's data to the final set.
      allNodes = allNodes.union(levelDataCommunityHierarchy)

      //Create the community subset, which is used to generate the hierarchy field.
      communityRDD = levelDataCommunityHierarchy.map(node => (node.nodeId, node.inclusiveHierarchy()))
    }

    //Save the complete dataset.
    allNodes.saveAsTextFile(outputDir)
  }

  private def extractLevel(sourceLayoutDir:String, delimiter:String, level:Int): RDD[ClusteredNode] = {

    val layoutData = sc.textFile(sourceLayoutDir + "/level_" + level)

    //Only keep node lines.
    val layoutNode = layoutData.filter(line => line.startsWith("node"))

    //Map the data to columns.
    val layoutSplit = layoutNode.map(line => line.split(delimiter))

    //Wrap the data in clustered node instances.
    val combinedData = layoutSplit.map(node =>
      new ClusteredNode(
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
        node.drop(12))) //Treat all other columns as metadata/

    return combinedData
  }
}
