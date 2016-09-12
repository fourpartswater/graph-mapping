package software.uncharted.graphing.export

import org.apache.spark.rdd.RDD
import org.apache.spark.SparkContext

/**
  * Created by phorne on 9/9/2016.
  */
class Exporter {
  private var sc:SparkContext = null

  def exportData(sc: SparkContext, sourceClusteringDir: String, sourceLayoutDir:String, outputDir:String, dataDelimiter:String, maxLevel: Int) = {
    this.sc = sc

    var allNodes: RDD[ClusteredNode] = sc.emptyRDD[ClusteredNode]
    var communityRDD: RDD[(String, String)] = sc.emptyRDD[(String, String)]
    //Work from top to bottom to generate the data extract.
    for(level <- maxLevel to 0 by -1) {
      //Get the data for the level.
      val levelData = extractLevel(sourceClusteringDir, sourceLayoutDir, dataDelimiter, level)

      //Join it to the previous level's community subset.
      val levelDataCommunity = levelData.map(node => (node.parentId, node))
      val levelDataJoined = levelDataCommunity.leftOuterJoin(communityRDD)

      //Create the community hierarchy field.
      val levelDataCommunityHierarchy = levelDataJoined.map(e =>
        new ClusteredNode(
          e._2._1.nodeId,
          e._2._1.xCoord,
          e._2._1.yCoord,
          e._2._1.radius,
          e._2._1.parentId,
          e._2._1.parentXCoord,
          e._2._1.parentYCoord,
          e._2._1.parentRadius,
          e._2._1.numInternalNodes,
          e._2._1.degree,
          e._2._1.level,
          if (e._2._2.isDefined) e._2._2.get else e._2._1.levelId(e._2._1.parentId, e._2._1.level),
          e._2._1.metaData))

      //Add the level's data to the final set.
      allNodes = allNodes.union(levelDataCommunityHierarchy)

      //Create the community subset, which is used to generate the hierarchy field.
      communityRDD = levelDataCommunityHierarchy.map(node => (node.nodeId, node.inclusiveHierarchy()))
    }

    //Save the complete dataset.
    allNodes.saveAsTextFile(outputDir)
  }

  private def extractLevel(sourceClusteringDir: String, sourceLayoutDir:String, delimiter:String, level:Int): RDD[ClusteredNode] = {

    val clusteringData = sc.textFile(sourceClusteringDir + "/level_" + level)
    val layoutData = sc.textFile(sourceLayoutDir + "/level_" + level)

    //Only keep node lines.
    val clusteringNode = clusteringData.filter(line => line.startsWith("node"))
    val layoutNode = layoutData.filter(line => line.startsWith("node"))

    //Map the data to pairwise RDDs for joining.
    val clusteringSplit = clusteringNode.map(line => line.split(delimiter))
    val clusteringKeyed = clusteringSplit.map(entry => (entry(1), entry))
    val layoutSplit = layoutNode.map(line => line.split(delimiter))
    val layoutKeyed = layoutSplit.map(entry => (entry(1), entry))

    //Join the clustering and layout outputs together to create a set for that level.
    //RIGHT NOW WE ONLY REALLY USE LAYOUT DATA.
    val combinedData = clusteringKeyed.join(layoutKeyed).map(node =>
      new ClusteredNode(
        node._1,
        node._2._2(2),
        node._2._2(3),
        node._2._2(4),
        node._2._2(5),
        node._2._2(6),
        node._2._2(7),
        node._2._2(8),
        node._2._2(9),
        node._2._2(10),
        level,
        "",
        if (node._2._2.length > 12) node._2._2(12) else ""))

    return combinedData
  }
}
