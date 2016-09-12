package software.uncharted.graphing.export

/**
  * Created by phorne on 9/9/2016.
  */
class ClusteredNode (val nodeId: String,
                     val xCoord: String,
                     val yCoord: String,
                     val radius: String,
                     val parentId: String,
                     val parentXCoord: String,
                     val parentYCoord: String,
                     val parentRadius: String,
                     val numInternalNodes: String,
                     val degree: String,
                     val level: Int,
                     val communityHierarchy: String,
                     val metaData: String) {

  override def toString() : String = {
    return "node\t" + levelId() + "\t" + xCoord + "\t" + yCoord + "\t" + radius + "\t" + parentId + "\t" + parentXCoord + "\t" + parentYCoord + "\t" + parentRadius + "\t" + numInternalNodes + "\t" + degree + "\t" + level + "\t" + inclusiveHierarchy() + "\t" + metaData
  }

  def levelId() : String = {
    //The level # tracks the parent data. A node is therefore level - 1.
    return levelId(nodeId, level-1)
  }

  def levelId(id : String, level : Int) : String = {
    return if(level >= 0) id + "_c_" + level else id
  }

  def inclusiveHierarchy() : String = {
    //A node is a parent in the level - 1.
    return if (communityHierarchy.length() > 0) communityHierarchy + "|" + levelId(nodeId, level - 1) else levelId(nodeId, level - 1)
  }
}
