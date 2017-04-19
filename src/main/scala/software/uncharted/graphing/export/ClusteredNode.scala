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

/**
  * Wrapper for a clustered node.
  * @param nodeId Id of the node
  * @param xCoord X coordinate of the node in the layout
  * @param yCoord Y coordinate of the node in the layout
  * @param radius Radius of the node / community
  * @param parentId Id of the node's parent
  * @param parentXCoord X coordinate of the node's parent
  * @param parentYCoord Y coordinate of the node's parent
  * @param parentRadius Radious of the node's parent
  * @param numInternalNodes Number of nodes within the community
  * @param degree Degree of the community
  * @param level Clustering level
  * @param communityHierarchy Hierarchy from the node all the way to the top community
  * @param metaData All metadata associated with the node
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
                     val metaData: Array[String]) extends Serializable {

  override def toString() : String = {
    "node\t" + levelId() + "\t" + xCoord + "\t" + yCoord + "\t" + radius + "\t" + parentId + "\t" + parentXCoord +
      "\t" + parentYCoord + "\t" + parentRadius + "\t" + numInternalNodes + "\t" + degree + "\t" + level + "\t" +
      inclusiveHierarchy() + "\t" + metaData.mkString("\t")
  }

  /**
    * Get the node's unique id, combining the id with the level.
    * @return Id of the node that is unique throughout the clustered hierarchy.
    */
  def levelId() : String = {
    //The level # tracks the parent data. A node is therefore level - 1.
    levelId(nodeId, level-1)
  }

  /**
    * Get the node's unique id, combining the id with the level.
    * @param id Id of the node
    * @param level Level of the node
    * @return Id of the node that is unique throughout the clustered hierarchy.
    */
  def levelId(id : String, level : Int) : String = {
    if(level >= 0) id + "_c_" + level else id
  }

  /**
    * Get the hierarchy to the top community, including this node.
    * @return The string representation of the node's hierarchy.
    */
  def inclusiveHierarchy() : String = {
    //A node is a parent in the level - 1.
    if (communityHierarchy.length() > 0) communityHierarchy + "|" + levelId(nodeId, level - 1) else levelId(nodeId, level - 1)
  }
}
