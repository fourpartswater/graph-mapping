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
    "node\t" + levelId() + "\t" + xCoord + "\t" + yCoord + "\t" + radius + "\t" + ClusteredObject.levelId(parentId, level) + "\t" + parentXCoord + "\t" + parentYCoord + "\t" + parentRadius + "\t" + numInternalNodes + "\t" + degree + "\t" + level + "\t" + inclusiveHierarchy() + "\t" + metaData.mkString("\t")
  }

  def levelId() : String = {
    //The level # tracks the parent data. A node is therefore level - 1.
    ClusteredObject.levelId(nodeId, level-1)
  }

  def inclusiveHierarchy() : String = {
    //A node is a parent in the level - 1.
    if (communityHierarchy.length() > 0) communityHierarchy + "|" + ClusteredObject.levelId(nodeId, level - 1) else ClusteredObject.levelId(nodeId, level - 1)
  }
}
