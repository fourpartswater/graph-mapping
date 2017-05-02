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
package software.uncharted.graphing.layout.forcedirected

/** A representation of an edge, to use when laying out a graph
  *
  * @param srcIndex The index of the source node in the source list
  * @param dstIndex The index of the destination node in the destination list
  * @param weight The weight of this edge
  */
case class LayoutEdge (srcIndex: Int, dstIndex: Int, weight: Long)
