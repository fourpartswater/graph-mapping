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
package software.uncharted.graphing.clustering.utilities



import org.apache.spark.graphx.{Edge, Graph, VertexId}

import scala.reflect.ClassTag



/**
 * A class to help consolidate nodes within a graph
 * Created by nkronenfeld on 10/29/2015.
 */
object ClusterConsolidator {
  /**
   * Consolidate multiple nodes in a graph into a single node.  Actually, it just changes IDs, but if multiple IDs
   * change to the same value, it consolidates them into one node.
   *
   * @param graph The graph to consolidate
   * @param newNodeFcn A function to determine the new node ID of any given current node in the graph
   * @param mergeNodesFcn A function to merge the data from two existing nodes  If not given, one node's data will be
   *                      kept at random
   * @tparam VD The vertex type of the graph
   * @tparam ED The edge type of the graph
   * @return A new graph with nodes re-IDed and consolidated, but edges all original
   */
  def consolidate[VD: ClassTag, ED: ClassTag] (graph: Graph[VD, ED],
                                               newNodeFcn: (VertexId, VD) => VertexId,
                                               mergeNodesFcn: Option[(VD, VD) => VD] = None): Graph[VD, ED] = {
    val newEdges = graph.mapTriplets{et =>
      (
        newNodeFcn(et.srcId, et.srcAttr),
        newNodeFcn(et.dstId, et.dstAttr),
        et.attr
      )
    }.edges.map { edge =>
      new Edge(edge.attr._1, edge.attr._2, edge.attr._3)
    }
    val newNodes =
      mergeNodesFcn.map{fcn =>
        graph.vertices.map{case (id, data) =>
          (newNodeFcn(id, data), data)
        }.reduceByKey(fcn)
      }.getOrElse {
        graph.vertices.filter { case (id, data) =>
          val newId = newNodeFcn(id, data)
          id == newId
        }
      }

    Graph(newNodes, newEdges)
  }
}
