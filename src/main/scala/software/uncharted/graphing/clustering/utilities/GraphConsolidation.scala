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
package software.uncharted.graphing.clustering.utilities

import org.apache.spark.rdd.RDD

/**
  * Functions to consolidate redundant elements in graphs
  */
object GraphConsolidation {
  /**
    * Consolidate edges in a graph that are between the same two nodes
    * @param edges A dataset of graph edges
    * @param aggregateMetadata A function to combine metadata from different edges
    * @tparam I The node ID type
    * @tparam N The edge weight type
    * @tparam T The metadata type
    * @return The same dataset, with edges between identical nodes combined
    */
  def consolidateEdges[I, N: Numeric, T] (edges: RDD[(I, I, N, T)],
                                          aggregateMetadata: (T, T) => T): RDD[(I, I, N, T)] = {
    val num = implicitly[Numeric[N]]
    import num.mkNumericOps //scalastyle:ignore

    edges.map { case (src, dst, weight, metadata) =>
      ((src, dst), (weight, metadata))
    }.reduceByKey((a, b) =>
      (a._1 + b._1, aggregateMetadata(a._2, b._2))
    ).map { case ((src, dst), (weight, metadata)) =>
      (src, dst, weight, metadata)
    }
  }

  /**
    * Consolidate edges in a graph that are between the same two nodes, when the graph has no metadata
    * @param edges A dataset of graph edges
    * @tparam I The node ID type
    * @tparam N The edge weight type
    * @return The same dataset, with edges between identical nodes combined
    */
  def consolidateEdges[I, N: Numeric] (edges: RDD[(I, I, N)]): RDD[(I, I, N)] = {
    consolidateEdges[I, N, Any](
      edges.map { case (src, dst, weight) => (src, dst, weight, Nil)},
      (a: Any, b: Any) => a
    ).map { case (src, dst, weight, metadata) =>
      (src, dst, weight)
    }
  }
}
