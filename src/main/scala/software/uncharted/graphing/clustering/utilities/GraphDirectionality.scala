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

import org.apache.spark.graphx.Graph
import org.apache.spark.rdd.RDD

import scala.reflect.ClassTag

/**
  * Utility functions to determine if a graph is bidirectional or unidirectional, and to convert between them
  * where possible
  */
object GraphDirectionality {
  /**
    * Determine if a graph is bidirectional or unidirectional
    */
  def isBidirectional[T] (edges: RDD[T], getSrcId: T => Long, getDstId: T => Long): Boolean = {
    edges.flatMap { t =>
      val src = getSrcId(t)
      val dst = getDstId(t)
      Seq(((src, dst), (1, 0)), ((dst, src), (0, 1)))
    }.reduceByKey { (a, b) =>
      (a._1 + b._1, a._2 + b._2)
    }.filter { case (a, b) =>
      a != b
    }.take(1).length == 0
  }

  /**
    * Take an edge list that is not bidirectional and make it bidirectional
    */
  def makeBidirectional[T: ClassTag] (edges: RDD[T], reverse: T => T): RDD[T] =
    edges.flatMap(t => Seq(t, reverse(t)))
}
