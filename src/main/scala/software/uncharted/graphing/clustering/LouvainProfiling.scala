/**
 * Copyright © 2014-2015 Uncharted Software Inc. All rights reserved.
 *
 * Property of Uncharted™, formerly Oculus Info Inc.
 * http://uncharted.software/
 *
 * This software is the confidential and proprietary information of
 * Uncharted Software Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Uncharted Software Inc.
 */
package software.uncharted.graphing.clustering

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import software.uncharted.graphing.RandomGraph
import software.uncharted.graphing.clustering.sotera.LouvainHarness

/**
 * Created by nkronenfeld on 10/26/15.
 */
object LouvainProfiling {
  // Time the running of a function, in nanoseconds
  def time[T] (fcn: () => T): (T, Long) = {
    val startTime = System.nanoTime()
    val result = fcn()
    val endTime = System.nanoTime()
    (result, (endTime - startTime))
  }

  def main (args: Array[String]) = {
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    Logger.getLogger("akka.event.slf4j").setLevel(Level.WARN)
    Logger.getLogger("org.spark-project.jetty").setLevel(Level.ERROR)
    Logger.getLogger("org.apache.spark").setLevel(Level.WARN)
    val sc = new SparkContext(new SparkConf().setAppName("Louvaine-Profiling"))
    val RG = new RandomGraph(sc)
    val g = RG.makeRandomGraph(1000, 1, 10000, 1, R => (((R.nextLong().abs) % 10)+ 1))
    g.cache
    for (i <- 1 to 10) {
      g.vertices.count
      g.edges.count
    }
    val LH = new LouvainHarness(0.15, 1)
    val results = time(() => LH.run(sc, g))
    println("Ran clustering on a random graph of 1000 nodes/10000 edges in "+(results._2/1000000000.0)+" seconds")
  }
}
