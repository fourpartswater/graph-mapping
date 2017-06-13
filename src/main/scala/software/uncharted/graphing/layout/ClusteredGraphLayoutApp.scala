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
package software.uncharted.graphing.layout


import java.util.Date

import scala.util.{Failure, Success}
import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.apache.spark.sql.SparkSession
import software.uncharted.graphing.layout.forcedirected.{ForceDirectedLayoutParameters, ForceDirectedLayoutParametersParser}
import software.uncharted.xdata.sparkpipe.jobs.AbstractJob


/**
  * A job that takes a hierarchically clustered graph and lays it out using a force-directed layout algorithm that has
  * two three main forces:
  *
  * <ul>
  *   <li> A general repulsion force between all nodes </li>
  *   <li> An attraction force between nodes that are linked by an edge </li>
  *   <li> A general gravitational or boundary force, to keep nodes from getting too far away from the center </li>
  * </ul>
  *
  * As the graph is clustered hierarchically, the layout also happens hierarchically.  This means that the single
  * community containing all nodes is placed in the center and given an area. All its direct children are then laid
  * out within its area, and each given an area.  All their children are then laid out within their respective parents'
  * area, and given their own area, etc, until all nodes are placed.
  *
  * The input is the output of the louvain clustering algorithm - see
  * software.uncharted.graphing.clustering.unithread.Community.display_partition
  *
  * The output transforms nodes to include location, size, parent location, parent size, and hierarchy level.  See
  * HierarchicFDLayout.saveLayoutResults for details
  */
object ClusteredGraphLayoutApp extends AbstractJob with Logging {


  /**
    * This function actually executes the task the job describes
    *
    * @param session A spark session in which to run spark processes in our job
    * @param config  The job configuration
    */
  override def execute(session: SparkSession, config: Config): Unit = {
    val hierarchicalLayoutConfig = HierarchicalLayoutConfigParser.parse(config) match {
      case Success(s) => s
      case Failure(f) =>
        error("Couldn't read hierarchical layout configuration", f)
        sys.exit(-1)
    }
    val forceDirectedLayoutConfig = ForceDirectedLayoutParametersParser.parse(config) match {
      case Success(s) => s
      case Failure(f) =>
        error("Couldn't read force-directed layout configuration", f)
        sys.exit(-1)
    }

    val fileStartTime = System.currentTimeMillis()

    // Hierarchical Force-Directed layout scheme
    info("\n\n\nStarting layout at " + new Date)
    HierarchicFDLayout.determineLayout(session.sparkContext, hierarchicalLayoutConfig, forceDirectedLayoutConfig)
    info("Layout complete at " + new Date + "\n\n\n")

    val fileEndTime = System.currentTimeMillis()
    info("Finished hierarchic graph layout job in " + ((fileEndTime - fileStartTime) / 60000.0) + " minutes")

    info("DONE!!")
  }


}
