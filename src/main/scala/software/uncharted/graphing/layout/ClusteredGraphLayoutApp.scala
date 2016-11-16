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
package software.uncharted.graphing.layout


import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import software.uncharted.graphing.layout.forcedirected.ForceDirectedLayoutParameters
import software.uncharted.graphing.utilities.ArgumentParser
import software.uncharted.xdata.sparkpipe.jobs.AbstractJob


object ClusteredGraphLayoutApp extends AbstractJob with Logging {


  /**
    * This function actually executes the task the job describes
    *
    * @param session A spark session in which to run spark processes in our job
    * @param config  The job configuration
    */
  override def execute(session: SparkSession, config: Config): Unit = {
    val hierarchicalLayoutConfig = HierarchicalLayoutConfig(config).getOrElse {
      error("Couldn't read hierarchical layout configuration")
      sys.exit(-1)
    }
    val forceDirectedLayoutConfig = ForceDirectedLayoutParameters(config).getOrElse{
      error("Couldn't read force-directed layout configuration")
      sys.exit(-1)
    }

		val fileStartTime = System.currentTimeMillis()

		// Hierarchical Force-Directed layout scheme
		val layouter = new HierarchicFDLayout()

		layouter.determineLayout(session.sparkContext, hierarchicalLayoutConfig, forceDirectedLayoutConfig)

		val fileEndTime = System.currentTimeMillis()
		println("Finished hierarchic graph layout job in "+((fileEndTime-fileStartTime)/60000.0)+" minutes")

		println("DONE!!")
	}


}
