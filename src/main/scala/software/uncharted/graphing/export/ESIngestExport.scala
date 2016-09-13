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

import org.apache.spark.{SparkConf, SparkContext}
import software.uncharted.graphing.utilities.ArgumentParser

object ESIngestExport {
  def main(args: Array[String]) {
    val argParser = new ArgumentParser(args)

    val sc = new SparkContext(new SparkConf().setAppName("Node Tiling"))

    val sourceClusteringDir = argParser.getStringOption("sourceClustering", "The source directory where to find clustered graph data", None).get
    val sourceLayoutDir = argParser.getStringOption("sourceLayout", "The source directory where to find graph layout data", None).get
    val outputDir = argParser.getStringOption("output", "The output location where to save data", None).get
    val dataDelimiter = argParser.getString("d", "Delimiter for the source graph data. Default is tab-delimited", "\t")
    val maxHierarchyLevel = argParser.getInt("maxLevel","Max cluster hierarchy level when the data was clustered", 0)

    val fileStartTime = System.currentTimeMillis()

    // Hierarchical Force-Directed layout scheme
    val exporter = new Exporter()

    exporter.exportData(sc,
      sourceClusteringDir,
      sourceLayoutDir,
      outputDir,
      dataDelimiter,
      maxHierarchyLevel)

    val fileEndTime = System.currentTimeMillis()
    println("Finished extracting data for ES ingestion in "+((fileEndTime-fileStartTime)/60000.0)+" minutes")

    println("Data extracted and available at " + outputDir)
  }
}
