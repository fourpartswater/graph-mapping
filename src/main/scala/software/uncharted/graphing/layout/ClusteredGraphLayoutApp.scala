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



import org.apache.spark.{SparkConf, SparkContext}
import software.uncharted.graphing.utilities.ArgumentParser



object ClusteredGraphLayoutApp {

	def main(args: Array[String]) {
		val argParser = new ArgumentParser(args)

		val sc = new SparkContext(new SparkConf().setAppName("Node Tiling"))

		val sourceDir = argParser.getStringOption("source", "The source directory where to find clustered graph data", None).get
		val outputDir = argParser.getStringOption("output", "The output location where to save data", None).get
		val partitions = argParser.getInt("parts", "The number of partitions into which to read the raw data", 0)
		val consolidationPartitions = argParser.getInt("p", "The number of partitions for data processing. Default=based on input partitions", 0)
		val dataDelimiter = argParser.getString("d", "Delimiter for the source graph data. Default is tab-delimited", "\t")
		val maxIterations = argParser.getInt("i", "Max number of iterations for force-directed algorithm", 500)
		val maxHierarchyLevel = argParser.getInt("maxLevel","Max cluster hierarchy level to use for determining graph layout", 0)
		val borderPercent = argParser.getDouble("border","Percent of parent bounding box to leave as whitespace between neighbouring communities during initial layout. Default is 2 percent", 2.0)
		val layoutLength = argParser.getDouble("layoutLength", "Desired width/height length of the total node layout region. Default = 256.0", 256.0)
		val nodeAreaPercent = argParser.getInt("nArea", "Used for Hierarchical Force-directed layout ONLY. Sets the area of all node 'circles' within the boundingBox vs whitespace.  Default is 30 percent", 30)
		val bUseEdgeWeights = argParser.getBoolean("eWeight", "Use edge weights, if present, to scale force-directed attraction forces.  Default is false", false)
		val gravity = argParser.getDouble("g", "Amount of gravitational force to use for Force-Directed layout to prevent outer nodes from spreading out too far. Default = 0 (no gravity)", 0.0)
		val isolatedDegreeThres = argParser.getInt("degreeThres", "Degree threshold used to define 'leaf communities'. Such leaf communities are automatically laid out in an outer radial/spiral pattern. Default = 0", 0)
		val communitySizeThres = argParser.getInt("commSizeThres", "Community size threshold used to exclude communities with < communitySizeThres nodes from layout. Default = 0", 0)

		val fileStartTime = System.currentTimeMillis()

		// Hierarchical Force-Directed layout scheme
		val layouter = new HierarchicFDLayout()

		layouter.determineLayout(sc,
		                         maxIterations,
		                         maxHierarchyLevel,
		                         partitions,
		                         consolidationPartitions,
		                         sourceDir,
		                         dataDelimiter,
		                         (layoutLength,layoutLength),
		                         borderPercent,
		                         nodeAreaPercent,
		                         bUseEdgeWeights,
		                         gravity,
		                         isolatedDegreeThres,
		                         communitySizeThres,
		                         outputDir)

		val fileEndTime = System.currentTimeMillis()
		println("Finished hierarchic graph layout job in "+((fileEndTime-fileStartTime)/60000.0)+" minutes")

		println("DONE!!")
	}


}
