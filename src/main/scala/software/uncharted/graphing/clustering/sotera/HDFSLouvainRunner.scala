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

package software.uncharted.graphing.clustering.sotera

import org.apache.spark.SparkContext
import org.apache.spark.graphx._

import scala.Array.canBuildFrom

/**
 * Execute the louvain algorithim and save the vertices and edges in hdfs at each level.
 * Can also save locally if in local mode.
 * 
 * See LouvainHarness for algorithm details
 * 
 * Code adapted from Sotera's graphX implementation of the distributed Louvain modularity algorithm
 * https://github.com/Sotera/spark-distributed-louvain-modularity
 */
class HDFSLouvainRunner(minProgressFactor:Double,progressCounter:Int,outputdir:String) extends LouvainHarness(minProgressFactor:Double,progressCounter:Int){

	var qValues = Array[(Int,Double)]()
	
	override def saveLevel(sc:SparkContext,level:Int,q:Double,graph:Graph[VertexState,Long]) = {
		//graph.vertices.saveAsTextFile(outputdir+"/level_"+level+"_vertices")
		//graph.edges.saveAsTextFile(outputdir+"/level_"+level+"_edges")

		// re-format results into tab-delimited strings for saving to text file
		val resultsNodes = graph.vertices.map(node =>
			{
				val (id, state) = node
				val parentId = state.community			// ID of parent community
				val internalNodes = state.internalNodes	// number of raw internal nodes in this community
				val nodeDegree = state.nodeDegree		// number of inter-community edges (unweighted)
				val extraAttributes = state.extraAttributes	// additional node attributes (labels, etc.)
					//val nodeWeight = state.nodeWeight		// weighted inter-community edges
					//val internalWeight = state.internalWeight	// weighted self-edges (ie intra-community edges)
					//val sigmaTot = state.communitySigmaTot	// analogous to modularity for this community
					
				("node\t" + id + "\t" + parentId + "\t" + internalNodes + "\t" + nodeDegree + "\t" + extraAttributes)
			}
		)

		val resultsEdges = graph.edges.map(et =>
			{
				val srcID = et.srcId
				val dstID = et.dstId
				//val srcCoords = et.srcAttr
				//val dstCoords = et.dstAttr
				//("edge\t" + srcID + "\t" + srcCoords._1 + "\t" + srcCoords._2 + "\t" + dstID + "\t" + dstCoords._1 + "\t" + dstCoords._2 + "\t" + et.attr)
				("edge\t" + srcID + "\t" + dstID + "\t" + et.attr)
			}
		)
		
		val resultsAll = resultsNodes.union(resultsEdges)	// put both node and edge results into one RDD
		
		resultsAll.saveAsTextFile(outputdir+"/level_"+level)	// save results to a separate sub-dir for each level
		
		// save the q values at each level
		qValues = qValues :+ ((level,q))
		println(s"qValue: $q")
		sc.parallelize(qValues, 1).saveAsTextFile(outputdir+"/level_"+level+"_qvalues")
	}
}
