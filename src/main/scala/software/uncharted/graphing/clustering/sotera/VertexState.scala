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

/**
 * Louvain vertex state
 * Contains all information needed for louvain community detection
 * 
 * Code adapted from Sotera's graphX implementation of the distributed Louvain modularity algorithm
 * https://github.com/Sotera/spark-distributed-louvain-modularity
 */
class VertexState extends Serializable{

	var community = -1L
	var communitySigmaTot = 0L
	var internalWeight = 0L  	// self edges
	var nodeWeight = 0L  		// out degree
	var internalNodes = 1L	// number of internal nodes (unweighted)
	var nodeDegree = 0		// out degree (unweighted)
	var extraAttributes = ""	// extra node attributes
	var changed = false
	
	override def toString(): String = {
		"community:"+community+",communitySigmaTot:"+communitySigmaTot+
		",internalWeight:"+internalWeight+
		",internalNodes:"+internalNodes+
		",nodeWeight:"+nodeWeight+
		",nodeDegree:"+nodeDegree+
		",extraAttributes:"+extraAttributes
	}
}
