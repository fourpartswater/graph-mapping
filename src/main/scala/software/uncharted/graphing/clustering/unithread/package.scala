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
package software.uncharted.graphing.clustering

/**
  * This package contains a hierarchical clustering algorithm based on the Louvain Clustering algorithm proposed by
  * Blondel, Guillaume, and Lambiotte, at at https://sites.google.com/site/findcommunities.
  *
  * It is modified in a number of ways:
  *
  * <ul>
  *   <li> It is translated into Scala, and altered slightly for readability </li>
  *   <li> It can interact with Spark-based GraphX objects (though not in a distributed manner - the information still
  *        must be collected to a single access point </li>
  *   <li> It preservers more information from the original graph throughout the process </li>
  *   <li> It allows for various further modifications to the baseline algorithm (through the algorithm modification
  *        input to the Community clustering object) </li>
  *   <li> It can be used as part of a larger process, rather than only as a separate application </li>
  * </ul>
  */
package object unithread {
}
