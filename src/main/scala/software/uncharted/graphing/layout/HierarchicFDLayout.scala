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



import scala.util.Try
import scala.collection.JavaConverters._
import org.apache.spark.SparkContext
import org.apache.spark.rdd.RDD
import org.apache.spark.graphx._
import software.uncharted.graphing.layout.forcedirected.{ForceDirectedLayout, ForceDirectedLayoutParameters, LayoutNode}



/**
  * Hierarchical algorithm that runs force-Directed layout on each community, starting at the most inclusive level,
  * laying out each community within the area of its parent.
  **/
object HierarchicFDLayout {
  private def getGraph (sc: SparkContext, config: HierarchicalLayoutConfig)(level: Int): Graph[GraphNode, Long] = {
    // parse edge data
    val gparser = new GraphCSVParser
    val rawData = config.inputParts
      .map(p => sc.textFile(config.input + "/level_" + level, p))
      .orElse(Some(sc.textFile(config.input + "/level_" + level))).get

    val edges = gparser.parseEdgeData(rawData, config.inputDelimiter, 1, 2, 3)

    val nodeData = gparser.parseNodeData(rawData, config.inputDelimiter, 1, 2, 3, 4)

    Graph(nodeData.map(node => (node.id, node)), edges).subgraph(vpred = (id, attr) => {
      (attr != null) && (attr.internalNodes > config.communitySizeThreshold || level == 0)
    })
  }

  def getIntraCommunityEdgesByCommunity (graph: Graph[GraphNode, Long],
                                         config: HierarchicalLayoutConfig): RDD[(Long, Iterable[GraphEdge])] = {
    // find all intra-community edges and store with parent ID as map key
    val intraEdges = graph.triplets.flatMap { et =>
      val srcParentId = et.srcAttr.parentId
      val dstParentId = et.dstAttr.parentId
      if (srcParentId == dstParentId) {
        Some( (srcParentId, GraphEdge(et.srcId, et.dstId, et.attr)))
      } else {
        None
      }
    }

    config.outputParts
      .map(p => intraEdges.groupByKey(p))
      .orElse(Some(intraEdges.groupByKey()))
      .get
  }

  def getNodesByCommunity (graph: Graph[GraphNode, Long],
                           config: HierarchicalLayoutConfig): RDD[(Long, Iterable[GraphNode])] = {
    // Group nodes by community, and store with parent ID as map key
    val nodes = graph.vertices.map { case (id, node) =>
      (node.parentId, node)
    }

    config.outputParts
      .map(p => nodes.groupByKey(p))
      .orElse(Some(nodes.groupByKey()))
      .get
  }

  case class LayoutData (parentId: Long,
                         nodes: Iterable[GraphNode],
                         edges: Iterable[GraphEdge],
                         bounds: Circle)

  private def getLayoutData (graph: Graph[GraphNode, Long],
                     lastLevelLayout: RDD[(Long, Circle)],
                     config: HierarchicalLayoutConfig,
                     level: Int): RDD[LayoutData] = {
    val vCount = graph.vertices.count
    val eCount = graph.edges.count
    val nbc = getNodesByCommunity(graph, config).collect
    val icec = getIntraCommunityEdgesByCommunity(graph, config).collect
    val lll = lastLevelLayout.collect()


    // join raw nodes with intra-community edges (key is parent ID), AND join with lastLevelLayout so have access
    // to parent rectangle coords too
    getNodesByCommunity(graph, config)
      .leftOuterJoin(getIntraCommunityEdgesByCommunity(graph, config))
      .join(lastLevelLayout).map { case (parentId, ((nodes, edgesOption), bounds)) =>
      // create a dummy edge for any communities without intra-cluster edges
      // (ie for leaf communities containing only 1 node)
      // Not sure why.
      val edges = edgesOption.getOrElse(Iterable(GraphEdge(-1L, -1L, 0L)))
      LayoutData(parentId, nodes, edges, bounds)
    }
  }

  // If the graph is the top-level graph, replace parents with a common parent ID
  // The common parent ID is the id of the largest node
  //
  // @param topLevel Indicates if this graph represents the top (most zoomed-out) level or not
  // @param graph The graph at the current level
  // @return (The adjusted graph, the new parent node ID (if one was used))
  private def adjustGraph (topLevel: Boolean, graph: Graph[GraphNode, Long]): (Graph[GraphNode, Long], Option[Long]) = {
    if (topLevel) {
      // find the top node
      val rootNode = graph.vertices.map { case (id, node) => (node.id, node.internalNodes) }.top(1)(Ordering.by(_._2))(0)._1
      (
        graph.mapVertices { case (id, node) => node.replaceParent(rootNode) },
        Some(rootNode)
        )
    } else {
      (graph, None)
    }
  }

	def determineLayout(sc: SparkContext,
                      layoutConfig: HierarchicalLayoutConfig,
                      layoutParameters: ForceDirectedLayoutParameters) = {
    val levelStats = new Array[Seq[(String, AnyVal)]](layoutConfig.maxHierarchyLevel+1)	// (numNodes, numEdges, minR, maxR, minParentR, maxParentR, min Recommended Zoom Level)
    val scaleFactors = sc.collectionAccumulator[Double]("scale factors")

    def withLayout (level: Int, graphForThisLevel: Graph[LayoutNode, Long], universeWidth: Double, maxLevel: Boolean): Int = {
      val numRaw = graphForThisLevel.vertices.count
      val numNodes = graphForThisLevel.vertices.count
      val numEdges = graphForThisLevel.edges.count
      println(s"Layout done on level $level with $numRaw raw data rows, $numNodes nodes, and $numEdges edges.  Calculating layout stats")
      levelStats(level) = calcLayoutStats(level,
        graphForThisLevel.vertices.count,	// calc some overall stats about layout for this level
        graphForThisLevel.edges.count,
        graphForThisLevel.vertices.map(n => Try(n._2.geometry.radius).toOption), // Get community radii
        graphForThisLevel.vertices.map(n => Try(n._2.parentGeometry.get.radius).toOption), // Get parent radii
        layoutConfig.layoutSize,
        level == layoutConfig.maxHierarchyLevel)
      println(s"Layout stats for level $level:")
      levelStats(level).foreach(stat => println("\t"+stat._1+": "+stat._2))

      // save layout results for this hierarchical level
      println(s"Saving layout for hierarchy level $level")
      saveLayoutResults(graphForThisLevel, layoutConfig.output, level, level == layoutConfig.maxHierarchyLevel)
      println("Layout done.  Scale factors used: "+scaleFactors.value.asScala.mkString("[", ", ", "]")+"\n\n\n")

      level
    }
    determineLayout[Int](layoutConfig, layoutParameters)(getGraph(sc, layoutConfig), withLayout)

    saveLayoutStats(sc, levelStats, layoutConfig.output)	// save layout stats for all hierarchical levels
  }

  def determineLayout[T] (layoutConfig: HierarchicalLayoutConfig,
                          layoutParameters: ForceDirectedLayoutParameters)
                         (getGraphLevel: Int => Graph[GraphNode, Long],
                          withLayout: (Int, Graph[LayoutNode, Long], Double, Boolean) => T): Seq[T] = {
		//TODO -- this class assumes edge weights are Longs.  If this becomes an issue for some datasets, then change expected edge weights to Doubles?
		if (layoutConfig.maxHierarchyLevel < 0) throw new IllegalArgumentException("maxLevel parameter must be >= 0")
		if (layoutParameters.nodeAreaFactor < 0.1 || layoutParameters.nodeAreaFactor > 0.9) {
      throw new IllegalArgumentException("nodeAreaFactor parameter must be between 0.1 and 0.9")
    }

    val forceDirectedLayouter = new ForceDirectedLayout(layoutParameters)

		// init results for 'parent group' rectangle with group ID -1 (because top hierarchical communities don't
    // have valid parents).  Rectangle format is left coord, bottom coord, width, height
		var lastLevelLayoutOpt: Option[RDD[(Long, Circle)]] = None

    for (level <- layoutConfig.maxHierarchyLevel to 0 by -1) yield {
			println(s"\n\n\nStarting Force Directed Layout for hierarchy level $level\n\n\n")
      // For each hierarchical level > 0, get community ID's, community degree (num outgoing edges),
      // and num internal nodes, and the parent community ID.
      // Group by parent community, and do Group-in-Box layout once for each parent community.
      // Then consolidate results and save in format (community id, rectangle in 'global coordinates')
      println(s"\n\nGetting graph nodes and edges for hierarchy level $level\n\n")
      val (graph, rootNode) = adjustGraph((level == layoutConfig.maxHierarchyLevel), getGraphLevel(level))
      val edges = graph.edges.cache()
      val sc = edges.context

      val parentLevelLayout = lastLevelLayoutOpt.getOrElse{
        val halfSize = layoutConfig.layoutSize / 2.0
        sc.parallelize(Seq(rootNode.get -> Circle(V2(halfSize, halfSize), halfSize)))
      }

      println(s"\n\nDoing actual layout for hierarchy level $level\n\n")
			// perform force-directed layout algorithm on all nodes and edges in a given parent community
			// note: format for nodeDataAll is (id, (x, y, radius, parentID, parentX, parentY, parentR, numInternalNodes, degree, metaData))
			val nodeDataAll = getLayoutData(graph, parentLevelLayout, layoutConfig, level).flatMap { p =>
        forceDirectedLayouter.run(p.nodes, p.edges, p.parentId, p.bounds).map { node =>
          (node.id, node.inParent(p.bounds))
        }
			}.cache

			val graphForThisLevel = Graph(nodeDataAll, graph.edges)	// create a graph of the layout results for this level

      val levelResult = withLayout(level, graphForThisLevel, layoutConfig.layoutSize, level == layoutConfig.maxHierarchyLevel)

			if (level > 0) {
				val levelLayout = nodeDataAll.map { data =>
          // Just store the geometry of each parent
          (data._1, data._2.geometry)
        }.cache

        // Force the evaluation of the map!
        val levelCount = levelLayout.count()
        println(s"\n\nLevel $level has $levelCount nodes.\n\n")

        lastLevelLayoutOpt.foreach(_.unpersist(false))
        lastLevelLayoutOpt = Some(levelLayout)
			}
			nodeDataAll.unpersist(blocking=false)
			edges.unpersist(blocking=false)

      levelResult
		}
	}

	private def calcLayoutStats(level: Int,
                              numNodes: Long,
	                            numEdges: Long,
	                            radii: RDD[Option[Double]],
	                            parentRadii: RDD[Option[Double]],
	                            totalLayoutLength: Double,
	                            bMaxHierarchyLevel: Boolean): Seq[(String, AnyVal)] = {
    // A lot of the stats are used to experiment with different approaches to obtaining the mapping
    // from cluster levels to tile levels. The median calculator was causing stackoverflow exceptions on some datasets.
    val undefinedRadii = radii.filter(_.isEmpty).count()
    val goodRadii = radii.filter(_.isDefined).map(_.get)
    val radiusTotals = goodRadii.map(r => (r*r, r, 1)).reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
		val maxRadius = goodRadii.reduce(_ max _)
    val meanRadius = radiusTotals._2 / radiusTotals._3
    //val medianRadius = MedianCalculator.median(goodRadii, 5)
    val stddevRadius = radiusTotals._1 /  radiusTotals._3 - meanRadius * meanRadius
		val minRadius = goodRadii.reduce(_ min _)

    val undefinedParentRadii = parentRadii.filter(_.isEmpty).count()
    val goodParentRadii = parentRadii.filter(_.isDefined).map(_.get)
    val parentRadiusTotals = goodParentRadii.map(r => (r*r, r, 1)).reduce((a, b) => (a._1 + b._1, a._2 + b._2, a._3 + b._3))
		val maxParentRadius = goodParentRadii.reduce(_ max _)
    val meanParentRadius = parentRadiusTotals._2 / parentRadiusTotals._3
    //val medianParentRadius = MedianCalculator.median(goodParentRadii, 5)
    val stddevParentRadius = parentRadiusTotals._1 / parentRadiusTotals._3 - meanParentRadius * meanParentRadius
		val minParentRadius = goodParentRadii.reduce(_ min _)

    // Calculate the ideal zoom level at which a given number of items of the specified radius fit on each tile
    def getZoomLevel (radius: Double, numberPerTile: Double) = {
      if (bMaxHierarchyLevel) 0
      else {
        // let
        //   T = totalLayoutLength
        //   L = level
        //   R = radius
        //   N = numberPerTile
        //
        // Tile size is (T * (1/2)^L)^2 = T^2 * (1/2)^2L
        // To fit N items on a tile, each item must therefor be of size T^2 * (1/2)^2L * 1/N
        // But item size is pi*R^2
        //
        // pi * R^2 = T^2 * (1/2)^2L * 1/N
        // (1/2)^2L = pi * R^2 * N/T^2
        // L = 1/2 log_1/2 (pi * R^2 * N/T^2)
        // L = 1/2 log(pi * R^2 * N/T^2) / log(1/2)
        math.round(0.5 * math.log(math.Pi * radius * radius * numberPerTile / (totalLayoutLength * totalLayoutLength)) / math.log(0.5)).toInt
      }
    }
    def getOldZoomLevel (radius: Double) = {
      val minRecommendedZoomLevel = if (bMaxHierarchyLevel) {
        0
      } else {
        // use max parent radius to give a min recommended zoom level for this hierarchy
        // (ideally want parent radius to correspond to approx 1 tile length)
        math.round(math.log(totalLayoutLength / radius) * 1.4427).toInt // 1.4427 = 1/log(2), so equation = log2(layoutlength/radius)
      }

      minRecommendedZoomLevel
    }

    collection.IndexedSeq
    Seq(
      ("hierarchical level", level),
      ("min recommended zoom level", getOldZoomLevel(maxParentRadius)),
      ("nodes", numNodes),
      ("edges", numEdges),
      ("min radius", minRadius),
      ("zoom level by min radius", getZoomLevel(minRadius, 10)),
      ("mean radius", meanRadius),
      ("zoom level by mean radius (10)", getZoomLevel(meanRadius, 10)),
      ("zoom level by mean radius (4)", getZoomLevel(meanRadius, 4)),
      ("zoom level by mean radius (16)", getZoomLevel(meanRadius, 16)),
      ("zoom level by mean radius (64)", getZoomLevel(meanRadius, 64)),
      ("zoom level by mean radius (256)", getZoomLevel(meanRadius, 256)),
      //("median radius", medianRadius),
      //("zoom level by median radius", getZoomLevel(medianRadius, 10)),
      ("max radius", maxRadius),
      ("zoom level by max radius", getZoomLevel(maxRadius, 10)),
      ("std dev radius", stddevRadius),
      ("min parent radius", minParentRadius),
      ("zoom level by min parent radius", getZoomLevel(minParentRadius, 1)),
      ("mean parent radius", meanParentRadius),
      ("zoom level by mean parent radius", getZoomLevel(meanParentRadius, 1)),
      //("median parent radius", medianParentRadius),
      //("zoom level by median parent radius", getZoomLevel(medianParentRadius, 10)),
      ("max parent radius", maxParentRadius),
      ("zoom level by max parent radius", getZoomLevel(maxParentRadius, 1)),
      ("std dev parent radius", stddevParentRadius)
    )
	}

	private def saveLayoutResults(graphWithCoords: Graph[forcedirected.LayoutNode, Long],
	                              outputDir: String,
	                              level: Int, bIsMaxLevel: Boolean) =	{

		// re-format results into tab-delimited strings for saving to text file
		val resultsNodes = graphWithCoords.vertices.flatMap{vertex =>
      Try{
        val (id, node) = vertex

        List("node",
          id, node.geometry.center.x, node.geometry.center.y, node.geometry.radius,
          node.parentId, node.parentGeometry.get.center.x, node.parentGeometry.get.center.y, node.parentGeometry.get.radius,
          node.internalNodes, node.degree, level, node.metadata
        ).mkString("\t")
      }.toOption
    }

		val resultsEdges = graphWithCoords.triplets.flatMap { et =>
      Try {
        val srcID = et.srcId
        val dstID = et.dstId
        val srcGeometry = et.srcAttr.geometry
        val dstGeometry = et.dstAttr.geometry
        // is this an inter-community edge (same parentID for src and dst)
        val interCommunityEdge = if ((et.srcAttr.parentId != et.dstAttr.parentId) || bIsMaxLevel) 1 else 0

        List("edge",
          srcID, srcGeometry.center.x, srcGeometry.center.y,
          dstID, dstGeometry.center.x, dstGeometry.center.y,
          et.attr, interCommunityEdge
        ).mkString("\t")
      }.toOption
    }.filter(line => line != null)

		val resultsAll = resultsNodes.union(resultsEdges)	// put both node and edge results into one RDD

		resultsAll.saveAsTextFile(outputDir+"/level_"+level)	// save results to outputDir + "level_#"
	}


	private def saveLayoutStats(sc: SparkContext, stats: Array[Seq[(String, AnyVal)]], outputDir: String) = {

		// re-format results into strings for saving to text file
		var level = stats.length - 1
		val statsStrings = new Array[(String)](stats.length)
		while (level >= 0) {
      val levelStats = stats(level)

			statsStrings(level) = levelStats.map(a => a._1+": "+a._2).mkString(", ")

			level -= 1
		}

		sc.parallelize(statsStrings, 1).saveAsTextFile(outputDir+"/stats")
	}
}
