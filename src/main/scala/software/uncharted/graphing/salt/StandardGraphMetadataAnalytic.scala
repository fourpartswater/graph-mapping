package software.uncharted.graphing.salt



import scala.collection.mutable.{Buffer => MutableBuffer}
import org.apache.spark.sql.{Row, DataFrame}
import software.uncharted.salt.core.analytic.Aggregator




//class StandardGraphMetadataAnalytic[VT, IBT, FBT] extends MetadataAnalytic[VT, IBT, FBT, Nothing, Nothing] {
//  override def getValueExtractor(inputData: DataFrame): (Row) => Option[VT] = {
//    None
//  }
//
//  override def getBinAggregator: Aggregator[VT, IBT, FBT] = {
//    null
//  }
//
//  override def getTileAggregator: Option[Aggregator[FBT, Nothing, Nothing]] = None
//}


object GraphRecord {
  private val maxCommunities = 25

  private[salt] def shrinkBuffer[T](buffer: MutableBuffer[T], maxSize: Int): Unit =
    while (buffer.length > maxSize) buffer.remove(maxSize)

  private[salt] def escapeString (string: String): String = {
    if (null == string) "null"
    else "\"" + string.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
  }

  private[salt] def unescapeString (string: String): String = {
    if (null == string) null
    else if ("null" == string) null
    else {
      // remove start and end quote, and replace escape characters
      string.substring(1, string.length - 1).replace("\\\"", "\"").replace("\\\\", "\\")
    }
  }
}
case class GraphRecord (communities: Option[MutableBuffer[GraphCommunity]], numCommunities: Int) {
  import GraphRecord._
  communities.map(c => shrinkBuffer(c, maxCommunities))

  override def toString: String = {
    val communityList = communities.map(_.mkString("[", ",", "]")).getOrElse("[]")
    s"""{
       |  "numCommunities": $numCommunities,
       |  "communities": $communityList
       |}""".stripMargin
  }

  // TODO: fromString
}

object GraphCommunity {
  import GraphRecord._

  private var maxStats = 32
  private var maxEdges = 10

  def setMaxStats(newMax: Int): Unit = {
    maxStats = newMax
  }

  def setMaxEdges(newMax: Int): Unit = {
    maxEdges = newMax
  }

  private def addEdgeInPlace(accumulatedEdges: MutableBuffer[GraphEdge], newEdge: GraphEdge): Unit = {
    val insertionIndex = accumulatedEdges.indexWhere(_.weight < newEdge.weight)
    if (-1 == insertionIndex) {
      // Add to end, if there is room
      if (accumulatedEdges.length < maxEdges)
        accumulatedEdges += newEdge
    } else {
      accumulatedEdges.insert(insertionIndex, newEdge)
    }
    shrinkBuffer(accumulatedEdges, maxEdges)
  }

  private[salt] def minPair(a: (Double, Double), b: (Double, Double)): (Double, Double) =
    (a._1 min b._1, a._2 min b._2)

  private[salt] def maxPair(a: (Double, Double), b: (Double, Double)): (Double, Double) =
    (a._1 max b._1, a._2 max b._2)

  private def reduceOptionalBuffers[T](a: Option[MutableBuffer[T]],
                                       b: Option[MutableBuffer[T]],
                                       reduceFcn: (T, T) => T): Option[MutableBuffer[T]] = {
    (a.map(_.reduce(reduceFcn)) ++ b.map(_.reduce(reduceFcn)))
      .reduceLeftOption(reduceFcn)
      .map(t => MutableBuffer(t))
  }
}

case class GraphCommunity (
                            heirarchyLevel: Int,
                            id: Long,
                            coordinates: (Double, Double),
                            radius: Double,
                            degree: Int,
                            numNodes: Long,
                            metadata: String,
                            isPrimaryNode: Boolean,
                            parentId: Long,
                            parentCoordinates: (Double, Double),
                            parentRadius: Double,
                            communityStats: Option[MutableBuffer[Double]] = None,
                            var internalEdges: Option[MutableBuffer[GraphEdge]] = None,
                            var externalEdges: Option[MutableBuffer[GraphEdge]] = None
                          ) {
  import GraphRecord._
  import GraphCommunity._

  communityStats.map(cs => shrinkBuffer(cs, maxStats))
  internalEdges.map(ie => shrinkBuffer(ie, maxEdges))
  externalEdges.map(ee => shrinkBuffer(ee, maxEdges))

  def addInternalEdge(newEdge: GraphEdge): Unit = {
    if (internalEdges.isEmpty) internalEdges = Some(MutableBuffer[GraphEdge]())
    addEdgeInPlace(internalEdges.get, newEdge)
  }

  def addExternalEdge(newEdge: GraphEdge): Unit = {
    if (externalEdges.isEmpty) externalEdges = Some(MutableBuffer[GraphEdge]())
    addEdgeInPlace(externalEdges.get, newEdge)
  }

  def min(that: GraphCommunity): GraphCommunity =
    GraphCommunity(
      this.heirarchyLevel min that.heirarchyLevel,
      this.id min that.id,
      minPair(this.coordinates, that.coordinates),
      this.radius min that.radius,
      this.degree min that.degree,
      this.numNodes min that.numNodes,
      "",
      false,
      this.parentId min that.parentId,
      minPair(this.parentCoordinates, that.parentCoordinates),
      this.parentRadius min that.parentRadius,
      reduceOptionalBuffers(this.communityStats, that.communityStats, _ min _),
      reduceOptionalBuffers(this.internalEdges, that.internalEdges, _ min _),
      reduceOptionalBuffers(this.externalEdges, that.externalEdges, _ min _)
    )

  def max(that: GraphCommunity): GraphCommunity =
    GraphCommunity(
      this.heirarchyLevel max that.heirarchyLevel,
      this.id max that.id,
      maxPair(this.coordinates, that.coordinates),
      this.radius max that.radius,
      this.degree max that.degree,
      this.numNodes max that.numNodes,
      "",
      false,
      this.parentId max that.parentId,
      maxPair(this.parentCoordinates, that.parentCoordinates),
      this.parentRadius max that.parentRadius,
      reduceOptionalBuffers(this.communityStats, that.communityStats, _ max _),
      reduceOptionalBuffers(this.internalEdges, that.internalEdges, _ max _),
      reduceOptionalBuffers(this.externalEdges, that.externalEdges, _ max _)
    )

  override def toString: String = {
    val (x, y) = coordinates
    val (px, py) = parentCoordinates
    val escapedMetaData = escapeString(metadata)
    val statsList = communityStats.map(_.mkString("[", ",", "]")).getOrElse("[]")
    val internalEdgeList = internalEdges.map(_.mkString("[", ",", "]")).getOrElse("[]")
    val externalEdgeList = externalEdges.map(_.mkString("[", ",", "]")).getOrElse("[]")
    s"""{
       |  "hierLevel": $heirarchyLevel,
       |  "id": $id,
       |  "coords": [$x, $y],
       |  "radius": $radius,
       |  "degree": $degree,
       |  "numNodes": $numNodes,
       |  "metadata": $escapedMetaData,
       |  "isPrimaryNode": $isPrimaryNode,
       |  "parentId": $parentId,
       |  "parentCoords": [$px, $py],
       |  "parentRadius": $parentRadius,
       |  "statsList": $statsList,
       |  "interEdges": $internalEdgeList,
       |  "intraEdges": $externalEdgeList
       |}""".stripMargin
  }
}

case class GraphEdge (destinationId: Long,
                      destinationCoordinates: (Double, Double),
                      weight: Long) {
  def min(that: GraphEdge): GraphEdge =
    GraphEdge(this.destinationId min that.destinationId,
      GraphCommunity.minPair(this.destinationCoordinates, that.destinationCoordinates),
      this.weight min that.weight)

  def max(that: GraphEdge): GraphEdge =
    GraphEdge(this.destinationId max that.destinationId,
      GraphCommunity.maxPair(this.destinationCoordinates, that.destinationCoordinates),
      this.weight max that.weight)

  override def toString: String = {
    val (x, y) = destinationCoordinates
    s"""{"dstId": $destinationId, "dstCoords": [$x, $y], "weight": $weight}"""
  }
}