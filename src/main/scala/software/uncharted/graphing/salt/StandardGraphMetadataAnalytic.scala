package software.uncharted.graphing.salt



import software.uncharted.graphing.utilities.{JSONParserUtils, StringParser}

import scala.collection.mutable.{Buffer => MutableBuffer}
import org.apache.spark.sql.{Row, DataFrame}
import software.uncharted.salt.core.analytic.Aggregator

import scala.util.parsing.json.JSON


/**
  * Constructs a standard graph metadata analytic that can read data from the given dataframe schema, and extract and
  * aggregate information from a dataframe with that schema.
  *
  * @tparam VT Value type
  * @tparam IBT Internal bin type
  * @tparam FBT external bin type
  */
class StandardGraphMetadataAnalytic[VT, IBT, FBT]  extends MetadataAnalytic[VT, IBT, FBT, Nothing, Nothing] {
  override def getValueExtractor(inputData: DataFrame): (Row) => Option[VT] = {
    row => None
  }

  override def getBinAggregator: Aggregator[VT, IBT, FBT] = {
    null
  }

  override def getTileAggregator: Option[Aggregator[FBT, Nothing, Nothing]] = None
}


object GraphRecord {
  val maxCommunities = 25

  private[salt] def shrinkBuffer[T](buffer: MutableBuffer[T], maxSize: Int): Unit =
    while (buffer.length > maxSize) buffer.remove(maxSize)

  def fromString(string: String): GraphRecord = {
    import JSONParserUtils._

    JSON.parseFull(string).map(_ match {
      case m: Map[String, Any] =>
        val numCommunities = getInt(m, "numCommunities").get
        val communities = getSeq(m, "communities", a =>
          GraphCommunity.fromJSON(a.asInstanceOf[Map[String, Any]])
        )
        GraphRecord(communities, numCommunities)
    }).get
  }
}
case class GraphRecord (communities: Option[Seq[GraphCommunity]], numCommunities: Int) {
  override def toString: String = {
    val communityList = communities.map(_.mkString("[", ",", "]")).getOrElse("[]")
    s"""{
       |  "numCommunities": $numCommunities,
       |  "communities": $communityList
       |}""".stripMargin
  }
}



object GraphCommunity {
  import GraphRecord._

  var maxStats = 32
  var maxEdges = 10

  def addEdgeInPlace(accumulatedEdges: MutableBuffer[GraphEdge], newEdge: GraphEdge): Unit = {
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

  private def reduceOptionalBuffers[T](a: Option[Seq[T]],
                                       b: Option[Seq[T]],
                                       reduceFcn: (T, T) => T): Option[Seq[T]] = {
    (a.map(_.reduce(reduceFcn)) ++ b.map(_.reduce(reduceFcn)))
      .reduceLeftOption(reduceFcn)
      .map(t => Seq(t))
  }

  def fromJSON (json: Map[String, Any]): GraphCommunity = {
    import JSONParserUtils._
    def tupleFromSeq[T] (s: Seq[T], offset: Int = 0): (T, T) = (s(offset), s(offset+1))

    val hierarchyLevel = getInt(json, "hierLevel").get
    val id = getLong(json, "id").get
    val coordinates = tupleFromSeq(getSeq(json, "coords", toDouble).get)
    val radius = getDouble(json, "radius").get
    val degree = getInt(json, "degree").get
    val numNodes = getLong(json, "numNodes").get
    val metadata = getString(json, "metadata").get
    val isPrimaryNode = getBoolean(json, "isPrimaryNode").get
    val parentId = getLong(json, "parentID").get
    val parentCoordinates = tupleFromSeq(getSeq(json, "parentCoords", toDouble).get)
    val parentRadius = getDouble(json, "parentRadius").get
    val externalEdges = getSeq(json, "interEdges", a => GraphEdge.fromJSON(a.asInstanceOf[Map[String, Any]]))
    val internalEdges = getSeq(json, "intraEdges", a => GraphEdge.fromJSON(a.asInstanceOf[Map[String, Any]]))

    GraphCommunity(hierarchyLevel, id, coordinates, radius, degree, numNodes, metadata, isPrimaryNode,
      parentId, parentCoordinates, parentRadius, externalEdges, internalEdges)
  }
}

case class GraphCommunity (
                            hierarchyLevel: Int,
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
                            externalEdges: Option[Seq[GraphEdge]] = None,
                            internalEdges: Option[Seq[GraphEdge]] = None
                          ) {
  import GraphCommunity._

  def min(that: GraphCommunity): GraphCommunity =
    GraphCommunity(
      this.hierarchyLevel min that.hierarchyLevel,
      this.id min that.id,
      minPair(this.coordinates, that.coordinates),
      this.radius min that.radius,
      this.degree min that.degree,
      this.numNodes min that.numNodes,
      "",
      isPrimaryNode = false,
      this.parentId min that.parentId,
      minPair(this.parentCoordinates, that.parentCoordinates),
      this.parentRadius min that.parentRadius,
      reduceOptionalBuffers[GraphEdge](this.externalEdges, that.externalEdges, _ min _),
      reduceOptionalBuffers[GraphEdge](this.internalEdges, that.internalEdges, _ min _)
    )

  def max(that: GraphCommunity): GraphCommunity =
    GraphCommunity(
      this.hierarchyLevel max that.hierarchyLevel,
      this.id max that.id,
      maxPair(this.coordinates, that.coordinates),
      this.radius max that.radius,
      this.degree max that.degree,
      this.numNodes max that.numNodes,
      "",
      isPrimaryNode = false,
      this.parentId max that.parentId,
      maxPair(this.parentCoordinates, that.parentCoordinates),
      this.parentRadius max that.parentRadius,
      reduceOptionalBuffers[GraphEdge](this.externalEdges, that.externalEdges, _ max _),
      reduceOptionalBuffers[GraphEdge](this.internalEdges, that.internalEdges, _ max _)
    )

  override def toString: String = {
    val (x, y) = coordinates
    val (px, py) = parentCoordinates
    val escapedMetaData = StringParser.escapeString(metadata)
    val externalEdgeList = externalEdges.map(_.mkString("[", ",", "]")).getOrElse("[]")
    val internalEdgeList = internalEdges.map(_.mkString("[", ",", "]")).getOrElse("[]")
    s"""{
       |  "hierLevel": $hierarchyLevel,
       |  "id": $id,
       |  "coords": [$x, $y],
       |  "radius": $radius,
       |  "degree": $degree,
       |  "numNodes": $numNodes,
       |  "metadata": $escapedMetaData,
       |  "isPrimaryNode": $isPrimaryNode,
       |  "parentID": $parentId,
       |  "parentCoords": [$px, $py],
       |  "parentRadius": $parentRadius,
       |  "interEdges": $externalEdgeList,
       |  "intraEdges": $internalEdgeList
       |}""".stripMargin
  }
}

object GraphEdge {
  def fromJSON (json: Map[String, Any]): GraphEdge = {
    import JSONParserUtils._
    def tupleFromSeq[T] (s: Seq[T], offset: Int = 0): (T, T) = (s(offset), s(offset+1))

    val dstId = getLong(json, "dstId").get
    val dstCoordinates = tupleFromSeq(getSeq(json, "dstCoords", toDouble).get)
    val weight = getLong(json, "weight").get

    GraphEdge(dstId, dstCoordinates, weight)
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