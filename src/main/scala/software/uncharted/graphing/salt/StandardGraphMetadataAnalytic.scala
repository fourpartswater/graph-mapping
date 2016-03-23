package software.uncharted.graphing.salt


import org.apache.spark.sql.types.{DataType, StructType}
import software.uncharted.graphing.utilities.StringParser

import scala.collection.mutable.{Buffer => MutableBuffer}
import org.apache.spark.sql.{Row, DataFrame}
import software.uncharted.salt.core.analytic.Aggregator

import scala.util.Try


/**
  * Constructs a standard graph metadata analytic that can read data from the given dataframe schema, and extract and
  * aggregate information from a dataframe with that schema.
  *

  * @param schema The schema of the which this analytic is build to analyze.
  * @tparam VT
  * @tparam IBT
  * @tparam FBT
  */
class StandardGraphMetadataAnalytic[VT, IBT, FBT] (schema: StructType) extends MetadataAnalytic[VT, IBT, FBT, Nothing, Nothing] {
  private def getColumnInfo (columnName: String, expectedDataType: DataType) = {
    val index = schema.fieldIndex(columnName)
    val fieldType = schema.fields(index).dataType
    if (fieldType != expectedDataType) {
      throw new IllegalArgumentException(s"Field $columnName has the wrong type.  Expected $expectedDataType, got $fieldType")
    }
    (columnName, index, fieldType)
  }


  override def getValueExtractor(inputData: DataFrame): (Row) => Option[VT] = {
    row => None
  }

  override def getBinAggregator: Aggregator[VT, IBT, FBT] = {
    null
  }

  override def getTileAggregator: Option[Aggregator[FBT, Nothing, Nothing]] = None
}


object GraphRecord {
  private val maxCommunities = 25

  private[salt] def shrinkBuffer[T](buffer: MutableBuffer[T], maxSize: Int): Unit =
    while (buffer.length > maxSize) buffer.remove(maxSize)

  def fromString (string: String): GraphRecord =
    fromString(new StringParser(string))

  def fromString (parser: StringParser): GraphRecord = {
    parser.eat("{")
    parser.eat(""""numCommunities"""")
    parser.eat(":")
    val numCommunities = parser.nextInt()
    parser.eat(",")
    parser.eat(""""communities"""")
    parser.eat(":")
    val communities = parser.nextSeq(() => GraphCommunity.fromString(parser), Some("["), Some(","), Some("]"))

    GraphRecord(Some(communities.toBuffer), numCommunities)
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

  def fromString (parser: StringParser): GraphCommunity = {
    parser.eat("{")
    parser.eat(""""heirLevel"""")
    parser.eat(":")
    val heirLevel = parser.nextInt()
    parser.eat(",")
    parser.eat(""""id"""")
    parser.eat(":")
    val id = parser.nextLong()
    parser.eat(",")
    parser.eat(""""coords"""")
    parser.eat(":")
    parser.eat("[")
    val x = parser.nextDouble()
    parser.eat(",")
    val y = parser.nextDouble()
    parser.eat("]")
    parser.eat(",")
    parser.eat(""""radius"""")
    parser.eat(":")
    val radius = parser.nextDouble()
    parser.eat(",")
    parser.eat(""""degree"""")
    parser.eat(":")
    val degree = parser.nextInt()
    parser.eat(",")
    parser.eat(""""numNodes"""")
    parser.eat(":")
    val numNodes = parser.nextLong()
    parser.eat(",")
    parser.eat(""""metadata"""")
    parser.eat(":")
    val metadata = parser.nextString()
    parser.eat(",")
    parser.eat(""""isPrimaryNode"""")
    parser.eat(":")
    val isPrimaryNode = parser.nextBoolean()
    parser.eat(",")
    parser.eat(""""parentID"""")
    parser.eat(":")
    val parentId = parser.nextLong()
    parser.eat(",")
    parser.eat(""""parentCoords"""")
    parser.eat(":")
    parser.eat("[")
    val px = parser.nextDouble()
    parser.eat(",")
    val py = parser.nextDouble()
    parser.eat("]")
    parser.eat(",")
    parser.eat(""""parentRadius"""")
    parser.eat(":")
    val parentRadius = parser.nextDouble()
    parser.eat(",")
    parser.eat(""""statsList"""")
    parser.eat(":")
    val stats = parser.nextSeq(() => parser.nextDouble(), Some("["), Some(","), Some("]"))
    parser.eat(",")
    parser.eat(""""interEdges"""")
    parser.eat(":")
    val externalEdges = parser.nextSeq(() => GraphEdge.fromString(parser), Some("["), Some(","), Some("]"))
    parser.eat(",")
    parser.eat(""""intraEdges"""")
    parser.eat(":")
    val internalEdges = parser.nextSeq(() => GraphEdge.fromString(parser), Some("["), Some(","), Some("]"))
    parser.eat("}")

    GraphCommunity(heirLevel, id, (x, y), radius, degree, numNodes, metadata, isPrimaryNode,
      parentId, (px, py), parentRadius,
      Some(stats.toBuffer), Some(externalEdges.toBuffer), Some(internalEdges.toBuffer))
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
                            var externalEdges: Option[MutableBuffer[GraphEdge]] = None,
                            var internalEdges: Option[MutableBuffer[GraphEdge]] = None
                          ) {
  import GraphRecord._
  import GraphCommunity._

  communityStats.foreach(cs => shrinkBuffer(cs, maxStats))
  externalEdges.foreach(ee => shrinkBuffer(ee, maxEdges))
  internalEdges.foreach(ie => shrinkBuffer(ie, maxEdges))

  def addExternalEdge(newEdge: GraphEdge): Unit = {
    if (externalEdges.isEmpty) externalEdges = Some(MutableBuffer[GraphEdge]())
    addEdgeInPlace(externalEdges.get, newEdge)
  }

  def addInternalEdge(newEdge: GraphEdge): Unit = {
    if (internalEdges.isEmpty) internalEdges = Some(MutableBuffer[GraphEdge]())
    addEdgeInPlace(internalEdges.get, newEdge)
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
      reduceOptionalBuffers(this.externalEdges, that.externalEdges, _ min _),
      reduceOptionalBuffers(this.internalEdges, that.internalEdges, _ min _)
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
      reduceOptionalBuffers(this.externalEdges, that.externalEdges, _ max _),
      reduceOptionalBuffers(this.internalEdges, that.internalEdges, _ max _)
    )

  override def toString: String = {
    val (x, y) = coordinates
    val (px, py) = parentCoordinates
    val escapedMetaData = StringParser.escapeString(metadata)
    val statsList = communityStats.map(_.mkString("[", ",", "]")).getOrElse("[]")
    val externalEdgeList = externalEdges.map(_.mkString("[", ",", "]")).getOrElse("[]")
    val internalEdgeList = internalEdges.map(_.mkString("[", ",", "]")).getOrElse("[]")
    s"""{
       |  "heirLevel": $heirarchyLevel,
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
       |  "statsList": $statsList,
       |  "interEdges": $externalEdgeList,
       |  "intraEdges": $internalEdgeList
       |}""".stripMargin
  }
}

object GraphEdge {
  def fromString (parser: StringParser): GraphEdge = {
    parser.eat("{")
    parser.eat(""""dstId"""")
    parser.eat(":")
    val dstId = parser.nextLong()
    parser.eat(",")
    parser.eat(""""dstCoords"""")
    parser.eat(":")
    parser.eat("[")
    val x = parser.nextDouble()
    parser.eat(",")
    val y = parser.nextDouble()
    parser.eat("]")
    parser.eat(",")
    parser.eat(""""weight"""")
    parser.eat(":")
    val weight = parser.nextLong()
    parser.eat("}")
    GraphEdge(dstId, (x, y), weight)
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