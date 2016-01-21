package software.uncharted.graphing.tiling


import java.util.{List => JavaList}

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.{Buffer => MutableBuffer}

import org.json.{JSONArray, JSONObject}

import com.oculusinfo.tilegen.tiling.analytics.{AnalysisDescriptionTileWrapper, TileAnalytic}


class GraphMaxRecordAnalytic extends TileAnalytic[List[GraphAnalyticsRecord]] {
  def name: String = "maximum"

  def aggregate(a: List[GraphAnalyticsRecord], b: List[GraphAnalyticsRecord]): List[GraphAnalyticsRecord] =
    (a ++ b).reduceOption(_ max _).toList

  override def storableValue(value: List[GraphAnalyticsRecord], location: TileAnalytic.Locations.Value): Option[JSONObject] = {
    val subResult = new JSONArray()
    value.foreach(gar => subResult.put(gar.toString))
    val result = new JSONObject()
    result.put(name, subResult)
    Some(result)
  }

  def defaultProcessedValue: List[GraphAnalyticsRecord] = List[GraphAnalyticsRecord]()

  def defaultUnprocessedValue: List[GraphAnalyticsRecord] = List[GraphAnalyticsRecord]()
}

class GraphMinRecordAnalytic extends TileAnalytic[List[GraphAnalyticsRecord]] {
  def name: String = "minimum"

  def aggregate(a: List[GraphAnalyticsRecord], b: List[GraphAnalyticsRecord]): List[GraphAnalyticsRecord] =
    (a ++ b).reduceOption(_ min _).toList

  override def storableValue(value: List[GraphAnalyticsRecord], location: TileAnalytic.Locations.Value): Option[JSONObject] = {
    val subResult = new JSONArray()
    value.foreach(gar => subResult.put(gar.toString))
    val result = new JSONObject()
    result.put(name, subResult)
    Some(result)
  }

  def defaultProcessedValue: List[GraphAnalyticsRecord] = List[GraphAnalyticsRecord]()

  def defaultUnprocessedValue: List[GraphAnalyticsRecord] = List[GraphAnalyticsRecord]()
}


object GraphListAnalysis {
  val convertFcn: JavaList[GraphAnalyticsRecord] => List[GraphAnalyticsRecord] =
    a => a.asScala.toList
}

class GraphListAnalysis(analytic: TileAnalytic[List[GraphAnalyticsRecord]])
  extends AnalysisDescriptionTileWrapper[JavaList[GraphAnalyticsRecord],
    List[GraphAnalyticsRecord]](GraphListAnalysis.convertFcn, analytic) {
}


private[tiling] class GraphRecordConstructionUtilities {
  def toLimitedBuffer[T](limit: Int)(input: Iterable[T]): MutableBuffer[T] = {
    val result = new ArrayBuffer[T](limit)
    val i = input.iterator
    var n = 0
    while (n < limit && i.hasNext) {
      result += i.next()
      n = n + 1
    }
    result
  }

  // To String helper functions
  def escapeString(string: String): String = {
    if (null == string) "null"
    else "\"" + string.replace("\\", "\\\\").replace("\"", "\\\"") + "\""
  }
}


object GraphEdgeDestination {
  def fromString(parser: FromStringParser): GraphEdgeDestination = {
    parser.eatTokens("{", """"dstID"""", ":")
    val id = parser.nextLong()
    parser.eatTokens(",", """"dstCoords"""", ":", "[")
    val x = parser.nextDouble()
    parser.eatToken(",")
    val y = parser.nextDouble()
    parser.eatToken("]")
    parser.eatTokens(",", """"weight"""", ":")
    val weight = parser.nextLong()
    parser.eatToken("}")

    GraphEdgeDestination(id, (x, y), weight)
  }
}

case class GraphEdgeDestination(id: Long,
                                coordinates: (Double, Double),
                                weight: Long) {
  def min(that: GraphEdgeDestination): GraphEdgeDestination =
    GraphEdgeDestination(
      this.id min that.id,
      (this.coordinates._1 min that.coordinates._1, this.coordinates._2 min that.coordinates._2),
      this.weight min that.weight
    )

  def max(that: GraphEdgeDestination): GraphEdgeDestination =
    GraphEdgeDestination(
      this.id max that.id,
      (this.coordinates._1 max that.coordinates._1, this.coordinates._2 max that.coordinates._2),
      this.weight max that.weight
    )

  override def toString: String = {
    val (x, y) = coordinates
    s"""{"dstId": $id, "dstCoords": [$x, $y], "weight": $weight}"""
  }
}

object GraphCommunity extends GraphRecordConstructionUtilities {
  var MAX_STATS = 32
  var MAX_EDGES = 10

  // Input edges are assumed to be already sorted
  def apply(hierarchyLevel: Int, id: Long, coordinates: (Double, Double), radius: Double,
            degree: Int, nodes: Long, metadata: String, isPrimary: Boolean,
            parentId: Long, parentCoordinates: (Double, Double), parentRadius: Double,
            stats: Iterable[Double],
            interEdges: Iterable[GraphEdgeDestination],
            intraEdges: Iterable[GraphEdgeDestination]): GraphCommunity = {
    new GraphCommunity(
      hierarchyLevel, id, coordinates, radius, degree, nodes, metadata, isPrimary,
      parentId, parentCoordinates, parentRadius,
      toLimitedBuffer(MAX_STATS)(stats),
      toLimitedBuffer(MAX_EDGES)(interEdges),
      toLimitedBuffer(MAX_EDGES)(intraEdges)
    )
  }

  def fromString(parser: FromStringParser): GraphCommunity = {
    parser.eatTokens("{", """"hierLevel"""", ":")
    val hierLevel = parser.nextInt()
    parser.eatTokens(",", """"id"""", ":")
    val id = parser.nextLong()
    parser.eatTokens(",", """"coords"""", ":", "[")
    val x = parser.nextDouble()
    parser.eatToken(",")
    val y = parser.nextDouble()
    parser.eatToken("]")
    parser.eatTokens(",", """"radius"""", ":")
    val radius = parser.nextDouble()
    parser.eatTokens(",", """"degree"""", ":")
    val degree = parser.nextInt()
    parser.eatTokens(",", """"numNodes"""", ":")
    val nodes = parser.nextLong()
    parser.eatTokens(",", """"metadata"""", ":")
    val metadata = parser.nextString()
    parser.eatTokens(",", """"isPrimaryNode"""", ":")
    val isPrimary = parser.nextBoolean()
    parser.eatTokens(",", """"parentID"""", ":")
    val parentId = parser.nextLong()
    parser.eatTokens(",", """"parentCoords"""", ":", "[")
    val parentX = parser.nextDouble()
    parser.eatToken(",")
    val parentY = parser.nextDouble()
    parser.eatToken("]")
    parser.eatTokens(",", """"parentRadius"""", ":")
    val parentRadius = parser.nextDouble()
    parser.eatTokens(",", """"statsList"""", ":", "[")
    parser.eatWhitespace()

    val statsList = MutableBuffer[Double]()
    while (']' != parser.peek) {
      statsList += parser.nextDouble()
      parser.eatToken(",")
      parser.eatWhitespace()
    }
    parser.eatToken("]")

    parser.eatTokens(",", """"interEdges"""", ":", "[")
    parser.eatWhitespace()
    val interEdges = MutableBuffer[GraphEdgeDestination]()
    while (']' != parser.peek) {
      interEdges += GraphEdgeDestination.fromString(parser)
      parser.eatToken(",")
      parser.eatWhitespace()
    }
    parser.eatToken("]")

    parser.eatTokens(",", """"intraEdges"""", ":", "[")
    parser.eatWhitespace()
    val intraEdges = MutableBuffer[GraphEdgeDestination]()
    while (']' != parser.peek) {
      intraEdges += GraphEdgeDestination.fromString(parser)
      parser.eatToken(",")
      parser.eatWhitespace()
    }
    parser.eatToken("]")

    apply(
      hierLevel, id, (x, y), radius, degree, nodes, metadata, isPrimary,
      parentId, (parentX, parentY), parentRadius,
      statsList, interEdges, intraEdges
    )
  }

}


class GraphCommunity(val hierarchyLevel: Int,
                     val id: Long,
                     val coordinates: (Double, Double),
                     val radius: Double,
                     val degree: Int,
                     val nodes: Long,
                     val metadata: String,
                     val isPrimary: Boolean,
                     val parentId: Long,
                     val parentCoordinates: (Double, Double),
                     val parentRadius: Double,
                     val communityStats: MutableBuffer[Double],
                     val interEdges: MutableBuffer[GraphEdgeDestination],
                     val intraEdges: MutableBuffer[GraphEdgeDestination]) extends Serializable {

  import GraphCommunity._


  private def addEdgeInPlace(currentEdges: MutableBuffer[GraphEdgeDestination])(newEdge: GraphEdgeDestination): Unit = {
    val firstLess = currentEdges.indexWhere(edge => edge.weight < newEdge.weight)
    if (-1 != firstLess) {
      currentEdges.insert(firstLess, newEdge)
      if (currentEdges.length >= MAX_EDGES)
        currentEdges.remove(MAX_EDGES, currentEdges.length - MAX_EDGES + 1)
    } else if (currentEdges.length < MAX_EDGES) {
      currentEdges += newEdge
    }
  }

  def addInterCommunityEdge(newEdge: GraphEdgeDestination): Unit = addEdgeInPlace(interEdges)(newEdge)

  def addIntraCommunityEdge(newEdge: GraphEdgeDestination): Unit = addEdgeInPlace(intraEdges)(newEdge)

  def min(that: GraphCommunity): GraphCommunity =
    GraphCommunity(
      this.hierarchyLevel min that.hierarchyLevel,
      this.id min that.id,
      (this.coordinates._1 min that.coordinates._1, this.coordinates._2 min that.coordinates._2),
      this.radius min that.radius,
      this.degree min that.degree,
      this.nodes min that.nodes,
      "",
      false,
      this.parentId min that.parentId,
      (this.parentCoordinates._1 min that.parentCoordinates._1, this.parentCoordinates._2 min that.parentCoordinates._2),
      this.parentRadius min that.parentRadius,
      (this.communityStats.reduceOption(_ min _) ++ that.communityStats.reduceOption(_ min _)).reduceOption(_ min _),
      (this.interEdges.reduceOption(_ min _) ++ that.interEdges.reduceOption(_ min _)).reduceOption(_ min _),
      (this.intraEdges.reduceOption(_ min _) ++ that.intraEdges.reduceOption(_ min _)).reduceOption(_ min _)
    )

  def max(that: GraphCommunity): GraphCommunity =
    GraphCommunity(
      this.hierarchyLevel max that.hierarchyLevel,
      this.id max that.id,
      (this.coordinates._1 max that.coordinates._1, this.coordinates._2 max that.coordinates._2),
      this.radius max that.radius,
      this.degree max that.degree,
      this.nodes max that.nodes,
      "",
      false,
      this.parentId max that.parentId,
      (this.parentCoordinates._1 max that.parentCoordinates._1, this.parentCoordinates._2 max that.parentCoordinates._2),
      this.parentRadius max that.parentRadius,
      (this.communityStats.reduceOption(_ max _) ++ that.communityStats.reduceOption(_ max _)).reduceOption(_ max _),
      (this.interEdges.reduceOption(_ max _) ++ that.interEdges.reduceOption(_ max _)).reduceOption(_ max _),
      (this.intraEdges.reduceOption(_ max _) ++ that.intraEdges.reduceOption(_ max _)).reduceOption(_ max _)
    )


  override def equals(other: Any): Boolean =
    other match {
      case that: GraphCommunity =>
        this.id == that.id &&
          this.hierarchyLevel == that.hierarchyLevel &&
          this.coordinates == that.coordinates &&
          this.radius == that.radius &&
          this.degree == that.degree &&
          this.nodes == that.nodes &&
          this.metadata == that.metadata &&
          this.isPrimary == that.isPrimary &&
          this.parentId == that.parentId &&
          this.parentCoordinates == that.parentCoordinates &&
          this.parentRadius == that.parentRadius &&
          this.communityStats == that.communityStats &&
          this.interEdges == that.interEdges &&
          this.intraEdges == that.intraEdges
      case _ => false
    }

  override def hashCode: Int =
    id.hashCode() + 2 * (
      hierarchyLevel.hashCode() + 3 * (
        coordinates.hashCode() + 5 * (
          radius.hashCode() + 7 * (
            degree.hashCode() + 11 * (
              nodes.hashCode() + 13 * (
                metadata.hashCode + 17 * (
                  isPrimary.hashCode() + 19 * (
                    parentId.hashCode() + 23 * (
                      parentCoordinates.hashCode() + 29 * parentRadius.hashCode()
                      )
                    )
                  )
                )
              )
            )
          )
        )
      )

  override def toString: String = {
    val result = new StringBuilder
    result.append("{")
    result.append( s""""hierLevel": $hierarchyLevel, """)
    result.append( s""""id": $id, """)
    result.append( """"coords": [""").append(coordinates._1).append(", ").append(coordinates._2).append("], ")
    result.append( s""""radius": $radius, """)
    result.append( s""""degree": $degree, """)
    result.append( s""""numNodes": $nodes, """)
    result.append( """"metadata": """).append(escapeString(metadata)).append(", ")
    result.append( s""""isPrimaryNode": $isPrimary, """)
    result.append( s""""parentId": $parentId, """)
    result.append( """"parentCoords": [""").append(parentCoordinates._1).append(", ").append(parentCoordinates._2).append("], ")
    result.append( s""""parentRadius": $parentRadius""")
    result.append(communityStats.mkString( """"statsList": [""", ", ", "], "))
    result.append(interEdges.mkString( """"interEdges": [""", ", ", "], "))
    result.append(intraEdges.mkString( """"intraEdges": [""", ", ", "]"))
    result.append("}")
    result.toString()
  }
}


object GraphAnalyticsRecord extends GraphRecordConstructionUtilities {
  var MAX_COMMUNITIES = 25

  def apply(numCommunities: Int, communities: Iterable[GraphCommunity]): GraphAnalyticsRecord =
    new GraphAnalyticsRecord(numCommunities, toLimitedBuffer(MAX_COMMUNITIES)(communities))

  def fromString(value: String): GraphAnalyticsRecord = {
    val communities = MutableBuffer[GraphCommunity]()

    val parser = new FromStringParser(value)
    parser.eatToken("{")
    parser.eatToken( """"numCommunities"""")
    parser.eatToken(":")
    val numCommunities = parser.nextInt()
    parser.eatToken(",")
    parser.eatToken( """"communities"""")
    parser.eatToken(":")
    parser.eatToken("[")
    parser.eatWhitespace()
    while ('{' == parser.peek) {
      communities += GraphCommunity.fromString(parser)
      parser.eatToken(",")
      parser.eatWhitespace()
    }

    new GraphAnalyticsRecord(numCommunities, communities)
  }

  def addRecords(records: GraphAnalyticsRecord*): GraphAnalyticsRecord = {
    val newRecord = new GraphAnalyticsRecord(0, MutableBuffer[GraphCommunity]())
    records.foreach(record =>
      newRecord.add(record)
    )
    newRecord
  }
}

class GraphAnalyticsRecord(var numCommunities: Int, val communities: MutableBuffer[GraphCommunity]) extends Serializable {

  import GraphAnalyticsRecord._

  private def addCommunity(newCommunity: GraphCommunity): Unit = {
    val hierarchyLevel = newCommunity.hierarchyLevel
    if (communities.exists(oldCommunity => oldCommunity.hierarchyLevel != hierarchyLevel)) {
      throw new IllegalArgumentException("Cannot aggregate communities from different hierarchy levels.")
    }

    val firstLess =
      if (0 == hierarchyLevel) {
        // For hierarchy level 0, rank by degree
        communities.indexWhere(oldCommunity => oldCommunity.degree < newCommunity.degree)
      } else {
        // For non-0 hierarchy levels, rank by number of nodes
        communities.indexWhere(oldCommunity => oldCommunity.nodes < newCommunity.nodes)
      }

    if (-1 != firstLess) {
      communities.insert(firstLess, newCommunity)
      if (communities.length > MAX_COMMUNITIES)
        communities.remove(MAX_COMMUNITIES, communities.length - MAX_COMMUNITIES + 1)
    } else if (communities.length < MAX_COMMUNITIES) {
      communities += newCommunity
    }
  }

  def add(record: GraphAnalyticsRecord): Unit = {
    numCommunities = numCommunities + record.numCommunities
    record.communities.foreach(newCommunity =>
      addCommunity(newCommunity)
    )
  }

  def max(that: GraphAnalyticsRecord): GraphAnalyticsRecord = {
    new GraphAnalyticsRecord(
      this.numCommunities max that.numCommunities,
      toLimitedBuffer(1)((this.communities ++ that.communities).reduceOption(_ max _))
    )
  }

  def min(that: GraphAnalyticsRecord): GraphAnalyticsRecord = {
    new GraphAnalyticsRecord(
      this.numCommunities min that.numCommunities,
      toLimitedBuffer(1)((this.communities ++ that.communities).reduceOption(_ min _))
    )
  }

  override def toString: String =
    s"""{"numCommunities" : $numCommunities,""" + communities.mkString( """"communities": [""", ", ", "]}")
}


object FromStringParser {
  private val notDigit: Char => Boolean = c => c < '0' || c > '9'
  private val whitespace = Set(' ', '\t', '\n', '\r')
}

class FromStringParser(value: String) {

  import FromStringParser._

  private var index = 0

  def reset(): Unit = {
    index = 0
  }


  def peek: Char = value.charAt(index)

  def eatWhitespace(): Unit = {
    index = value.indexWhere((c: Char) => !whitespace.contains(c), index)
  }

  private def eatLiteral(token: String): Unit = {
    if (value.startsWith(token, index)) {
      index = index + token.length
    } else {
      val message = "Unexpected substring in \n" + value + "\n" + (" " * (index - 1)) + "^" + token
      throw new IllegalArgumentException(message)
    }
  }

  def eatToken(token: String, skipWhitespace: Boolean = true): Unit = {
    if (skipWhitespace) eatWhitespace()

    eatLiteral(token)
  }

  def eatTokens(tokens: String*): Unit = {
    tokens.foreach(eatToken(_))
  }

  def nextBoolean(skipWhitespace: Boolean = true): Boolean = {
    if (skipWhitespace) eatWhitespace()

    if (value.startsWith("true", index)) {
      index = index + 4
      true
    } else if (value.startsWith("false", index)) {
      index = index + 5
      false
    } else {
      throw new IllegalArgumentException("Expected boolean in \n" + value + "\n" + (" " * (index - 1)) + "^")
    }
  }

  def nextInt(skipWhitespace: Boolean = true): Int = {
    if (skipWhitespace) eatWhitespace()

    val start = index
    index = value.indexWhere(notDigit, index)
    value.substring(start, index).toInt
  }

  def nextLong(skipWhitespace: Boolean = true): Long = {
    if (skipWhitespace) eatWhitespace()

    val start = index
    index = value.indexWhere(notDigit, index)
    value.substring(start, index).toLong
  }

  def nextDouble(skipWhitespace: Boolean = true): Double = {
    if (skipWhitespace) eatWhitespace()

    val start = index
    index = value.indexWhere(notDigit, index)
    if ('.' == value.charAt(index)) {
      index = index + 1
      index = value.indexWhere(notDigit, index)
    }
    if ('E' == value.charAt(index) || 'e' == value.charAt(index)) {
      index = index + 1
      index = value.indexWhere(notDigit, index)
    }

    value.substring(start, index).toDouble
  }

  def nextString(skipWhitespace: Boolean = true): String = {
    if (skipWhitespace) eatWhitespace()

    if (value.startsWith("null", index)) null
    else if (!value.startsWith("\"", index)) throw new IllegalArgumentException("Expected quoted string didn't start with quote")
    else {
      index = index + 1
      val start = index

      while (index > -1) {
        index = value.indexOf("\"", index)
        if (index > -1) {
          val lastNonSlash = value.lastIndexWhere((c: Char) => c != '\\', index - 1)
          val slashes = (index - 1) - lastNonSlash
          if (0 == (slashes % 2)) {
            return value.substring(start, index - 1).replace("\\\"", "\"").replace("\\\\", "\\")
          }
        }
      }

      throw new IllegalArgumentException("Couldn't find the end of quoted string")
    }
  }
}

