package software.uncharted.graphing.clustering.unithread

import java.io._

import scala.collection.mutable.{Buffer, Map => MutableMap}
import scala.reflect.ClassTag

/**
 * Created by nkronenfeld on 11/10/15.
 */
class GraphEdges (val links: Array[Buffer[(Int, Float)]]) {
  var metaData: Option[Array[String]] = None

  def readMetadata (metadataInput: BufferedReader, md_filter: Option[String], separator: String, id_column: Int, md_column: Int): Unit = {
    metaData = Some(new Array[String](links.size))
    metaData.foreach{data =>
      var line = metadataInput.readLine()
      while (null != line) {
        val fields = line.split(separator)
        if (md_filter.map(filter => line.startsWith(filter)).getOrElse(true)) {
          val edgeId = fields(id_column).toInt
          if (fields.size <= md_column)
            println("Too short")
          val md = fields(md_column)
          data(edgeId) = md
        }

        line = metadataInput.readLine()
      }
    }
  }

  // Combine edges between the same nodes
  def clean (weighted: Boolean): GraphEdges = {
    val newLinks = new Array[Buffer[(Int, Float)]](links.size)

    for (i <- 0 until links.size) {
      val m = MutableMap[Int, Float]()

      for (j <- 0 until links(i).size) {
        val (dst, weight) = links(i)(j)
        m(dst) =
          if (weighted) (m.get(dst).getOrElse(0.0f) + weight)
          else (m.get(dst).getOrElse(weight))
      }

      newLinks(i) = Buffer[(Int, Float)]()
      newLinks(i).appendAll(m)
    }

    new GraphEdges(newLinks)
  }

  def renumber (weighted: Boolean): GraphEdges = {
    val linked = new Array[Boolean](links.size)
    val renum = new Array[Int](links.size)
    for (i <- 0 until links.size) {
      linked(i) = false
      renum(i) = -1
    }

    for (i <- 0 until links.size) {
      linked(i) = true
      for (j <- 0 until links(i).size) {
        linked(links(i)(j)._1) = true
      }
    }

    var nb = 0
    for (i <- 0 until links.size) {
      if (linked(i)) {
        renum(i) = nb
        nb = nb + 1
      }
    }

    val newLinks = new Array[Buffer[(Int, Float)]](nb)
    for (i <- 0 until nb) newLinks(i) = Buffer[(Int, Float)]()
    for (i <- 0 until links.size) {
      if (linked(i)) {
        val ir = renum(i)
        val nll = newLinks(ir)
        for (j <- 0 until links(i).size) {
          val lle = links(i)(j)
          nll.append((renum(lle._1), lle._2))
        }
      }
    }

    val newGE = new GraphEdges(newLinks)
    metaData.foreach{md =>
      val newMetaData = new Array[String](nb)
      for (i <- 0 until links.size)
        if (linked(i))
          newMetaData(renum(i)) = md(i)
      newGE.metaData = Some(newMetaData)
    }
    newGE
  }

  def display (weighted: Boolean): Unit = {
    for (i <- 0 until links.size; j <- 0 until links(i).size) {
      val (dest, weight) = links(i)(j)
      if (weighted) {
        println(i + " " + dest + " " + weight)
      } else {
        println(i + " " + dest)
      }
    }
  }

  def display_binary (edgeStream: DataOutputStream,
                      weightStream: Option[DataOutputStream],
                      metadataStream: Option[DataOutputStream]): Unit = {
    // output number of nodes
    val s = links.size
    edgeStream.writeInt(s)

    // output cumulative degree sequence
    var tot = 0L
    for (i <- 0 until s) {
      val degree: Int = links(i).size
      tot = tot + degree
      edgeStream.writeLong(tot)
    }

    // output links
    for (i <- 0 until s; j <- 0 until links(i).size) {
      val dest = links(i)(j)._1
      edgeStream.writeInt(dest)
    }

    // Output weights to a separate file
    weightStream.foreach{stream =>
      for (i <- 0 until s; j <- 0 until links(i).size) {
        val weight = links(i)(j)._2
        stream.writeFloat(weight)
      }
    }

    // Output metadata to yet another separate file
    metadataStream.foreach{stream =>
      metaData.foreach { data =>
        for (i <- 0 until s) {
          if (null == data(i)) stream.writeUTF("")
          else stream.writeUTF(data(i))
        }
      }
    }
  }
}


object GraphEdges {
  def apply (edge_input: BufferedReader,
             edge_filter: Option[String],
             edge_separator: String,
             source_column: Int,
             destination_column: Int,
             weight_column: Option[Int]): GraphEdges = {
    var nb_links = 0
    val edges = new GrowableArray[Buffer[(Int, Float)]](0, () => Buffer[(Int, Float)]())
    var line = edge_input.readLine()
    while (null != line) {
      val fields = line.split(edge_separator)
      if (edge_filter.map(filter => line.startsWith(filter)).getOrElse(true)) {
        val source = fields(source_column).toInt
        val destination = fields(destination_column).toInt
        val weight = weight_column.map(c => fields(c).toFloat)
        edges(source).append((destination, weight.getOrElse(1.0f)))
        if (source != destination)
          edges(destination).append((source, weight.getOrElse(1.0f)))
      }

      line = edge_input.readLine()
    }
    new GraphEdges(edges.data)
  }
}

class GrowableArray[T: ClassTag](var size: Int = 0, initialize: () => T) {
  var data = {
    val initData = new Array[T](size)
    for (i <- 0 until size) initData(i) = initialize()
    initData
  }
  private def growTo (newSize: Int): Unit = {
    if (data.size < newSize) {
      val newData = new Array[T](newSize)
      for (i <- 0 until data.size) newData(i) = data(i)
      for (i <- data.size until newSize) newData(i) = initialize()
      size = newSize
      data = newData
    }
  }
  def apply (n: Int): T = {
    growTo(n+1)
    data(n)
  }
  def update (n: Int, value: T) = {
    data(n) = value
  }
}