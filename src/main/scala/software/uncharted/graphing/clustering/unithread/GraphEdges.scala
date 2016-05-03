package software.uncharted.graphing.clustering.unithread



import java.io._
import java.util.Date

import software.uncharted.graphing.analytics.CustomGraphAnalytic

import scala.collection.mutable.{Buffer => MutableBuffer}
import scala.reflect.ClassTag



class GraphEdges (val links: Array[MutableBuffer[(Int, Float, Seq[String])]]) {
  var metaData: Option[Array[(String, Seq[String])]] = None

  def readMetadata (metadataInput: BufferedReader, md_filter: Option[String], separator: String,
                    id_column: Int, md_column: Int, analytics: Seq[CustomGraphAnalytic[_, _]]): Unit = {
    metaData = Some(new Array[(String, Seq[String])](links.length))
    val analyticColumns = CustomGraphAnalytic.determineColumns(analytics)
    metaData.foreach{data =>
      var line = metadataInput.readLine()
      while (null != line) {
        val fields = line.split(separator)
        if (md_filter.map(filter => line.startsWith(filter)).getOrElse(true)) {
          val edgeId = fields(id_column).toInt
          if (fields.size <= md_column) println("Too short")
          val md = fields(md_column)
          val analyticValues = analyticColumns.map { c =>
            if (fields.size <= c) ""
            else fields(c).trim
          }
          data(edgeId) = (md, analyticValues)
        }

        line = metadataInput.readLine()
      }
    }
  }

  def renumber (weighted: Boolean): GraphEdges = {
    val linked = new Array[Boolean](links.length)
    val renum = new Array[Int](links.length)
    for (i <- links.indices) {
      linked(i) = false
      renum(i) = -1
    }

    for (i <- links.indices) {
      linked(i) = true
      for (j <- links(i).indices) {
        linked(links(i)(j)._1) = true
      }
    }

    var nb = 0
    for (i <- links.indices) {
      if (linked(i)) {
        renum(i) = nb
        nb = nb + 1
      }
    }

    val newLinks = new Array[MutableBuffer[(Int, Float, Seq[String])]](nb)
    for (i <- 0 until nb) newLinks(i) = MutableBuffer[(Int, Float, Seq[String])]()
    for (i <- links.indices) {
      if (linked(i)) {
        val ir = renum(i)
        val nll = newLinks(ir)
        for (j <- links(i).indices) {
          val lle = links(i)(j)
          nll.append((renum(lle._1), lle._2, lle._3))
        }
      }
    }

    val newGE = new GraphEdges(newLinks)
    metaData.foreach{md =>
      val newMetaData = new Array[(String, Seq[String])](nb)
      for (i <- links.indices)
        if (linked(i))
          newMetaData(renum(i)) = md(i)
      newGE.metaData = Some(newMetaData)
    }
    newGE
  }

  def display (weighted: Boolean): Unit = {
    for (i <- links.indices; j <- links(i).indices) {
      val (dest, weight, analyticValues) = links(i)(j)
      if (weighted) {
        println(i + " " + dest + " " + weight + analyticValues.mkString(" ", " ", ""))
      } else {
        println(i + " " + dest + analyticValues.mkString(" ", " ", ""))
      }
    }
  }

  def display_binary (edgeStream: DataOutputStream,
                      weightStream: Option[DataOutputStream],
                      metadataStream: Option[DataOutputStream]): Unit = {
    // output number of nodes
    val s = links.length
    edgeStream.writeInt(s)

    // output cumulative degree sequence
    var tot = 0L
    for (i <- 0 until s) {
      val degree: Int = links(i).size
      tot = tot + degree
      edgeStream.writeLong(tot)
    }

    // output links
    for (i <- 0 until s; j <- links(i).indices) {
      val dest = links(i)(j)._1
      edgeStream.writeInt(dest)
    }

    // Output weights to a separate file
    weightStream.foreach{stream =>
      for (i <- 0 until s; j <- links(i).indices) {
        val weight = links(i)(j)._2
        stream.writeFloat(weight)
      }
    }

    // Output metadata to yet another separate file
    metadataStream.foreach{stream =>
      metaData.foreach { data =>
        for (i <- 0 until s) {
          if (null == data(i)) {
            stream.writeUTF("")
            stream.writeInt(0)
          } else {
            val di = data(i)
            stream.writeUTF(di._1)
            stream.writeInt(di._2.length)
            di._2.foreach(dias => stream.writeUTF(dias))
          }
        }
      }
    }
  }
}


object GraphEdges {
  def apply (edgeInputFile: String,
             edge_filter: Option[String],
             edge_separator: String,
             source_column: Int,
             destination_column: Int,
             weight_column: Option[Int],
             analytics: Seq[CustomGraphAnalytic[_, _]]): GraphEdges = {
    // First read through the file once, counting edges
    println("Counting nodes in graph")
    val countReader = new BufferedReader(new InputStreamReader(new FileInputStream(edgeInputFile)))
    var maxNode = 0
    var line = countReader.readLine()
    var n = 0
    while (null != line) {
      val fields = line.split(edge_separator)
      if (edge_filter.map(filter => line.startsWith(filter)).getOrElse(true)) {
        val source = fields(source_column).toInt
        val destination = fields(destination_column).toInt
        maxNode = maxNode max source max destination
      }

      line = countReader.readLine()
      n += 1
      if (0 == (n % 100000))
        println("Counted " + n + " ("+new Date()+")")
    }
    countReader.close()
    println("Reading graph with "+(maxNode+1)+" nodes")

    // Now actually read the graph
    val graphReader = new BufferedReader(new InputStreamReader(new FileInputStream(edgeInputFile)))
    val result = apply(graphReader, edge_filter, edge_separator, source_column, destination_column, weight_column, Some(maxNode+1))
    graphReader.close()
    result
  }

  def apply (edge_input: BufferedReader,
             edge_filter: Option[String],
             edge_separator: String,
             source_column: Int,
             destination_column: Int,
             weight_column: Option[Int],
             initialSize: Option[Int] = None,
             analytics: Seq[CustomGraphAnalytic[_, _]] = Seq()): GraphEdges = {
    val edges = new GrowableArray[MutableBuffer[(Int, Float, Seq[String])]](initialSize.getOrElse(0), () => MutableBuffer[(Int, Float, Seq[String])]())
    var line = edge_input.readLine()
    var n = 0
    val analyticColumns = CustomGraphAnalytic.determineColumns(analytics)
    while (null != line) {
      val fields = line.split(edge_separator)
      if (edge_filter.map(filter => line.startsWith(filter)).getOrElse(true)) {
        val source = fields(source_column).toInt
        val destination = fields(destination_column).toInt
        val weight = weight_column.map(c => fields(c).toFloat)
        val analyticValues = analyticColumns.map(c => fields(c))

        edges(source).append((destination, weight.getOrElse(1.0f), analyticValues))
        if (source != destination)
          edges(destination).append((source, weight.getOrElse(1.0f), analyticValues))
      }

      line = edge_input.readLine()
      n += 1
      if (0 == (n % 100000))
        println("Read " + n + " ("+new Date()+")")
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
    if (data.length < newSize) {
      println("Growing from "+data.length+" to "+newSize)
      val newData = new Array[T](newSize)
      for (i <- data.indices) newData(i) = data(i)
      for (i <- data.length until newSize) newData(i) = initialize()
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