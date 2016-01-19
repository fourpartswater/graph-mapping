package software.uncharted.graphing.clustering.unithread.reference

import java.io._

import scala.collection.mutable.Buffer
import scala.collection.mutable.{Map => MutableMap}

/**
 * Created by nkronenfeld on 11/10/15.
 */
class GraphEdges (links: Array[Buffer[(Int, Float)]]) {
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

    new GraphEdges(newLinks)
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

  def display_binary (filename: String, filename_w: Option[String], weighted: Boolean): Unit = {
    val foutput = new DataOutputStream(new FileOutputStream(filename))

    // output number of nodes
    val s = links.size
    foutput.writeInt(s)

    // output cumulative degree sequence
    var tot = 0L
    for (i <- 0 until s) {
      val degree: Int = links(i).size
      tot = tot + degree
      foutput.writeLong(tot)
    }

    // output links
    for (i <- 0 until s; j <- 0 until links(i).size) {
      val dest = links(i)(j)._1
      foutput.writeInt(dest)
    }
    foutput.flush()
    foutput.close

    // Output weights to a separate file
    if (weighted) {
      val foutput_w = new DataOutputStream(new FileOutputStream(filename_w.get))

      for (i <- 0 until s; j <- 0 until links(i).size) {
        val weight = links(i)(j)._2
        foutput_w.writeFloat(weight)
      }

      foutput_w.flush
      foutput_w.close
    }
  }
}


object GraphEdges {
  def grow (links: Array[Buffer[(Int, Float)]], newSize: Int): Array[Buffer[(Int, Float)]] = {
    if (links.size < newSize) {
      val newLinks = new Array[Buffer[(Int, Float)]](newSize)
      for (i <- 0 until links.size) newLinks(i) = links(i)
      for (i <- links.size until newSize) newLinks(i) = Buffer[(Int, Float)]()

      newLinks
    } else {
      links
    }
  }

  def apply (filename: String, weighted: Boolean): GraphEdges = {
    val finput = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))

    var nb_links = 0
    var line = finput.readLine()
    var links = new Array[Buffer[(Int, Float)]](0)
    while (null != line) {
      val fields = line.split("[  ]+")
      val src = fields(0).toInt
      val dst = fields(1).toInt
      val weight = if (weighted) fields(2).toFloat else 1.0f

      links = grow(links, (src max dst) + 1)

      links(src).append((dst, weight))
      if (src != dst) {
        links(dst).append((src, weight))
      }

      line = finput.readLine()
    }
    finput.close()

    new GraphEdges(links)
  }
}
