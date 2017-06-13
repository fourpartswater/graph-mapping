//scalastyle:off
package software.uncharted.graphing.clustering.unithread.reference


import java.io.{InputStreamReader, BufferedReader, FileInputStream}
import java.util.Date
import scala.collection.mutable.{Buffer, Map => MutableMap}
import scala.util.Random



/**
 *
 * @param g The graph to cluster
 * @param nb_pass number of pass for one level computation
 *                if -1, compute as many pass as needed to increas modularity
 * @param min_modularity a new pass is computed if the last one has generated an increase greater than min_modularity
 *                       if 0, even a minor increase is enough to go for one more pass
 */
class Community (val g: Graph, nb_pass: Int, min_modularity: Double) {

  def this (filename: String,  filename_w: Option[String], weighted: Boolean, nbp: Int, minm: Double) =
    this(Graph(filename, filename_w, weighted), nbp, minm)




  val size = g.nb_nodes
  val n2c = (0 until size).toArray
  val tot = n2c.map(n => g.weighted_degree(n))
  val in = n2c.map(n => g.nb_selfloops(n))
  val neigh_weight = n2c.map(n => -1.0)
  val neigh_pos = n2c.map(n => 0)
  var neigh_last: Int = 0


  def remove(node: Int, comm: Int, dnodecomm: Double): Unit = {
    tot(comm) = tot(comm) - g.weighted_degree(node)
    in(comm) = in(comm) - (2 * dnodecomm + g.nb_selfloops(node))
    n2c(node) = -1
  }

  def insert(node: Int, comm: Int, dnodecomm: Double): Unit = {
    tot(comm) = tot(comm) + g.weighted_degree(node)
    in(comm) = in(comm) + (2 * dnodecomm + g.nb_selfloops(node))
    n2c(node) = comm
  }

  def modularity_gain(node: Int, comm: Int, dnodecomm: Double, w_degree: Double): Double = {
    val totc = tot(comm)
    val degc = w_degree
    val m2 = g.total_weight
    val dnc = dnodecomm

    (dnc - totc * degc / m2)
  }

  def modularity: Double = {
    var q = 0.0
    val m2 = g.total_weight

    for (i <- 0 until size) {
      if (tot(i) > 0) {
        val tm = tot(i) / m2
        q += in(i) / m2 - tm * tm
      }
    }

    q
  }

  def neigh_comm(node: Int): Unit = {
    for (i <- 0 until neigh_last) {
      neigh_weight(neigh_pos(i)) = -1
    }
    val degree = g.nb_neighbors(node)
    neigh_pos(0) = n2c(node)
    neigh_weight(neigh_pos(0)) = 0
    neigh_last = 1
    val neighbors = g.neighbors(node)
    for (i <- 0 until degree) {
      val (neighbor, neighbor_weight) = neighbors.next
      val neighbor_community = n2c(neighbor)
      if (neighbor != node) {
        if (neigh_weight(neighbor_community) == -1.0) {
          neigh_weight(neighbor_community) = 0.0
          neigh_pos(neigh_last) = neighbor_community
          neigh_last += 1
        }
        neigh_weight(neighbor_community) += neighbor_weight
      }
    }
  }

  def one_level (randomize: Boolean = true): Boolean = {
    var improvement = false
    var nb_moves: Int = 0
    var nb_pass_done: Int = 0
    var new_mod = modularity
    var cur_mod = new_mod

    // Randomize the order of vertex inspection
    val random_order = (0 until size).toArray
    val randomizer = new Random()
    if (randomize) {
      for (i <- 0 until (size - 1)) {
        val rand_pos = randomizer.nextInt(size - i) + i
        val tmp = random_order(i)
        random_order(i) = random_order(rand_pos)
        random_order(rand_pos) = tmp
      }
    }

    // repeat while
    //   there is an improvement of modularity
    //   or there is an improvement of modularity greater than a given minimumModularityGain
    //   or a predefined number of pass have been done
    do {
      cur_mod = new_mod
      nb_moves = 0
      nb_pass_done += 1

      // for each node: remove the node from its community and insert it in the best community
      for (node_tmp <- 0 until size) {
        val node = random_order(node_tmp)
        val node_comm = n2c(node)
        val w_degree = g.weighted_degree(node)

        // computation of all neighboring communities of current node
        neigh_comm(node)
        // remove node from its current community
        remove(node, node_comm, neigh_weight(node_comm))

        // compute the nearest community for node
        // default choice for future insertion is the former community
        var best_comm = node_comm
        var best_nblinks = 0.0
        var best_increase = 0.0
        for (i <- 0 until neigh_last) {
          val increase = modularity_gain(node, neigh_pos(i), neigh_weight(neigh_pos(i)), w_degree)
          if (increase > best_increase) {
            best_comm = neigh_pos(i)
            best_nblinks = neigh_weight(neigh_pos(i))
            best_increase = increase
          }
        }

        // insert node in the nearest community
        insert(node, best_comm, best_nblinks)

        if (best_comm != node_comm)
          nb_moves += 1
      }

      var total_tot = 0.0
      var total_in = 0.0
      for (i <- 0 until tot.size) {
        total_tot += tot(i)
        total_in += in(i)
      }

      new_mod = modularity
      if (nb_moves > 0)
        improvement = true;

    } while (nb_moves > 0 && new_mod - cur_mod > min_modularity)

    improvement
  }

  private def getRenumbering: (Array[Int], Int) = {
    val renumber = (0 until size).map(n => -1).toArray
    for (node <- 0 until size)
      renumber(n2c(node)) += 1

    var last = 0
    for (i <- 0 until size) {
      if (renumber(i) != -1) {
        renumber(i) = last
        last += 1
      }
    }

    (renumber, last)
  }

  def init_partition (filename: String): Unit = {
    val finput = new BufferedReader(new InputStreamReader(new FileInputStream(filename)))

    var line = finput.readLine()
    while (null != line) {
      val fields = line.split("[ \t]+")
      val node = fields(0).toInt
      val comm = fields(1).toInt

      val old_comm = n2c(node)
      neigh_comm(node)
      remove(node, old_comm, neigh_weight(old_comm))

      var i = 0
      var done = false
      while (i < neigh_last && !done) {
        val best_comm = neigh_pos(i)
        val best_nblinks = neigh_weight(best_comm)
        if (best_comm == comm) {
          insert(node, best_comm, best_nblinks)
          done = true
        } else {
          i = i + 1
        }
      }

      if (!done)
        insert(node, comm, 0)

      line = finput.readLine()
    }
    finput.close
  }

  def display_partition: Unit = {
    val (renumber, last) = getRenumbering
    for (i <- 0 until size) {
      println(i + " " + renumber(n2c(i)))
    }
  }

  def partition2graph_binary: Graph = {
    val (renumber, last) = getRenumbering

    // Compute communities
    val comm_nodes = Buffer[Buffer[Int]]()
    for (node <- 0 until size) {
      val n = renumber(n2c(node))
      while (comm_nodes.size < n+1) comm_nodes += Buffer[Int]()
      comm_nodes(n) += node
    }

    // Compute weighted graph
    val nb_nodes = comm_nodes.size
    val degrees = new Array[Int](nb_nodes)
    val links = Buffer[Int]()
    val weights = Buffer[Float]()

    val comm_deg = comm_nodes.size
    for (comm <- 0 until comm_deg) {
      val m = MutableMap[Int, Float]()

      val comm_size = comm_nodes(comm).size
      for (node <- 0 until comm_size) {
        val neighbors = g.neighbors(comm_nodes(comm)(node))
        val deg = g.nb_neighbors(comm_nodes(comm)(node))
        for (i <- 0 until deg) {
          val (neigh, neigh_weight) = neighbors.next
          val neigh_comm = renumber(n2c(neigh))
          m(neigh_comm) = m.get(neigh_comm).getOrElse(0.0f) + neigh_weight
        }
      }
      degrees(comm) = if (0 == comm) m.size else degrees(comm-1) + m.size
      m.map{case (neighbor, weight) =>
          links += neighbor
          weights += weight
      }
    }

    new Graph(degrees, links.toArray, Some(weights.toArray))
  }
}



class CommunityHarness {
  def run (graph: Graph,
           passes: Int = -1,
           precision: Double = 0.000001,
           display_level: Int = -2,
           verbose: Boolean = false,
           randomize: Boolean = true) = {
    val time_begin = System.currentTimeMillis()
    if (verbose) {
      println("Start time: "+new Date(time_begin))
      println("Input:")
      graph.display
    }

    var g = graph
    var c = new Community(g, passes, precision)
    var improvement = true
    var mod = c.modularity
    var new_mod = mod
    var level = 0

    do {
      improvement = c.one_level(randomize)
      new_mod = c.modularity
      level += 1
      if (level == display_level)
        g.display
      if (display_level == -1)
        c.display_partition

      g = c.partition2graph_binary
      c = new Community(g, -1, precision)

      if (verbose) {
        println("  modularity increased from " + mod + " to " + new_mod)
        g.display
      }
      mod = new_mod
      if (verbose)
        println("  end computation")
    } while (improvement)

    val time_end = System.currentTimeMillis()
    if (verbose) {
      println("End time: "+new Date(time_end))
      println("Total duration: "+((time_end - time_begin)/1000.0)+" seconds")
    }
    println("Final modularity: "+new_mod)
  }
}

object Community {
  var weighted = false
  var nb_pass = 0
  var precision = 0.000001
  var display_level = -2
  var k1 = 16
  var filename: Option[String] = None
  var filename_w: Option[String] = None
  var filename_part: Option[String] = None
  var verbose = false
  var randomize = true

  def usage (prog_name: String, more: String) = {
    println(more)
    println("usage: " + prog_name + " input_file [-w weight_file] [-p part_file] [-q minimumModularityGain] [-l display_level] [-v] [-h]")
    println
    println("input_file: file containing the graph to decompose in communities.")
    println("-w file\tread the graph as a weighted one (weights are set to 1 otherwise).")
    println("-p file\tstart the computation with a given partition instead of the trivial partition.")
    println("\tfile must contain lines \"node community\".")
    println("-q eps\ta given pass stops when the modularity is increased by less than minimumModularityGain.")
    println("-l k\tdisplays the graph of level k rather than the hierachical structure.")
    println("\tif k=-1 then displays the hierarchical structure rather than the graph at a given level.")
    println("-v\tverbose mode: gives computation time, information about the hierarchy and modularity.")
    println("-h\tshow this usage message.")
	println("-n\tDon't randomize the node order when converting, for repeatability in testing.")
    System.exit(0)
  }

  def parse_args (args: Array[String]) = {
    if (args.size < 1) usage("community", "Bad arguments number")

    var i = 0;
    while (i < args.size) {
      if (args(i).startsWith("-")) {
        args(i).substring(1).toLowerCase match {
          case "w" => {
            i = i + 1
            weighted = true
            filename_w = Some(args(i))
          }
          case "p" => {
            i = i + 1
            filename_part = Some(args(i))
          }
          case "q" => {
            i = i + 1
            precision = args(i).toDouble
          }
          case "l" => {
            i = i + 1
            display_level = args(i).toInt
          }
          case "k" => {
            i = i + 1
            k1 = args(i).toInt
          }
          case "v" => {
            verbose = true
          }
          case "n" => randomize = false
          case _ => usage("community", "Unknown option: "+args(i))
        }
      } else if (filename.isDefined) {
        usage("community", "More than one filename")
      } else {
        filename = Some(args(i))
      }
      i = i + 1
    }

  }

  def display_time (msg: String): Unit =
    Console.err.println(msg+": "+new Date(System.currentTimeMillis()))

  def main (args: Array[String]): Unit = {
    parse_args(args)

    val time_begin = System.currentTimeMillis()
    if (verbose) display_time("Begin")

    var c = new Community(filename.get, filename_w, weighted, -1, precision)
    filename_part.foreach(part => c.init_partition(part))

    var g: Graph = null
    var improvement: Boolean = true
    var mod: Double = c.modularity
    var level: Int = 0

    do {
      if (verbose) {
        Console.err.println("level "+level+":")
        display_time("  start computation")
        Console.err.println("  network size: "+c.g.nb_nodes+" nodes, "+c.g.nb_links+" links, "+c.g.total_weight+" weight.")
      }

      improvement = c.one_level(randomize)
      val new_mod = c.modularity

      level = level + 1
      if (level == display_level && null != g)
        g.display

      if (-1 == display_level)
        c.display_partition

      g = c.partition2graph_binary
      c = new Community(g, -1, precision)

      if (verbose)
        Console.err.println("  modularity increased from " + mod + " to "+new_mod)

      mod = new_mod
      if (verbose)
        display_time("  end computation")

      // do at least one more computation if partition is provided
      filename_part.foreach(part => if (1 == level) improvement = true)
    } while (improvement)
    val time_end = System.currentTimeMillis()
    if (verbose) {
      display_time("End")
      Console.err.println("Total duration: %.3f sec.".format((time_end-time_begin)/1000.0))
    }
    Console.err.println("Final modularity: "+mod)
  }
}
//scalastyle:on
