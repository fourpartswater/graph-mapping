/**
 * This code is copied and translated from https://sites.google.com/site/findcommunities
 *
 * This means it is probably (c) 2008 V. Blondel, J.-L. Guillaume, R. Lambiotte, E. Lefebvre, and that
 * we can't distribute it without permission - though as a translation, with some optimization for readability in
 * scala, it may be a gray area.
 */
package software.uncharted.graphing.clustering.unithread

import java.io.{BufferedReader, File, FileInputStream, FileOutputStream, InputStreamReader, PrintStream}
import java.util.Date

import software.uncharted.graphing.utilities.SimpleProfiling

import scala.collection.mutable.{Buffer => MutableBuffer, Map => MutableMap}
import scala.util.Random


/**
  * Trait to specify what modification to apply to the baseline algorithm, along with the parameters needed
  * to modify it as specified
  */
trait AlgorithmModification

/** Use the algorithm as is */
class BaselineAlgorithm extends AlgorithmModification

/** Only allow nodes to change community to that of a node with sufficiently low degree, where "sufficiently" is
  * defined in a level-dependent manner */
class NodeDegreeAlgorithm (val degreeLimit: Long) extends AlgorithmModification

/** Only allow nodes to change community to communities still below a given size. */
class CommunitySizeAlgorithm (val maximumSize: Int) extends AlgorithmModification

/**
 *
 * @param g The graph to cluster
 * @param nb_pass number of pass for one level computation
 *                if -1, compute as many pass as needed to increas modularity
 * @param min_modularity a new pass is computed if the last one has generated an increase greater than min_modularity
 *                       if 0, even a minor increase is enough to go for one more pass
 * @param algorithmMod The modification to apply to the baseline clustering algorithm.
 */
class Community (val g: Graph,
                 nb_pass: Int,
                 min_modularity: Double,
                 algorithmMod: AlgorithmModification = new BaselineAlgorithm) {

  def this (filename: String,  filename_w: Option[String], filename_m: Option[String],
            nbp: Int, minm: Double, algorithmMod: AlgorithmModification = new BaselineAlgorithm) =
    this(Graph(filename, filename_w, filename_m), nbp, minm, algorithmMod)




  val size = g.nb_nodes
  val n2c = new Array[Int](size)
  val community_size = new Array[Int](size)
  for (i <- 0 until size) {
    n2c(i) = i
    community_size(i) = 1
  }
  val tot = n2c.map(n => g.weighted_degree(n))
  val in = n2c.map(n => g.nb_selfloops(n))
  val neigh_weight = n2c.map(n => -1.0)
  val neigh_pos = n2c.map(n => 0)
  var neigh_last: Int = 0


  def remove(node: Int, comm: Int, dnodecomm: Double): Int = {
    tot(comm) = tot(comm) - g.weighted_degree(node)
    in(comm) = in(comm) - (2 * dnodecomm + g.nb_selfloops(node))
    n2c(node) = -1
    val cs = community_size(node)
    community_size(node) = 0
    cs
  }

  def insert(node: Int, comm: Int, dnodecomm: Double, representedNodes: Int): Unit = {
    tot(comm) += + g.weighted_degree(node)
    in(comm) += (2 * dnodecomm + g.nb_selfloops(node))
    n2c(node) = comm
    community_size(node) += representedNodes
  }

  def modularity_gain(node: Int, comm: Int, dnodecomm: Double, w_degree: Double): Double = {
    val totc = tot(comm)
    val degc = w_degree
    val m2 = g.total_weight
    val dnc = dnodecomm

    dnc - totc * degc / m2
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

    val allowed: Int => Boolean = algorithmMod match {
      case nd: NodeDegreeAlgorithm =>
        // Only allow joining with this neighbor if its degree is less than our limit.
        nn => g.nb_neighbors(nn) < nd.degreeLimit
      case cs: CommunitySizeAlgorithm =>
        // Only allow joining with this neighbor if its internal size is less than our limit
        nn => community_size(n2c(node)) < cs.maximumSize
      case _ =>
        nn => true
    }

    if (allowed(node)) {
      val neighbors = g.neighbors(node)
      for (i <- 0 until degree) {
        val (neighbor, neighbor_weight) = neighbors.next
        val neighbor_community = n2c(neighbor)
        if (allowed(neighbor)) {
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
    }
  }

  def one_level (randomize: Boolean = true): Boolean = {
    var improvement = false
    var nb_moves: Int = 0
    var nb_pass_done: Int = 0
    var new_mod = modularity
    var cur_mod = new_mod

    // Randomize the order of vertex inspection
    val random_order = new Array[Int](size)
    for (i <- 0 until size) random_order(i) = i
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
    //   or there is an improvement of modularity greater than a given epsilon
    //   or a predefined number of pass have been done
    do {
      cur_mod = new_mod
      nb_moves = 0
      nb_pass_done += 1

      // for each node: remove the node from its community and insert it in the best community
      for (node_tmp <- 0 until size) {
        if (0 == (node_tmp % 100000)) println(node_tmp+" ...")
        val node = random_order(node_tmp)
        val node_comm = n2c(node)
        val w_degree = g.weighted_degree(node)

        // computation of all neighboring communities of current node
        neigh_comm(node)
        // remove node from its current community
        val currentCommunitySize = remove(node, node_comm, neigh_weight(node_comm))

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
        insert(node, best_comm, best_nblinks, currentCommunitySize)

        if (best_comm != node_comm)
          nb_moves += 1
      }

      var total_tot = 0.0
      var total_in = 0.0
      for (i <- tot.indices) {
        total_tot += tot(i)
        total_in += in(i)
      }

      new_mod = modularity
      if (nb_moves > 0)
        improvement = true

    } while (nb_moves > 0 && new_mod - cur_mod > min_modularity)

    improvement
  }

  private def getRenumbering: (Array[Int], Int) = {
    val renumber = new Array[Int](size)
    for (i <- 0 until size) renumber(i) = -1

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
      val currentCommunitySize = remove(node, old_comm, neigh_weight(old_comm))

      var i = 0
      var done = false
      while (i < neigh_last && !done) {
        val best_comm = neigh_pos(i)
        val best_nblinks = neigh_weight(best_comm)
        if (best_comm == comm) {
          insert(node, best_comm, best_nblinks, currentCommunitySize)
          done = true
        } else {
          i = i + 1
        }
      }

      if (!done)
        insert(node, comm, 0, currentCommunitySize)

      line = finput.readLine()
    }
    finput.close()
  }

  def display_partition (out: PrintStream, stats: Option[PrintStream]) : Unit = {
    val (renumber, last) = getRenumbering

    // write nodes to output file
    for (i <- 0 until size) {
      out.println("node\t"+g.id(i)+"\t"+g.id(n2c(i))+"\t"+g.internalSize(i)+"\t"+g.weighted_degree(i).round.toInt+"\t"+g.metaData(i))
    }
    // write links to output file
    g.display_links(out)

    // write stats to stats file
    stats.map{statsStream =>

      var nodes = 0.0
      var totalL0 = 0.0
      var totalL1 = 0.0
      var totalL2 = 0.0
      var minCS = Int.MaxValue
      var maxCS = Int.MinValue
      community_size.foreach{n =>
        nodes += 1.0
        if (n > 0) {
          totalL0 += 1
          totalL1 += n
          totalL2 += n * n
          minCS = minCS min n
          maxCS = maxCS max n
        }
      }
      val mean = totalL1 / totalL0
      val stdDev = totalL2 / totalL0 - mean * mean

      statsStream.println()
      statsStream.println("Total nodes at this level: "+nodes)
      statsStream.println("Total communities at this level: " + totalL0)
      statsStream.println("Minimum(community size) at this level: "+minCS)
      statsStream.println("Maximum(community size) at this level: "+maxCS)
      statsStream.println("Sum(community size) at this level: " + totalL1)
      statsStream.println("Sum(community size ^ 2) at this level: " + totalL2)
      statsStream.println("Mean community size at this level: " + mean)
      statsStream.println("Standard deviation of community size at this level: " + stdDev)
    }
  }

  def partition2graph_binary: Graph = {
    val (renumber, _) = getRenumbering

    // Compute communities
    val comm_nodes = MutableBuffer[MutableBuffer[Int]]()
    for (node <- 0 until size) {
      val n = renumber(n2c(node))
      while (comm_nodes.size < n+1) comm_nodes += MutableBuffer[Int]()
      comm_nodes(n) += node
    }

    // Compute weighted graph
    val nb_nodes = comm_nodes.size
    val degrees = new Array[Int](nb_nodes)
    val links = MutableBuffer[Int]()
    val nodeInfos = new Array[NodeInfo](nb_nodes)
    val weights = MutableBuffer[Float]()

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
          m(neigh_comm) = m.getOrElse(neigh_comm, 0.0f) + neigh_weight
        }
      }
      degrees(comm) = if (0 == comm) m.size else degrees(comm-1) + m.size
      m.map{case (neighbor, weight) =>
          links += neighbor
          weights += weight
      }

      nodeInfos(comm) = comm_nodes(comm).map(node => (g.nodeInfo(node), g.weighted_degree(node))).reduce{(a, b) =>
        if (a._2 > b._2) (a._1 + b._1, a._2) else (b._1 + a._1, b._2)
      }._1
    }

    new Graph(degrees, links.toArray, nodeInfos, Some(weights.toArray))
  }
}



object Community {
  var nb_pass = 0
  var precision = 0.000001
  var display_level = -2
  var k1 = 16
  var filename: Option[String] = None
  var filename_w: Option[String] = None
  var filename_m: Option[String] = None
  var filename_part: Option[String] = None
  var verbose = false
  var randomize = true
  var algorithm: AlgorithmModification = new BaselineAlgorithm

  def usage (prog_name: String, more: String) = {
    println(more)
    println("usage: " + prog_name + " input_file [-w weight_file] [-p part_file] [-q epsilon] [-l display_level] [-v] [-h]")
    println
    println("input_file: file containing the graph to decompose in communities.")
    println("-w file\tread the graph as a weighted one (weights are set to 1 otherwise).")
    println("-m file\tread metadata for nodes (set to none otherwise).")
    println("-p file\tstart the computation with a given partition instead of the trivial partition.")
    println("\tfile must contain lines \"node community\".")
    println("-q eps\ta given pass stops when the modularity is increased by less than epsilon.")
    println("-l k\tdisplays the graph of level k rather than the hierachical structure.")
    println("\tif k=-1 then displays the hierarchical structure rather than the graph at a given level.")
    println("-v\tverbose mode: gives computation time, information about the hierarchy and modularity.")
    println("-h\tshow this usage message.")
    println("-n\tDon't randomize the node order when converting, for repeatability in testing.")
    println("-nd <base>\tUse the node degree algorithm modification with the specified base.  This will only join nodes with other nodes of at most degree <base> on the first clustering level, base^2 on the second, base^3 on the third, etc.  Exclusive with -cs.")
    println("-cs <size>\tUse the cluster size algorithm modification with the specified size.  Nodes will only be merged into a community if that community is smaller than the specified size. Exclusive with -nd")
    System.exit(0)
  }

  def parse_args (args: Array[String]) = {
    if (args.length < 1) usage("community", "Bad arguments number")

    var i = 0
    while (i < args.length) {
      if (args(i).startsWith("-")) {
        args(i).substring(1).toLowerCase match {
          case "w" =>
            i = i + 1
            filename_w = Some(args(i))

          case "m" =>
            i = i + 1
            filename_m = Some(args(i))

          case "p" =>
            i = i + 1
            filename_part = Some(args(i))

          case "q" =>
            i = i + 1
            precision = args(i).toDouble

          case "l" =>
            i = i + 1
            display_level = args(i).toInt

          case "k" =>
            i = i + 1
            k1 = args(i).toInt

          case "v" =>
            verbose = true

          case "n" => randomize = false

          case "nd" =>
            i = i + 1
            algorithm = new NodeDegreeAlgorithm(args(i).toInt)

          case "cs" =>
            i = i + 1
            algorithm = new CommunitySizeAlgorithm(args(i).toInt)

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
    val curDir: Option[File] = if (-1 == display_level) Some(new File(".")) else None

    SimpleProfiling.register("init.community")
    var c = new Community(filename.get, filename_w, filename_m, -1, precision, algorithm)
    SimpleProfiling.finish("init.community")

    SimpleProfiling.register("init.partition")
    filename_part.foreach(part => c.init_partition(part))
    SimpleProfiling.finish("init.partition")

    var g: Graph = null
    var improvement: Boolean = true
    SimpleProfiling.register("init.modularity")
    var mod: Double = c.modularity
    SimpleProfiling.finish("init.modularity")
    var level: Int = 0

    do {
      SimpleProfiling.register("iterative")
      if (verbose) {
        Console.err.println("level "+level+":")
        display_time("  start computation")
        Console.err.println("  network size: "+c.g.nb_nodes+" nodes, "+c.g.nb_links+" links, "+c.g.total_weight+" weight.")
      }

      SimpleProfiling.register("iterative.one_level")
      improvement = c.one_level(randomize)
      SimpleProfiling.finish("iterative.one_level")
      SimpleProfiling.register("iterative.modularity")
      val new_mod = c.modularity
      SimpleProfiling.finish("iterative.modularity")

      level = level + 1
      if (level == display_level && null != g) {
        g.display_nodes(Console.out)
        g.display_links(Console.out)
      }

      SimpleProfiling.register("iterative.convert")
      g = c.partition2graph_binary
      SimpleProfiling.finish("iterative.convert")

      SimpleProfiling.register("iterative.write")
      curDir.foreach { pwd =>
        val levelDir = new File(pwd, "level_" + (level-1))
        levelDir.mkdir()
        val out = new PrintStream(new FileOutputStream(new File(levelDir, "part_00000")))
        val stats = new PrintStream(new FileOutputStream(new File(levelDir, "stats")))
        c.display_partition(out, Some(stats))
        out.flush()
        out.close()
        stats.flush()
        stats.close()
      }
      SimpleProfiling.finish("iterative.write")

      val levelAlgorithm = algorithm match {
        case nd: NodeDegreeAlgorithm => new NodeDegreeAlgorithm(math.pow(nd.degreeLimit, level + 1).toLong)
        case cs: CommunitySizeAlgorithm => algorithm
        case _ => algorithm
      }
      SimpleProfiling.register("iterative.communitize")
      c = new Community(g, -1, precision, levelAlgorithm)
      SimpleProfiling.finish("iterative.communitize")

      if (verbose)
        Console.err.println("  modularity increased from " + mod + " to "+new_mod)

      mod = new_mod
      if (verbose)
        display_time("  end computation")

      // do at least one more computation if partition is provided
      filename_part.foreach(part => if (1 == level) improvement = true)
      println
      println("After level "+level+": ")
      SimpleProfiling.report(System.out)
      SimpleProfiling.finish("iterative")
    } while (improvement)
    val time_end = System.currentTimeMillis()
    if (verbose) {
      display_time("End")
      Console.err.println("Total duration: %.3f sec.".format((time_end-time_begin)/1000.0))
    }
    Console.err.println("Final modularity: "+mod)
  }
}
