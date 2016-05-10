package software.uncharted.graphing.clustering.experiments.samples


import java.io.{OutputStreamWriter, FileOutputStream}

import scala.collection.mutable.{Buffer => MutableBuffer}
import software.uncharted.graphing.utilities.ArgumentParser

import scala.util.Random


/**
  * Generate a set of raw sample graph data with analytic information
  */
object AnalyticSampleGenerator {
  def main (args: Array[String]): Unit = {
    val argParser = new ArgumentParser(args)

    val (levels, clusterSize, analytics, fileName) =
    try {
      val levels = argParser.getInt("levels", "The number of levels to generate in the cluster hierarchy", 5)
      val clusterSize = argParser.getInt("size", "The size of each cluster, at each level", 5)
      val analytics =
        argParser.getStrings("analytic", "Formulae to determine analytic values. r for random in [0, 1), n for node number, + - * / ^ accepted")
        .map(entryString =>
          Entry.parseEntryString(entryString)
        )
      val fileName = argParser.getString("output", "The file to which to output all values", "output.graph")

      (levels, clusterSize, analytics, fileName)
    } catch {
      case e: Exception =>
        argParser.usage
        throw e
    }

    // Generate nodes
    val numNodes = (1 to levels).map(level => clusterSize).product
    val nodes = new Array[(Int, String, Seq[Double])](numNodes)
    for (i <- nodes.indices) {
      nodes(i) = (i, s"node $i", analytics.map(_.calculate(i)))
    }
    val edges = MutableBuffer[(Int, Int)]()
    for (level <- 0 until levels) {
      val clusters = (0 to level).foldLeft(numNodes)((a, b) => a / clusterSize)
      val nodesPerCluster = numNodes / clusters
      val nodesPerSubCluster = nodesPerCluster / clusterSize
      val random = new Random

      // Node
      //     00 01 02 03 04 05 06 07 08 09 10 11 12 13 14 15 16 17 18 19 20 21 22 23 24 25 26
      // level 0
      //   c  0  0  0  1  1  1  2  2  2  3  3  3  4  4  4  5  5  5  6  6  6  7  7  7  8  8  8
      //   n  0  1  2  0  1  2  0  1  2  0  1  2  0  1  2  0  1  2  0  1  2  0  1  2  0  1  2
      // level 1
      //   c  0  0  0  0  0  0  0  0  0  1  1  1  1  1  1  1  1  1  2  2  2  2  2  2  2  2  2
      //   n  0  0  0  1  1  1  2  2  2  0  0  0  1  1  1  2  2  2  0  0  0  1  1  1  2  2  2
      // level 2
      //   c  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0  0
      //   n  0  0  0  0  0  0  0  0  0  1  1  1  1  1  1  1  1  1  2  2  2  2  2  2  2  2  2
      def getNode (clusterNumber: Int, nodeNumber: Int): Int = {
        val clusterStart = nodesPerCluster * clusterNumber
        val nodeStart = clusterStart + nodeNumber * nodesPerSubCluster
        val nodeEnd = nodeStart + nodesPerSubCluster
        (nodeStart until nodeEnd).apply(random.nextInt(nodesPerSubCluster))
      }
      for (c <- 0 until clusters) {
        for (n1 <- 0 until clusterSize; n2 <- (n1 + 1) until clusterSize) {
          edges += ((getNode(c, n1), getNode(c, n2)))
        }
      }
    }

    val outFile = new OutputStreamWriter(new FileOutputStream(fileName))
    // Write out nodes
    nodes.foreach { case (id, name, analyticValues) =>
      outFile.write("node\t")
      outFile.write(id.toString)
      outFile.write("\t")
      outFile.write(name)
      analyticValues.foreach { a =>
        outFile.write("\t")
        outFile.write(a.toString)
      }
      outFile.write("\n")
    }

    // Write out edges
    edges.foreach { case (start, end) =>
      outFile.write("edge\t")
      outFile.write(start.toString)
      outFile.write("\t")
      outFile.write(end.toString)
      outFile.write("\n")
    }

    outFile.flush()
    outFile.close()
    println("Wrote " + nodes.length + " nodes and " + edges.length + " edges")
  }
}


object Entry {
  private type Token = (Int, String)
  private type TokenOrEntry = Either[Token, Entry]

  private val operatorType = 0
  private val variableType = 1
  private val constantType = 2

  private val leftToRight = true
  private val rightToLeft = false

  val rules: Seq[(String, Int, Boolean, (Int, Seq[TokenOrEntry]) => Option[Seq[TokenOrEntry]])] = Seq(
    // Convert variables and numbers
    ("node", 1, leftToRight, (tokenNum: Int, ts: Seq[TokenOrEntry]) => {
      ts(0) match {
        case Left((`variableType`, "n")) => Some(Seq(Right(NodeValueEntry)))
        case _ => None
      }
    }),
    ("random", 1, leftToRight, (tokenNum: Int, ts: Seq[TokenOrEntry]) => {
      ts(0) match {
        case Left((`variableType`, "r")) => Some(Seq(Right(RandomValueEntry)))
        case _ => None
      }
    }),
    ("number", 1, leftToRight, (tokenNum: Int, ts: Seq[TokenOrEntry]) => {
      ts(0) match {
        case Left((`constantType`, number)) => Some(Seq(Right(NumericEntry(number.toDouble))))
        case _ => None
      }
    }),
    // Consolidate negative and positive signs with numbers, and negate existing entries
    ("initial negation", 2, leftToRight, (tokenNum: Int, ts: Seq[TokenOrEntry]) => {
      (tokenNum, ts(0), ts(1)) match {
        case (0, Left((`operatorType`, "-")), Right(entry)) =>
          Some(Seq(Right(NegationEntry(entry))))
        case _ => None
      }
    }),
    ("internal negation", 3, rightToLeft, (tokenNum: Int, ts: Seq[TokenOrEntry]) => {
      (ts(0), ts(1), ts(2)) match {
        case (Left((`operatorType`, op0)), Left((`operatorType`, "-")), Right(entry)) =>
          Some(Seq(Left((operatorType, op0)), Right(NegationEntry(entry))))
        case _ => None
      }
    }),
    // Exponentiation
    ("exponentiation", 3, leftToRight, (tokenNum: Int, ts: Seq[TokenOrEntry]) => {
      (ts(0), ts(1), ts(2)) match {
        case (Right(entry0), Left((`operatorType`, "^")), Right(entry2)) =>
          Some(Seq(Right(ExponentEntry(entry0, entry2))))
        case _ => None
      }
    }),
    // Multiplication and Division
    ("multiplication", 3, leftToRight, (tokenNum: Int, ts: Seq[TokenOrEntry]) => {
      (ts(0), ts(1), ts(2)) match {
        case (Right(entry0), Left((`operatorType`, "*")), Right(entry2)) =>
          Some(Seq(Right(ProductEntry(entry0, entry2))))
        case (Right(entry0), Left((`operatorType`, "/")), Right(entry2)) =>
          Some(Seq(Right(QuotientEntry(entry0, entry2))))
        case _ => None
      }
    }),
    // Addition and Subtraction
    ("addition", 3, leftToRight, (tokenNum: Int, ts: Seq[TokenOrEntry]) => {
      (ts(0), ts(1), ts(2)) match {
        case (Right(entry0), Left((`operatorType`, "+")), Right(entry2)) =>
          Some(Seq(Right(SumEntry(entry0, entry2))))
        case (Right(entry0), Left((`operatorType`, "-")), Right(entry2)) =>
          Some(Seq(Right(DifferenceEntry(entry0, entry2))))
        case _ => None
      }
    })
  )

  def parseEntryString (entryString: String): Entry = {
    val tokens: MutableBuffer[TokenOrEntry] =
      tokenize(entryString).map(t => Left((t._1, t._2)))

    rules.foreach { case (name, size, lToR, rule) =>
      if (lToR) {
        var i = 0
        while (i <= tokens.length - size) {
          val slice = tokens.slice(i, i + size)
          rule(i, slice).foreach{replacement =>
            tokens.remove(i, size)
            tokens.insertAll(i, replacement)
            i -= 1 // Retry at same point
          }
          i += 1
        }
      } else {
        var i = tokens.length - size
        while (i >= 0) {
          val slice = tokens.slice(i, i + size)
          rule(i, slice).foreach{replacement =>
            tokens.remove(i, size)
            tokens.insertAll(i, replacement)
          }
          i -= 1
        }
      }
    }

    // We had better be left with a single entry
    if (1 == tokens.length) {
      tokens(0) match {
        case Right(entry) => entry
        case _ => throw new IllegalArgumentException(s"Cannot parse $entryString - unparsable")
      }
    } else {
      throw new IllegalArgumentException(s"Cannot parse $entryString - irreducible")
    }
  }

  private def tokenize (entryString: String): MutableBuffer[(Int, String, Int)] = {
    var i = 0
    val tokens = MutableBuffer[(Int, String, Int)]()

    while (i < entryString.length) {
      entryString.charAt(i) match {
        case 'n' | 'r' =>
          tokens += ((variableType, entryString.substring(i, i+1), i))
          i += 1
        case '+' | '-' | '*' | '/' | '^' =>
          tokens += ((operatorType, entryString.substring(i, i+1), i))
          i += 1
        case '0' | '1' | '2' | '3' | '4' | '5' | '6' | '7' | '8' | '9' =>
          val (number, place) = getNumber(entryString, i).get
          tokens += ((constantType, number, i))
          i = place
        case _ =>
          i += 1
      }
    }

    tokens
  }

  private def getNumber (entry: String, place: Int): Option[(String, Int)] = {
    def inRange (index: Int, start: Char, end: Char): Boolean = {
      val value = entry.charAt(index)
      start <= value && value <= end
    }
    def inSet (index: Int, possibilities: Char*): Boolean = {
      val value = entry.charAt(index)
      possibilities.map(_ == value).reduce(_ || _)
    }

    val start = place
    var end = place
    if (inSet(end, '-', '+')) end += 1
    if (!inRange(end, '0', '9')) {
      None
    } else {
      while (inRange(end, '0', '9')) end += 1
      if (inSet(end, '.')) {
        end += 1
        while (inRange(end, '0', '9')) end += 1
      }
      if (inSet(end, 'e', 'E')) {
        var expEnd = end + 1
        if (inSet(expEnd, '-', '+')) expEnd += 1
        if (inRange(expEnd, '0', '9')) {
          end = expEnd
          while (inRange(end, '0', '9')) end += 1
        }
      }
      Some((entry.substring(start, end), end))
    }
  }
}
trait Entry {
  def calculate (node: Int): Double
}
case class NumericEntry (value: Double) extends Entry {
  override def calculate(node: Int): Double = value
}
case object NodeValueEntry extends Entry {
  override def calculate(node: Int): Double = node.toDouble
}
case object RandomValueEntry extends Entry {
  override def calculate(node: Int): Double = math.random
}
case class NegationEntry (rhs: Entry) extends Entry {
  override def calculate(node: Int): Double =
    -rhs.calculate(node)
}
case class SumEntry (lhs: Entry, rhs: Entry) extends Entry {
  override def calculate(node: Int): Double =
    lhs.calculate(node) + rhs.calculate(node)
}
case class DifferenceEntry (lhs: Entry, rhs: Entry) extends Entry {
  override def calculate (node: Int): Double =
    lhs.calculate(node) - rhs.calculate(node)
}
case class ProductEntry (lhs: Entry, rhs: Entry) extends Entry {
  override def calculate(node: Int): Double =
    lhs.calculate(node) * rhs.calculate(node)
}
case class QuotientEntry (numerator: Entry, denominator: Entry) extends Entry {
  override def calculate(node: Int): Double =
    numerator.calculate(node) / denominator.calculate(node)
}
case class ExponentEntry (base: Entry, exponent: Entry) extends Entry {
  override def calculate(node: Int): Double =
    math.pow(base.calculate(node), exponent.calculate(node))
}