/*
 * Copyright (c) 2014-2016 Uncharted Software Inc. All rights reserved.
 *
 * Property of Uncharted(tm), formerly Oculus Info Inc.
 * http://uncharted.software/
 *
 * This software is the confidential and proprietary information of
 * Uncharted Software Inc. ("Confidential Information"). You shall not
 * disclose such Confidential Information and shall use it only in
 * accordance with the terms of the license agreement you entered into
 * with Uncharted Software Inc.
 */
package software.uncharted.graphing.layout

import java.awt.{BorderLayout, Color, Dimension, Graphics}
import java.awt.event.ActionEvent
import java.io.{BufferedReader, File, FileInputStream, FileOutputStream, InputStreamReader, PrintStream}
import javax.swing.{AbstractAction, JFrame, JMenu, JMenuBar, JMenuItem, JPanel, JTabbedPane}

import scala.collection.mutable.{Buffer => MutableBuffer}
import org.apache.spark.SharedSparkContext
import org.scalatest.FunSuite


class LayoutExample extends FunSuite with SharedSparkContext {
  def levelWeight (level: Int): Int = {
    if (0 == level) 0
    else 4 * levelWeight(level - 1) + 6
  }

  private def connectedNodeName (id: Int, level: Int, levels: Int): String = {
    val parts = for (i <- (levels - level) to 0 by -1) yield (id >> (2 * i)) & 3
    val hid = parts.mkString(":")
    s"connected node $hid[$id] on hierarchy level $level"
  }
  private def makeSource: File = {
    val srcDir = new File("layout-example-input")
    srcDir.mkdir()

    val numDisconnected = 128
    val maxLevel = 2
    val maxNodeId = 4 << (2 * maxLevel)
    for (level <- 0 to maxLevel) {
      val levelDir = new File(srcDir, "level_" + level)
      levelDir.mkdir()
      val src = new File(levelDir, "part-00000")
      val srcStream = new PrintStream(new FileOutputStream(src))

      val idInc = 4 << (2 * level)
      val nodeSize = idInc >> 2
      // Write out 4^(maxLevel-level) fully connected sub-clusters of 4 nodes each
      for (i <- 0 until maxNodeId by idInc) {
        for (c <- 0 until 4) {
          val id = i + c * idInc / 4
          srcStream.println("node\t"+id+"\t"+i+"\t"+nodeSize+"\t"+nodeSize+"\t"+connectedNodeName(id, level, maxLevel))
        }
      }
      // Write out numDisconnected fully disconnected nodes
      for (i <- 0 until numDisconnected) {
        val id = maxNodeId + i
        srcStream.println("node\t"+id+"\t"+id+"\t"+1+"\t"+0+"\t"+s"disconnected node $i on hierarchy level $level")
      }

      // Connect nodes
      val internalWeight = 1
      val externalWeight = 1
      for (edgeLevel <- level to maxLevel) {
        val lvlInc = 4 << (2 * edgeLevel)
        for (i <- 0 until maxNodeId by lvlInc) {
          val a = i + 0 * lvlInc / 4
          val b = i + 1 * lvlInc / 4
          val c = i + 2 * lvlInc / 4
          val d = i + 3 * lvlInc / 4

          // self-connections from lower levels
          if (edgeLevel == level) {
            val selfWeight = levelWeight(level)
            if (selfWeight > 0) {
              srcStream.println("edge\t" + a + "\t" + a + "\t" + selfWeight)
              srcStream.println("edge\t" + b + "\t" + b + "\t" + selfWeight)
              srcStream.println("edge\t" + c + "\t" + c + "\t" + selfWeight)
              srcStream.println("edge\t" + d + "\t" + d + "\t" + selfWeight)
            }
          }

          // Connect up nodes on this level
          srcStream.println("edge\t" + a + "\t" + b + "\t" + internalWeight)
          srcStream.println("edge\t" + a + "\t" + c + "\t" + internalWeight)
          srcStream.println("edge\t" + a + "\t" + d + "\t" + internalWeight)
          srcStream.println("edge\t" + b + "\t" + c + "\t" + internalWeight)
          srcStream.println("edge\t" + b + "\t" + d + "\t" + internalWeight)
          srcStream.println("edge\t" + c + "\t" + d + "\t" + internalWeight)
        }
      }
      srcStream.flush()
      srcStream.close()
    }

    srcDir
  }
  private def getOutputLocation: File = {
    val outDir = new File("layout-example-output")
    outDir
  }
  test("Example of layout application, to be used to debug through the process") {
    val sourceDir = makeSource
    val outputDir = getOutputLocation
    val partitions = 0
    val consolidationPartitions = 0
    val dataDelimiter = "\t"
    val maxIterations = 500
    val maxHierarchyLevel = 2
    val borderPercent = 2.0
    val layoutLength = 256.0
    val nodeAreaPercent = 30
    val bUseEdgeWeights = false
    val gravity = 0.0
    val isolatedDegreeThres = 0
    val communitySizeThres = 0

    val fileStartTime = System.currentTimeMillis()

    // Hierarchical Force-Directed layout scheme
    val layouter = new HierarchicFDLayout()

    layouter.determineLayout(sc,
      maxIterations,
      maxHierarchyLevel,
      partitions,
      consolidationPartitions,
      sourceDir.getAbsolutePath,
      dataDelimiter,
      (layoutLength,layoutLength),
      borderPercent,
      nodeAreaPercent,
      bUseEdgeWeights,
      gravity,
      isolatedDegreeThres,
      communitySizeThres,
      outputDir.getAbsolutePath)

    sourceDir.delete()
    outputDir.delete()
  }
}

object DisplayApp {
  def main (args: Array[String]): Unit = {
    val app = new DisplayApp("layout-example-output", 3)
    app.setup
    app.setVisible(true)
  }
}
class DisplayApp (outputPath: String, levels: Int) extends JFrame {
  def setup: Unit = {
    val tabs = new JTabbedPane()
    for (i <- 0 until levels) {
      val levelPane = new LevelPane(outputPath, i)
      tabs.add("Level "+i, levelPane)
    }
    getContentPane.setLayout(new BorderLayout())
    getContentPane.add(tabs, BorderLayout.CENTER)

    setLocation(100, 100)
    setSize(600, 700)
    setupMenu

    setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
  }
  def setupMenu: Unit = {
    val bar: JMenuBar = new JMenuBar
    val fileMenu: JMenu = new JMenu("file")
    val exit = new JMenuItem(new AbstractAction("exit") {
      override def actionPerformed(actionEvent: ActionEvent): Unit = {
        setVisible(false);
      }
    })
    fileMenu.add(exit)
    bar.add(fileMenu)
    setJMenuBar(bar)
  }
}
class LevelPane (outputPath: String, level: Int) extends JPanel {
  val nodes: List[NodeEntry] = readData()
  def readData (): List[NodeEntry] = {
    val dataDir = new File(outputPath, "level_"+level)
    val dataFile = new File(dataDir, "part-00000")
    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile)))
    val bnodes = MutableBuffer[NodeEntry]()

    var line = reader.readLine()
    while (null != line) {
      val fields = line.split("\t")
      bnodes += NodeEntry(
        fields(1).toLong, fields(2).toDouble, fields(3).toDouble, fields(4).toDouble,
        fields(5).toLong, fields(6).toDouble, fields(7).toDouble, fields(8).toDouble,
        fields(9).toInt, fields(10).toInt, fields(11)
      )
      line = reader.readLine()
    }
    bnodes.toList
  }
  override def paint (g: Graphics): Unit = {
    val size: Dimension = getSize
    def coords (x: Double, y: Double): (Int, Int) = {
      ((size.getWidth * x / 256.0).round.toInt, (size.getHeight* y / 256.0).round.toInt)
    }
    def circle (color: Color, x: Double, y: Double, r: Double, fill: Boolean, idOpt: Option[Long] = None): Unit = {
      g.setColor(color)
      val (lx, ly) = coords(x, y)
      val (dxr, dyr) = coords(r, r)
      val dx = dxr max 1
      val dy = dyr max 1
      if (fill) g.fillOval(lx - dx, ly - dy, 2*dx+1, 2*dy+1)
      else g.drawOval(lx - dx, ly - dy, 2*dx+1, 2*dy+1)
      idOpt.foreach{id =>
        g.setColor(Color.BLACK)
        g.drawString(""+id, lx, ly)
      }
    }
    g.setColor(Color.WHITE)
    g.fillRect(0, 0, size.getWidth.toInt, size.getHeight.toInt)

    // draw nodes
    for (node <- nodes) {
      circle(Color.BLUE, node.x, node.y, node.r, true /*, Some(node.id) */)
      circle(Color.RED, node.px, node.py, node.pr, false)
    }

    // Draw tick marks
    def plus (color: Color, x: Double, y: Double, r: Double): Unit = {
      g.setColor(color)
      val (lx, ly) = coords(x, y)
      val (dx, dy) = coords(r, r)
      g.drawLine(lx - dx, ly, lx + dx, ly)
      g.drawLine(lx, ly - dy, lx, ly + dy)
    }

    plus(Color.GREEN, 128.0, 128.0, 16.0)
    plus(Color.GREEN,  64.0,  64.0,  8.0)
    plus(Color.GREEN, 128.0,  64.0,  8.0)
    plus(Color.GREEN, 196.0,  64.0,  8.0)
    plus(Color.GREEN, 196.0, 128.0,  8.0)
    plus(Color.GREEN, 196.0, 196.0,  8.0)
    plus(Color.GREEN, 128.0, 196.0,  8.0)
    plus(Color.GREEN,  64.0, 196.0,  8.0)
    plus(Color.GREEN,  64.0, 128.0,  8.0)
  }
}

case class NodeEntry (id: Long, x: Double, y: Double, r: Double, pid:Long, px: Double, py: Double, pr: Double, internal: Int, degree: Int, metadata: String)
