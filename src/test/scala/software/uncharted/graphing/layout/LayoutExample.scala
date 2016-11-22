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

import java.awt.{BorderLayout, Color, Dimension, Font, Graphics, GraphicsEnvironment}
import java.awt.event.ActionEvent
import java.io.{BufferedReader, File, FileInputStream, FileOutputStream, InputStreamReader, PrintStream}
import javax.swing.{AbstractAction, JFrame, JMenu, JMenuBar, JMenuItem, JPanel, JTabbedPane}

import com.typesafe.config.ConfigFactory
import software.uncharted.graphing.layout.forcedirected.ForceDirectedLayoutParameters

import scala.collection.mutable.{Buffer => MutableBuffer}
import org.apache.spark.SharedSparkContext
import org.scalatest.FunSuite


class LayoutExample extends FunSuite with SharedSparkContext {
  def levelWeight (level: Int, connectedNodesPerLevel: Int): Int = {
    if (0 == level) 0
    else connectedNodesPerLevel * levelWeight(level - 1, connectedNodesPerLevel) + connectedNodesPerLevel * (connectedNodesPerLevel - 1) / 2
  }

  private def connectedNodeName (id: Int, level: Int, levels: Int, connectedNodesPerLevel: Int): String = {
    val lp = for (i <- levels to level by -1) yield math.pow(connectedNodesPerLevel, i).round.toInt
    val parts = for (i <- levels to level by -1) yield ((id / math.pow(connectedNodesPerLevel, i).round.toInt) % connectedNodesPerLevel)
    val hid = parts.mkString(":")
    s"connected node $hid[$id] on hierarchy level $level"
  }
  private def makeSource: File = {
    val srcDir = new File("layout-example-input")
    srcDir.mkdir()

    val numDisconnected = 128
    val maxLevel = 2
    val connectedNodesPerLevel = 7
    val maxConnectedNodeId = math.pow(connectedNodesPerLevel, 1 + maxLevel).round.toInt
    for (level <- 0 to maxLevel) {
      val levelDir = new File(srcDir, "level_" + level)
      levelDir.mkdir()
      val src = new File(levelDir, "part-00000")
      val srcStream = new PrintStream(new FileOutputStream(src))

      val idInc = math.pow(connectedNodesPerLevel, 1 + level).round.toInt
      val nodeSize = idInc / connectedNodesPerLevel

      // Write out connectedNodesPerLevel^(maxLevel-level) fully connected sub-clusters of connectedNodesPerLevel
      // nodes each
      for (i <- 0 until maxConnectedNodeId by idInc) {
        for (c <- 0 until connectedNodesPerLevel) {
          val id = i + c * idInc / connectedNodesPerLevel
          srcStream.println("node\t"+id+"\t"+i+"\t"+nodeSize+"\t"+nodeSize+"\t"+connectedNodeName(id, level, maxLevel, connectedNodesPerLevel))
        }
      }
      // Write out numDisconnected fully disconnected nodes
      for (i <- 0 until numDisconnected) {
        val id = maxConnectedNodeId + i
        srcStream.println("node\t"+id+"\t"+id+"\t"+1+"\t"+0+"\t"+s"disconnected node $i on hierarchy level $level")
      }

      // Connect nodes
      for (edgeLevel <- level to maxLevel) {
        val lvlInc = math.pow(connectedNodesPerLevel, 1 + edgeLevel).round.toInt
        for (i <- 0 until maxConnectedNodeId by lvlInc) {
          val vn = (0 until connectedNodesPerLevel).map(n => i + n*lvlInc / connectedNodesPerLevel)

          // self-connections from lower levels
          if (edgeLevel == level) {
            val selfWeight = levelWeight(level, connectedNodesPerLevel)
            if (selfWeight > 0) {
              for (i <- 0 until connectedNodesPerLevel)
                srcStream.println("edge\t"+vn(i)+"\t"+vn(i)+"\t"+selfWeight)
            }
          }

          // Connect up nodes on this level
          val internalWeight = 1
          for (i <- 0 until connectedNodesPerLevel - 1) {
            for (j <- i+1 until connectedNodesPerLevel) {
              srcStream.println("edge\t"+vn(i)+"\t"+vn(j)+"\t"+internalWeight)
            }
          }
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
    val sourcePath = sourceDir.getAbsolutePath
    val outputDir = getOutputLocation
    val outputPath = outputDir.getAbsolutePath
    val config = HierarchicalLayoutConfig(ConfigFactory.parseString(
      s"""
         |{
         |  layout: {
         |    input: { location = "$sourcePath" }
         |    output : { location = "$outputPath" }
         |    max-level : 2
         |  }
         |}
       """.stripMargin)).get
    val parameters = ForceDirectedLayoutParameters(ConfigFactory.parseString(
      s"""
         |{
         |  layout: {
         |    force-directed: {
         |      use-node-sizes: true
         |      node-area-factor: 0.6
         |    }
         |  }
         |}
       """.stripMargin)).get

    val fileStartTime = System.currentTimeMillis()

    // Hierarchical Force-Directed layout scheme
    val layouter = new HierarchicFDLayout()

    layouter.determineLayout(sc, config, parameters)

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
  val nodes: List[NodeEntry] = readNodeData()
  val edges: List[EdgeEntry] = readEdgeData()

  def readNodeData (): List[NodeEntry] = {
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
        fields(9).toInt, fields(10).toInt, fields(11).toInt, fields(12)
      )
      line = reader.readLine()
    }
    reader.close()

    bnodes.toList
  }

  def readEdgeData (): List[EdgeEntry] = {
    val dataDir = new File(outputPath, "level_"+level)
    val dataFile = new File(dataDir, "part-00001")
    val reader = new BufferedReader(new InputStreamReader(new FileInputStream(dataFile)))
    val bedges = MutableBuffer[EdgeEntry]()

    var line = reader.readLine()
    while (null != line) {
      val fields = line.split("\t")
      bedges += EdgeEntry(
        fields(1).toLong, fields(2).toDouble, fields(3).toDouble,
        fields(4).toLong, fields(5).toDouble, fields(6).toDouble,
        fields(7).toLong,
        1 == fields(8).toInt
      )
      line = reader.readLine()
    }
    reader.close()

    bedges.toList
  }

  override def paint (g: Graphics): Unit = {
    val size: Dimension = getSize
    def coords (x: Double, y: Double): (Int, Int) = {
      ((size.getWidth * x / 256.0).round.toInt, (size.getHeight* y / 256.0).round.toInt)
    }

    g.setFont(new Font("SansSerif", Font.BOLD, level * 2 + 10))
    val fontColor = new Color(0, 192, 0)

    def circle (color: Color, x: Double, y: Double, r: Double, fill: Boolean, idOpt: Option[Long] = None): Unit = {
      g.setColor(color)
      val (lx, ly) = coords(x, y)
      val (dxr, dyr) = coords(r, r)
      val dx = dxr max 1
      val dy = dyr max 1
      if (fill) g.fillOval(lx - dx, ly - dy, 2*dx+1, 2*dy+1)
      else g.drawOval(lx - dx, ly - dy, 2*dx+1, 2*dy+1)
      idOpt.foreach{id =>
        g.setColor(fontColor)
        g.drawString(""+id, lx, ly)
      }
    }

    g.setColor(Color.WHITE)
    g.fillRect(0, 0, size.getWidth.toInt, size.getHeight.toInt)

    // draw nodes
    for (node <- nodes) {
      circle(Color.BLUE, node.x, node.y, node.r, true, Some(node.id))
      circle(Color.RED, node.px, node.py, node.pr, false)
    }

    // draw edges
    val internalEdgeColor = new Color(0, 128, 128)
    val externalEdgeColor = new Color(128, 128, 0)
    for (edge <- edges) {
      val (sx, sy) = coords(edge.sx, edge.sy)
      val (dx, dy) = coords(edge.dx, edge.dy)
      if (edge.external) {
        g.setColor(externalEdgeColor)
        g.drawLine(sx, sy, dx, dy)
      } else {
        g.setColor(internalEdgeColor)
        g.drawLine(sx, sy, dx, dy)
      }
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

case class NodeEntry (id: Long, x: Double, y: Double, r: Double, pid:Long, px: Double, py: Double, pr: Double, internal: Int, degree: Int, level: Int, metadata: String)
case class EdgeEntry (sid: Long, sx: Double, sy: Double, did: Long, dx: Double, dy: Double, weight: Long, external: Boolean)
