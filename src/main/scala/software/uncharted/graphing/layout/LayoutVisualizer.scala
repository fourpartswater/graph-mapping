/**
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



import java.awt.event.{ActionEvent, ActionListener}
import java.awt.{Color, Graphics, GridBagConstraints, GridBagLayout, Insets}
import javax.swing._

import com.typesafe.config.Config
import grizzled.slf4j.Logging
import org.apache.spark.sql.SparkSession
import software.uncharted.graphing.layout.forcedirected._
import software.uncharted.xdata.sparkpipe.config.ConfigParser
import software.uncharted.xdata.sparkpipe.jobs.AbstractJob

import scala.io.Source
import scala.util.{Failure, Success, Try}



/**
  * An application to lay out a set of data, playing out the layout process visually for the user.
  */
object LayoutVisualizer extends AbstractJob with Logging {
  private def readLayoutParameters (config: Config): ForceDirectedLayoutParameters =
    ForceDirectedLayoutParametersParser.parse(config) match {
      case Success(p) => p
      case Failure(e) =>
        error("Error reading layout parameters", e)
        sys.exit(-1)
    }
  private def readDataParameters (config: Config): VisualLayoutDataConfig =
    VisualLayoutDataConfigParser.parse(config) match {
      case Success(p) => p
      case Failure(e) =>
        error("Error reading data parameters", e)
        sys.exit(-1)
    }
  private val pauseControl = new PauseControl



  override def execute(args: Array[String]): Unit = {
    // load properties file from supplied URI
    val config = readConfigArguments(args)

    if (debug) {
      debugConfig(config)
    }

    execute(null, config) // scalastyle:ignore
  }

  /**
    * This function actually executes the task the job describes
    *
    * @param session Empty spark session - we don't need spark here
    * @param config  The job configuration
    */
  override def execute(session: SparkSession, config: Config): Unit = {
    val layoutParameters = readLayoutParameters(config)
    val dataParameters = readDataParameters(config)

    val arranger = new ForceDirectedLayout(layoutParameters)

    val (parent, nodes) = readNodes(dataParameters)
    val edges = readEdges(dataParameters)

    val visualization = new LayoutVisualizer(pauseControl)

    startLayout(visualization, arranger, dataParameters, nodes, parent, edges)

    visualization.setVisible(true)
  }

  /*
   * Start the layout, but in another thread that can be paused/continued at will
   */
  private def startLayout (visualizer: LayoutVisualizer,
                           arranger: ForceDirectedLayout,
                           dataParameters: VisualLayoutDataConfig,
                           nodes: Iterable[GraphNode],
                           parentId: Long,
                           edges: Iterable[GraphEdge]): Unit = {
    val layoutThread = new Thread(new Runnable {
      override def run(): Unit = {
        arranger.setIterationCallback(Some(iterationCallback(visualizer)))
        arranger.setIsolatedNodeCallback(Some(isolatedNodeCallback(visualizer)))
        arranger.run(
          nodes, edges,
          parentId,
          dataParameters.bounds)
      }
    }, "Force-directed layout")
    layoutThread.setDaemon(true)
    layoutThread.start()
  }

  private def isolatedNodeCallback (visualizer: LayoutVisualizer): Iterable[LayoutNode] => Unit = {
    isolatedNodes => {
      val isolatedNodeArray = isolatedNodes.toArray
      SwingUtilities.invokeLater(new Runnable {
        override def run(): Unit = {
          visualizer.graphPanel.setIsolatedModel(isolatedNodeArray)
        }
      })
    }
  }
  private def iterationCallback (visualizer: LayoutVisualizer): (Array[LayoutNode], Iterable[LayoutEdge], Int, Double) => Unit = {
    (nodes, edges, iteration, temperature) => {
      // Copy data for visualization
      val nodesCopy = new Array[LayoutNode](nodes.length)
      for (n <- nodes.indices) nodesCopy(n) = LayoutNode.copy(nodes(n))
      val edgesCopy = edges.map(e => LayoutEdge.copy(e)).toArray
      val minG = nodesCopy.map(_.geometry.center).reduce(_ min _)
      val maxG = nodesCopy.map(_.geometry.center).reduce(_ max _)
      val n = nodesCopy.length
      val e = edges.size
      val infoText =
        f"""# of nodes: $n%s
           |# of edges: $e%s
           |minimum coordinate value: [${minG.x}%.4f, ${minG.y}%.4f]
           |maximum coordinate value: [${maxG.x}%.4f, ${maxG.y}%.4f]
           |temperature: ${temperature}%.4f
         """.stripMargin

      // Change the visualization next time we can
      SwingUtilities.invokeLater(new Runnable {
        override def run(): Unit = {
          visualizer.generalInfo.setText(infoText)
          visualizer.frameField.setText(s"$iteration")
          visualizer.graphPanel.setModel(nodesCopy, edgesCopy)
        }
      })
      pauseControl.pause()
    }
  }

  private def readNodes (config: VisualLayoutDataConfig): (Long, Iterable[GraphNode]) = {
    val separator = "\t"
    val rawNodes = Source.fromFile(config.source).getLines().filter(_.startsWith("node")).map { line =>
      val fields = line.split(separator).map(_.trim)
      val id = fields(1).toLong
      val parentID = fields(2).toLong
      val internalNodes = fields(3).toLong
      val degree = fields(4).toInt
      val metaData = fields.drop(5).mkString(separator)
      GraphNode(id, parentID, internalNodes, degree, metaData)
    }.toSeq
    val zeroNode = new GraphNode(-1L, -1L, -1L, -1, "")
    val maxNode = rawNodes.fold(zeroNode)((a, b) =>
      if (a.internalNodes >= b.internalNodes) {
        a
      } else {
        b
      }
    )
    (
      maxNode.id,
      rawNodes.map(n =>
        GraphNode(n.id, maxNode.id, n.internalNodes, n.degree, n.metadata)
      )
    )
  }

  private def readEdges (config: VisualLayoutDataConfig): Iterable[GraphEdge] = {
    val separator = "\t"
    Source.fromFile(config.source).getLines().filter(_.startsWith("edge")).map { line =>
      val fields = line.split(separator).map(_.trim)
      val srcId = fields(1).toLong
      val dstId = fields(2).toLong
      val weight = fields(3).toLong
      GraphEdge(srcId, dstId, weight)
    }.toSeq
  }
}

/**
  * A class that allows a pause signal
  */
class PauseControl {
  private val lock = new Object

  /** Pause until the release method is called */
  def pause(): Unit = {
    lock.synchronized( lock.wait() )
  }
  /** Release any currently paused threads */
  def release(): Unit = {
    lock.synchronized( lock.notifyAll())
  }
}

class LayoutVisualizer (pauseControl: PauseControl) extends JFrame {
  // Initialize visuals
  setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE)
  val graphPanel = new GraphPanel
  val controlPanel = new JPanel
  controlPanel.setLayout(new GridBagLayout)
  val frameField = new JLabel
  val step = new JButton(new AbstractAction("Step forward one iteration") {
    override def actionPerformed(actionEvent: ActionEvent): Unit = {
      pauseControl.release()
    }
  })
  var speed = 600
  var timer: Option[Timer] = None
  val speedField = new JTextField(s"$speed")
  private val MILLI_PER_MINUTE = 60000
  val playPause: JButton = new JButton(new AbstractAction("Step forward continuously") {
    override def actionPerformed(actionEvent: ActionEvent): Unit = {
      if (timer.isDefined) {
        playPause.setText("Step forward continuously")
        step.setEnabled(true)
        timer.get.stop()
        timer = None
      } else {
        playPause.setText("Pause continuous stepping")
        step.setEnabled(false)
        speed = Try(speedField.getText.trim.toInt).getOrElse(30)
        timer = Some(new Timer(MILLI_PER_MINUTE/speed, new ActionListener {
          override def actionPerformed(actionEvent: ActionEvent): Unit = {
            pauseControl.release()
          }
        }))
        timer.get.start()
      }
    }
  })
  val drawChoice: JButton = new JButton(new AbstractAction("Show penultimate nodes") {
    override def actionPerformed(actionEvent: ActionEvent): Unit = {
      if (graphPanel.showPenultimateNodes) {
        graphPanel.showPenultimateNodes = false
        drawChoice.setText("Show penultimate nodes")
      } else {
        graphPanel.showPenultimateNodes = true
        drawChoice.setText("Show ultimate nodes")
      }
      graphPanel.invalidate()
      graphPanel.repaint()
    }
  })
  val exit = new JButton(new AbstractAction("quit") {
    override def actionPerformed(actionEvent: ActionEvent): Unit = {
      sys.exit(0)
    }
  })
  val generalInfo = new JTextArea("Display layout in action")
  generalInfo.setEnabled(false)

  private val helpText = "Display layout in action"
  controlPanel.add(new JLabel("frame:"),                new GBC(0, 0, anchor = GridBagConstraints.EAST))
  controlPanel.add(frameField,                          new GBC(1, 0, width=2, anchor = GridBagConstraints.WEST, xWeight = 1.0))
  controlPanel.add(new JLabel("playback speed (f/m):"), new GBC(0, 1, anchor = GridBagConstraints.EAST))
  controlPanel.add(speedField,                          new GBC(1, 1, width=2, anchor = GridBagConstraints.WEST, xWeight = 1.0))
  controlPanel.add(step,                                new GBC(0, 2, width=3))
  controlPanel.add(playPause,                           new GBC(0, 3, width=3))
  controlPanel.add(drawChoice,                          new GBC(0, 4, width=3))
  controlPanel.add(generalInfo,                         new GBC(0, 5, width=3, height=2, yWeight=1.0, anchor = GridBagConstraints.NORTHWEST, fill=GridBagConstraints.HORIZONTAL))
  controlPanel.add(exit,                                new GBC(2, 6, anchor=GridBagConstraints.SOUTHEAST, fill = GridBagConstraints.NONE))
  val split = new JSplitPane
  split.setLeftComponent(graphPanel)
  split.setRightComponent(controlPanel)
  setContentPane(split)
  setSize(1600, 1200)                                    // scalastyle:ignore
  setLocation(100, 100)                                 // scalastyle:ignore

  override def setVisible(b: Boolean): Unit = {
    super.setVisible(b)
    split.setDividerLocation(0.75)
  }
}

class GraphPanel extends JPanel {
  private[layout] var showPenultimateNodes = false
  private var isolatedNodeModel: Array[LayoutNode] = new Array[LayoutNode](0)
  private var nodeModel:         Array[LayoutNode] = new Array[LayoutNode](0)
  private var lastNodeModel:     Array[LayoutNode] = new Array[LayoutNode](0)
  private var edgeModel:         Array[LayoutEdge] = new Array[LayoutEdge](0)
  def setIsolatedModel (nodes: Array[LayoutNode]): Unit =
    isolatedNodeModel = nodes
  def setModel (nodes: Array[LayoutNode], edges: Array[LayoutEdge]): Unit = {
    lastNodeModel = nodeModel
    nodeModel = nodes
    edgeModel = edges
    invalidate()
    repaint()
  }

  private def getNodes = if (showPenultimateNodes) {
    lastNodeModel
  } else {
    nodeModel
  }
  private def getGeo (node: LayoutNode) = {
    val latest = node.geometry
    //        val penultimate = lastNodeModel.get(node.id).map(_.geometry.center).getOrElse(latest.center)
    //        Circle((latest.center + penultimate) / 2.0, latest.radius)
    latest
  }
  private def paintEdges (g: Graphics, cx: Int, cy: Int, scale: Double): Unit = {
    if (!edgeModel.isEmpty && !getNodes.isEmpty) {
      g.setColor(Color.green.darker().darker())
      for (edge <- edgeModel) {
        val srcGeo = getGeo(getNodes(edge.srcIndex))
        val dstGeo = getGeo(getNodes(edge.dstIndex))
        val sx = cx + ((srcGeo.center.x - 128.0) * scale).round.toInt
        val sy = cy + ((srcGeo.center.y - 128.0) * scale).round.toInt
        val dx = cx + ((dstGeo.center.x - 128.0) * scale).round.toInt
        val dy = cy + ((dstGeo.center.y - 128.0) * scale).round.toInt
        g.drawLine(sx, sy, dx, dy)
      }
    }
  }

  private def paintConnectedCommunities (g: Graphics, cx: Int, cy: Int, scale: Double): Unit = {
    if (!getNodes.isEmpty) {
      g.setColor(Color.LIGHT_GRAY)
      for (node <- getNodes) {
        val geo = getGeo(node)
        val x = cx + ((geo.center.x - 128.0) * scale).round.toInt
        val y = cy + ((geo.center.y - 128.0) * scale).round.toInt
        val r = (geo.radius * scale).round.toInt max 2
        g.drawOval(x - r, y - r, 2 * r + 1, 2 * r + 1)
        // g.drawString(s"${node.id}", x, y)
      }
    }
  }

  private def paintIsolatedCommunities (g: Graphics, cx: Int, cy: Int, scale: Double): Unit = {
    if (!isolatedNodeModel.isEmpty) {
      // Draw communities on top of them
      g.setColor(Color.DARK_GRAY)
      for (node <- isolatedNodeModel) {
        val geo = getGeo(node)
        val x = cx + ((geo.center.x - 128.0) * scale).round.toInt
        val y = cy + ((geo.center.y - 128.0) * scale).round.toInt
        val r = (geo.radius * scale).round.toInt max 2
        g.drawOval(x - r, y - r, 2 * r + 1, 2 * r + 1)
      }
    }
  }

  override def paint(g: Graphics): Unit = {
    g.setColor(Color.BLACK)
    val s = getSize()
    g.fillRect(0, 0, s.width, s.height)

    val cx = s.width / 2
    val cy = s.height / 2
    val scale = 2.0

    paintEdges(g, cx, cy, scale)
    paintConnectedCommunities(g, cx, cy, scale)
    paintIsolatedCommunities(g, cx, cy, scale)
  }
}

class GBC (x: Int,
           y: Int,
           width: Int = 1,
           height: Int = 1,
           xWeight: Double = 0.0,
           yWeight: Double = 0.0,
           anchor: Int = GridBagConstraints.CENTER,
           fill: Int = GridBagConstraints.BOTH,
           insets: Insets = new Insets(0, 0, 0, 0),
           xPad: Int = 0,
           yPad: Int = 0)
  extends GridBagConstraints(x, y, width, height, xWeight, yWeight, anchor, fill, insets, xPad, yPad)





case class VisualLayoutDataConfig (source: String, level: Int, /* parentId: Long, */ bounds: Circle)
object VisualLayoutDataConfigParser extends ConfigParser {
  private val SECTION_KEY = "data"
  private val SOURCE_KEY = "source"
  private val LEVEL_KEY = "level"
//  private val PARENT_KEY = "parent"
  private val BOUNDS_KEY = "bounds"
  private val CENTER_KEY = "center"
  private val RADIUS_KEY = "radius"
  def parse (config: Config): Try[VisualLayoutDataConfig] = {
    Try {
      val dataConfig = config.getConfig(SECTION_KEY)
      val source = dataConfig.getString(SOURCE_KEY)
      val level = dataConfig.getInt(LEVEL_KEY)
//      val parentId = dataConfig.getLong(PARENT_KEY)
      val boundsSection = dataConfig.getConfig(BOUNDS_KEY)
      val center = boundsSection.getDoubleList(CENTER_KEY)
      val radius = boundsSection.getDouble(RADIUS_KEY)

      VisualLayoutDataConfig(source, level, /* parentId, */ Circle(V2(center.get(0), center.get(1)), radius))
    }
  }
}
