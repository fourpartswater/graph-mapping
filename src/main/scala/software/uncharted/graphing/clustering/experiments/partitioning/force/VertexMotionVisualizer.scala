package software.uncharted.graphing.clustering.experiments.partitioning.force


import java.awt.event.ActionEvent
import java.awt.{FlowLayout, BorderLayout, Color, Graphics}
import javax.swing._

import org.apache.log4j.{Level, Logger}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.graphx.{Graph, Edge, VertexId}



/**
  * Created by nkronenfeld on 2016-01-17.
  */
class VertexMotionVisualizer[VD, ED] (edgeWeightFcn: ED => Double) extends JFrame {
  val panel = new VertexMotionPanel
  var rawData = Array[(VertexId, (VD, Vector, Double))]()
  setLayout(new BorderLayout())
  add(panel, BorderLayout.CENTER)
  setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE)

  val graphProcessor = new GraphProcessor
  val graphAdvancer = new Thread(graphProcessor, "Run force-directed physics on graph")
  graphAdvancer.setDaemon(true)
  graphAdvancer.start()
  val iterateButton = new JButton(new AbstractAction("next iteration"){
    override def actionPerformed(e: ActionEvent): Unit = {
      graphProcessor.moving = !graphProcessor.moving
      updateIterateButton
    }
  })
  val iterationsDisplay = new JLabel("")
  def updateIterateButton: Unit = {
    if (graphProcessor.moving) iterateButton.setText("Stop moving")
    else iterateButton.setText("Start moving")
    iterationsDisplay.setText("Iterations: "+graphProcessor.iterations)
  }

  setupButtonPanel

  def setupButtonPanel: Unit = {
    val buttonPanel = new JPanel()
    buttonPanel.setLayout(new FlowLayout())
    val pointCheckbox = new JCheckBox(new AbstractAction("points"){
      override def actionPerformed(e: ActionEvent): Unit = {
        panel.usePoints = !panel.usePoints
        panel.validate()
        panel.invalidate()
        panel.repaint()
        debugOpportunity
      }
    })
    pointCheckbox.setSelected(panel.usePoints)
    val edgeCheckbox = new JCheckBox(new AbstractAction("edges") {
      override def actionPerformed(e: ActionEvent): Unit = {
        panel.useEdges = !panel.useEdges
        panel.validate()
        panel.invalidate()
        panel.repaint()
        debugOpportunity
      }
    })
    edgeCheckbox.setSelected(panel.useEdges)
    val heatmapCheckbox = new JCheckBox(new AbstractAction("heatmap") {
      override def actionPerformed(e: ActionEvent): Unit = {
        panel.useHeatmap = !panel.useHeatmap
        panel.validate()
        panel.invalidate()
        panel.repaint()
        debugOpportunity
      }
    })
    updateIterateButton
    heatmapCheckbox.setSelected(panel.usePoints)
    buttonPanel.add(pointCheckbox)
    buttonPanel.add(edgeCheckbox)
    buttonPanel.add(heatmapCheckbox)
    buttonPanel.add(iterateButton)
    buttonPanel.add(iterationsDisplay)
    add(buttonPanel, BorderLayout.NORTH)
  }

  def setGraph (g: Graph[(VD, Vector, Double), ED]): Unit = {
    graphProcessor.graph = g

    // Set point model
    panel.pointModel = g.vertices.collect.map{p =>
      (p._1, (p._2._2, p._2._3))
    }.toMap

    // Set edge model
    panel.edgeModel = g.triplets.map(trip =>
      (trip.srcAttr._2, trip.dstAttr._2)
    ).collect

    // Set heatmap model
    panel.heatmapModel = g.vertices.flatMap { case (id, data) =>
      val (originalData, coordinates, weight) = data
      val x = ((coordinates(0) + 1.0) * 100.0).round.toInt
      val y = ((coordinates(1) + 1.0) * 100.0).round.toInt
      if (0 <= x && x < 300 && 0 <= y && y < 300) {
        Some(((x, y), weight))
      } else {
        None
      }
    }.reduceByKey(_ + _).collect

    rawData = g.vertices.collect

    panel.invalidate()
    panel.validate()
    panel.repaint()
  }

  def debugOpportunity: Unit = {
    println("Chance to debug now")
  }

  class GraphProcessor extends Runnable {
    @volatile var moving = false
    @volatile var iterations = 0
    var graph: Graph[(VD, Vector, Double), ED] = null
    override def run(): Unit = {
      while (true) {
        if (moving) {
          val g = ForceDirectedGraphUtilities.forceDirectedMotion(graph, 2, edgeWeightFcn)
          iterations = iterations + 1
          graph.unpersist(false)
          g.cache
          setGraph(g)
          iterationsDisplay.setText("Iterations: "+iterations)

          if (0 == (iterations%10)) {
            g.checkpoint()
          }
        }
        Thread.sleep(250)
      }
    }
  }
}



object VertexMotionVisualizer {
  def main (args: Array[String]): Unit = {
    Logger.getRootLogger.setLevel(Level.WARN)
    val sc = new SparkContext(new SparkConf().setMaster("local").setAppName("test"))
    sc.setCheckpointDir("checkpoints")
    val vertices = sc.parallelize((0L to 15L).map(n => (n, n)))
    val edges = sc.parallelize(Seq(
      (0, 1), (0, 2), (0, 3), (1, 0), (1, 2), (1, 3), (2, 0), (2, 1), (2, 3), (3, 0), (3, 1), (3, 2),
      (4, 5), (4, 6), (4, 7), (5, 4), (5, 6), (5, 7), (6, 4), (6, 5), (6, 7), (7, 4), (7, 5), (7, 6),
      (8, 9), (8, 10), (8, 11), (9, 8), (9, 10), (9, 11), (10, 8), (10, 9), (10, 11), (11, 8), (11, 9), (11, 10),
      (12, 13), (12, 14), (12, 15), (13, 12), (13, 14), (13, 15), (14, 12), (14, 13), (14, 15), (15, 12), (15, 13), (15, 14),
      (0, 12), (12, 0), (2, 6), (6, 2), (4, 8), (8, 4), (10, 14), (14, 10),
      (1, 7), (7, 1), (3, 13), (13, 3), (5, 11), (11, 5), (9, 15), (15, 9)
    )).map{case (src, dst) => new Edge[Double](src, dst, 1.0)}
    val graph = Graph(vertices, edges)

    import ForceDirectedGraphUtilities._
    var g = addForceDirectedInfo(graph, 2, (d: Double) => d)

    val frame = new VertexMotionVisualizer[Long, Double]((d: Double) => d)
    frame.setSize(950, 1000)
    frame.setLocation(500, 25)
    frame.setVisible(true)
    frame.setGraph(g)
  }
}

class VertexMotionPanel extends JPanel {
  var pointModel = Map[VertexId, (Vector, Double)]()
  var edgeModel = Array[(Vector, Vector)]()
  var heatmapModel = Array[((Int, Int), Double)]()
  var usePoints = true
  var useEdges = true
  var useHeatmap = false

  override def paint (g: Graphics): Unit = {
    val width = getSize().getWidth.round.toInt
    val height = getSize().getHeight.round.toInt

    // Draw the background
    g.setColor(Color.BLACK)
    g.fillRect(0, 0, width, height)

    drawAxes(g)
    if (useHeatmap) drawHeatmap(g)
    if (useEdges) drawEdges(g)
    if (usePoints) drawPoints(g)
  }

  def mapping (width: Int, height: Int)(v: Vector): (Int, Int) = {
    val x = (v(0) + 1.0) * width / 3
    val y = (2.0 - v(1)) * height / 3
    (x.round.toInt, y.round.toInt)
  }

  def drawLine (g: Graphics, width: Int, height: Int)(x0: Double, y0: Double, x1: Double, y1: Double): Unit = {
    val m = mapping(width, height)(_)

    val (sx, sy) = m(new Vector(List(x0, y0)))
    val (ex, ey) = m(new Vector(List(x1, y1)))
    g.drawLine(sx, sy, ex, ey)
  }

  def drawCell (g: Graphics, width: Int, height: Int, xBins: Int, yBins: Int, maxWeight: Double)
               (x: Int, y: Int, weight: Double): Unit = {
    val minX = ((x.toDouble * width) / xBins).round.toInt
    val maxX = (((x+1).toDouble * width) / xBins).round.toInt - 1
    val maxY = height - ((y.toDouble * height) / yBins).round.toInt
    val minY = height - ((((y + 1).toDouble * height) / yBins).round.toInt - 1)

    val color = new Color(Color.HSBtoRGB((weight / maxWeight).toFloat, 0.75f, 0.75f))
    g.setColor(color)
    g.fillRect(minX, minY, maxX-minX, maxY-minY)
  }

  def drawPoint (g: Graphics, width: Int, height: Int)(id: Option[String], v: Vector, weight: Double): Unit = {
    val m = mapping(width, height)(_)

    val (x, y) = m(v)
    g.setColor(new Color(Color.HSBtoRGB(weight.toFloat, 0.75f, 1.0f)))
    g.fillArc(x, y, 5, 5, 0, 360)
    g.setColor(Color.WHITE)
    id.foreach(text =>
      g.drawString(text, x, y)
    )
  }

  def drawAxes (g: Graphics): Unit = {
    val width = getSize().getWidth.round.toInt
    val height = getSize().getHeight.round.toInt

    // Some simple drawing helper functions
    val line = drawLine(g, width, height)(_, _, _, _)

    // Draw the bounds of the unit square
    g.setColor(Color.WHITE)
    line(-1.0, 0.0, 2.0, 0.0)
    line(-1.0, 1.0, 2.0, 1.0)
    line(0.0, -1.0, 0.0, 2.0)
    line(1.0, -1.0, 1.0, 2.0)

  }

  def drawHeatmap (g: Graphics): Unit = {
    val width = getSize().getWidth.round.toInt
    val height = getSize().getHeight.round.toInt
    val maxWeight = heatmapModel.map(_._2).fold(0.0)(_ max _)
    val cell = drawCell(g, width, height, 300, 300, maxWeight)(_, _, _)

    heatmapModel.foreach{case ((x, y), weight) =>
      cell(x, y, weight)
    }
  }

  def drawPoints (g: Graphics): Unit = {
    val width = getSize().getWidth.round.toInt
    val height = getSize().getHeight.round.toInt

    // Some simple drawing helper functions
    val point = drawPoint(g, width, height)(_, _, _)

    // And, finally, draw our points
    pointModel.foreach { case (id, data) =>
      val (position, weight) = data
      point(Some(id.toString), position, weight)
    }
  }

  def drawEdges (g: Graphics): Unit = {
    val width = getSize().getWidth.round.toInt
    val height = getSize().getHeight.round.toInt

    val line = drawLine(g, width, height)(_, _, _, _)

    g.setColor(Color.LIGHT_GRAY)
    edgeModel.foreach { edge =>
      line (edge._1(0), edge._1(1), edge._2(0), edge._2(1))
    }
  }
}
