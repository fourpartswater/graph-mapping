package software.uncharted.graphing.tiling

import com.oculusinfo.tilegen.util.ArgumentParser
import org.apache.log4j.{Level, Logger}
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext

/**
 * A pipeline application to create the text layers used when displaying graphs.
 */
object TextTilingPipelineApp {
  def main (args: Array[String]): Unit = {
    // Reduce log clutter
    Logger.getRootLogger.setLevel(Level.WARN)

    val argParser = new ArgumentParser(args)

    val sc = new SparkContext()
    val sqlc = new SQLContext(sc)

    try {
    } catch {
      case e: Exception => {
        e.printStackTrace()
        argParser.usage
      }
    }
  }
}
