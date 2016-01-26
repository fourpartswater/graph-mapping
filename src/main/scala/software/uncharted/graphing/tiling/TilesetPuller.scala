package software.uncharted.graphing.tiling

import java.io.{File, FileOutputStream}
import java.util.zip.{ZipEntry, ZipOutputStream}

import com.oculusinfo.binning.TileIndex
import com.oculusinfo.binning.io.impl.HBasePyramidIO
import com.oculusinfo.tilegen.util.ArgumentParser
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration}
import org.apache.hadoop.hbase.client.ConnectionFactory

/**
 *  Pull a tile set from HBase, stick it in a .zip file
 */
object TilesetPuller {
  def main (args: Array[String]): Unit = {
    val argParser = new ArgumentParser(args)
    val hbaseConfig = argParser.getStringPropSeq("config", "config files for HBase.  Can be entered multiple times for multiple files")
    val tableName = argParser.getString("table", "table to copy")
    val file = argParser.getString("file", "The zip file to which to write it")

    val config = HBaseConfiguration.create
    hbaseConfig.foreach{configFile =>
      val file = new File(configFile)
      config.addResource(new Path(file.toURI.toString))
      config.addResource(configFile)
    }

    val connection = ConnectionFactory.createConnection(config)
    val admin = connection.getAdmin

    val table = connection.getTable(TableName.valueOf(tableName))
    val column = "tileData".getBytes()
    val empty = new Array[Byte](0)

    val scanner = table.getScanner(column, empty)

    val zipper = new ZipOutputStream(new FileOutputStream(file))

    var rows = scanner.next(1000)
    var rowsWritten = 0
    while (rows.length != 0) {
      rows.foreach{row =>
        val rowId = new String(row.getRow())
        val data = row.getValue(column, empty)
        val index = HBasePyramidIO.tileIndexFromRowId(rowId)
        val zipEntry = new ZipEntry(index.getLevel+"/"+index.getY+"/"+index.getX+".bins")
        zipper.putNextEntry(zipEntry)
        zipper.write(data, 0, data.length)
        zipper.closeEntry()
        rowsWritten += 1
      }
      rows = scanner.next(1000)
    }

    println("Wrote "+rowsWritten+" tiles")

    zipper.flush()
    zipper.close()
    admin.close()
    connection.close()
  }
}
