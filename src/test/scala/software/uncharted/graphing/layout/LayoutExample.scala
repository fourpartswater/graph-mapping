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

import java.io.{File, FileOutputStream, PrintStream}

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

    val numDisconnected = 32
    val maxLevel = 2
    val maxNodeId = 4 << (2 * maxLevel)
    for (level <- 0 to maxLevel) {
      val levelDir = new File(srcDir, "level_" + level)
      levelDir.mkdir()
      val src = new File(levelDir, "part-00000")
      val srcStream = new PrintStream(new FileOutputStream(src))

      val idInc = 4 << (2 * level)
      // Write out 4^(maxLevel-level) fully connected sub-clusters of 4 nodes each
      for (i <- 0 until maxNodeId by idInc) {
        for (c <- 0 until 4) {
          val id = i + c * idInc / 4
          srcStream.println("node\t"+id+"\t"+id+"\t"+idInc+"\t"+idInc+"\t"+connectedNodeName(id, level, maxLevel))
        }
      }
      // Write out numDisconnected fully disconnected nodes
      for (i <- 0 until numDisconnected) {
        val id = maxNodeId + i
        srcStream.println("node\t"+id+"\t"+id+"\t"+1+"\t"+1+"\t"+s"disconnected node $i on hierarchy level $level")
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
  private def makeOutputLocation: File = {
    val outDir = new File("layout-example-output")
    outDir.mkdir()
    outDir
  }
  test("Example of layout application, to be used to debug through the process") {
    val sourceDir = makeSource
    val outputDir = makeOutputLocation
    val partitions = 0
    val consolidationPartitions = 0
    val dataDelimiter = "\t"
    val maxIterations = 500
    val maxHierarchyLevel = 0
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
