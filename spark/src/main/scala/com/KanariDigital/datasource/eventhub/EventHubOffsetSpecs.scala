/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.datasource.eventhub

import scala.collection.mutable.ArrayBuffer
import scala.util.{Failure, Success, Try}

import java.io.InputStream

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.Path


import org.apache.spark.eventhubs.NameAndPartition
import org.apache.spark.eventhubs.EventPosition

import com.kanaridi.common.util.HdfsUtils
import com.KanariDigital.JsonConfiguration.JsonConfig

/** Class to record offsets in HDFS and to also get the latest eventhub offsets
  * from the hdfs
  */
class EventHubOffsetSpecs(val appName: String, val storeOffsetSpecsLocation: String)
    extends Serializable {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  val storeOffsetSpecsPath = new Path(storeOffsetSpecsLocation)

  /** get the last (latest) recorded eventhub offsets from hdfs
    * @return {{ Map[NameAndPartition, EventPosition] }}
    */
  def getStoredOffsetsMap(): Map[NameAndPartition, EventPosition] = {

    val offsetBuffer = getStoredOffsets()

    var offsetMap:Map[NameAndPartition, EventPosition] = Map.empty[NameAndPartition, EventPosition]

    for (offsetItem <- offsetBuffer) {

      val nameAndPartition: NameAndPartition = offsetItem.offsetRange.nameAndPartition

      val eventPosition: EventPosition = EventPosition.fromSequenceNumber(offsetItem.offsetRange.untilSeqNo)

      offsetMap += (nameAndPartition -> eventPosition)

    }

    offsetMap
  }

  /** get the last (latest) recorded eventhub offsets from hdfs
    * @return array of {{EventHubOffsetSpec}}
    */
  def getStoredOffsets(): ArrayBuffer[EventHubOffsetSpec] = {

    var eventHubOffsetSpecBuffer: ArrayBuffer[EventHubOffsetSpec] = ArrayBuffer.empty[EventHubOffsetSpec]

    try {

      val fileList = HdfsUtils.listFiles(storeOffsetSpecsLocation, "offsets_*.txt")

      if (fileList.size > 0) {

        fileList.foreach(tup => log.info("found offset file: {" + tup.toString + "}"))

        // get latest file in the folder
        val latestOffsetFile = fileList.head
        log.info("going to read LATEST offset file: " + latestOffsetFile)

        val fileContents = HdfsUtils.readFileContents(fileList.head)

        for (line <- fileContents) {
          val eventHubOffsetSpec = EventHubOffsetSpec.jsonSpecToEventHubOffsetSpec(line)
          eventHubOffsetSpecBuffer += eventHubOffsetSpec
        }

        eventHubOffsetSpecBuffer.foreach(spec => log.info(s"file: $latestOffsetFile, spec: {" + spec.toString + "}"))

      }

    } catch {

      case e: Exception => {
        log.info(s"Error reading offsets at: $storeOffsetSpecsPath. Message: " + e.getMessage)
        throw e
      }

    }
    eventHubOffsetSpecBuffer
  }

  /** manages the number of offset files that are stored in hdfs
    * If the number of files exceeds {{maxFiles}} older files
    * are moved to history folder.
    * If the number of files in the history folder exceeds  {{maxHistoryFiles}}
    * then older files are removed from hdfs
    * @param maxFiles - maximum number of offset files
    * @param maxHistoryFiles - maximum number of offset files in history folder
    */
  private def manageOffsetFiles(maxFiles: Int, maxHistoryFiles: Int): Unit = {

    try {

      val offsetFiles = HdfsUtils.listFiles(storeOffsetSpecsLocation, "offsets_*.txt")

      log.info("EventHub: manageOffsetFiles: offsetFiles: "
        + offsetFiles.map(tup => log.info("offsetFile: {" + tup.toString + "}")))

      //move files, keep files around
      val candidateFiles = offsetFiles.slice(maxFiles, offsetFiles.length)

      log.info("EventHub: manageOffsetFiles: move to history folder: "
        + offsetFiles.map(tup => log.info("move to history file: {" + tup.toString + "}")))

      HdfsUtils.moveFiles(candidateFiles, s"$storeOffsetSpecsLocation/history/")

      //delete any files that are over maxHistoryFiles limit
      val historyOffsetFiles = HdfsUtils.listFiles(s"$storeOffsetSpecsLocation/history/", "offsets_*.txt")
      val oldHistoryFiles = historyOffsetFiles.slice(maxHistoryFiles, historyOffsetFiles.length)
      log.info("EventHub: manageOffsetFiles: going to remove: "
        + oldHistoryFiles.map(tup => log.info("remove old history file: {" + tup.toString + "}")))
      HdfsUtils.removeFiles(oldHistoryFiles, s"$storeOffsetSpecsLocation/history/")


    } catch {

      case e: Exception => {
        log.info(s"Error in manageOffsetFiles. Message: $e.getMessage")
        throw e
      }

    }
  }

  /** store offsets in hdfs at location specified by {{storeOffsetSpecsLocation}}
    * @param offsetSpecArray - array of {{EventHubOffsetSpec}}
    * @param maxFiles - maximum number of offset files
    * @param maxHistoryFiles - maximum number of offset files in history folder
    */
  def storeOffsets(
    offsetSpecArray: ArrayBuffer[EventHubOffsetSpec],
    maxFiles: Int,
    maxHistoryFiles: Int) = {

    try {

      val contentArray: ArrayBuffer[String] =
        offsetSpecArray.map(offsetSpec => (EventHubOffsetSpec.toJSONString(offsetSpec)))

      val batchTimeStr = offsetSpecArray(0).batchTime.replace(":", "")

      // store offset file
      val storeOffsetLoc = s"$storeOffsetSpecsLocation/offsets_$batchTimeStr.txt"

      HdfsUtils.saveFileContents(storeOffsetLoc, contentArray)

      //manage offset files
      manageOffsetFiles(maxFiles, maxHistoryFiles)

    } catch {

        case e: Exception => {
          log.info(s"Error storing offsets at: $storeOffsetSpecsPath. Message: $e.getMessage")
          throw e
      }
    }
  }

  /** check of the start offset matches end offset of last saved offset specs
    * for all topics and partitions to prevent offsets from being skipped over.
    * 
    * @param savedOffsetSpecArray last saved offsets
    * @param newOffsetSpecArray new offsets to be saved
    * 
    * @returns true if check is successful

    */
  def compareOffsetsBeforeSave(
    savedOffsetSpecs: ArrayBuffer[EventHubOffsetSpec],
    newOffsetSpecs: ArrayBuffer[EventHubOffsetSpec]): Map[NameAndPartition, String] = {

    try {

      var newOffsetMap:Map[NameAndPartition, EventPosition] = Map.empty[NameAndPartition, EventPosition]
      for (newOffsetItem <- newOffsetSpecs) {
        val nameAndPartition: NameAndPartition = newOffsetItem.offsetRange.nameAndPartition
        // get starting offset
        val eventPosition: EventPosition = EventPosition.fromSequenceNumber(newOffsetItem.offsetRange.fromSeqNo)
        newOffsetMap += (nameAndPartition -> eventPosition)
      }

      var savedOffsetMap:Map[NameAndPartition, EventPosition] = Map.empty[NameAndPartition, EventPosition]
      for (savedOffsetItem <- savedOffsetSpecs) {
        val nameAndPartition: NameAndPartition = savedOffsetItem.offsetRange.nameAndPartition
        //get the ending offset
        val eventPosition: EventPosition = EventPosition.fromSequenceNumber(savedOffsetItem.offsetRange.untilSeqNo)
        savedOffsetMap += (nameAndPartition -> eventPosition)
      }

      var diffMap:Map[NameAndPartition, String] = Map.empty[NameAndPartition, String]

      for ((nameAndPartition, eventPosition) <- newOffsetMap) {

        val lastSavedOption = savedOffsetMap.get(nameAndPartition)

        lastSavedOption match {
          case Some(lastSavedEventPosition) => {
            if (eventPosition == lastSavedEventPosition) {
              log.info(s"$nameAndPartition: newOffset: {$eventPosition} compare to lastSavedOffset: {$lastSavedEventPosition}: MATCHES..ok")
            } else {
              log.info(s"$nameAndPartition: newOffset: {$eventPosition} compare to lastSavedOffset: {$lastSavedEventPosition}: NO MATCH!!")
              diffMap += (nameAndPartition -> s"$eventPosition != $lastSavedEventPosition")
            }
          }
          case None => {
            log.info(s"$nameAndPartition: newOffset: {$eventPosition} compare to lastSavedOffset: {None}: SAVED OFFSET NOT FOUND..ok")
          }
        }
      }

      diffMap

    } catch {
      case e: Exception => {
        log.info(s"Error comparing offsets: $e.getMessage")
        throw e
      }
    }
  }
}
