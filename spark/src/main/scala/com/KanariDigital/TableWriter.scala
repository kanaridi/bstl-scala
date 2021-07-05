/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.hbase

import java.util

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.hbase.client.{Put, Table, Get, Result}
import org.apache.hadoop.hbase.util.Bytes
import org.apache.hadoop.hbase.Cell
import org.apache.hadoop.hbase.CellUtil

import scala.collection.JavaConverters._
import scala.collection.mutable.ArrayBuffer
import scala.collection.mutable.Buffer
import scala.collection.mutable.HashMap
import scala.collection.JavaConversions.asScalaBuffer

// see https://github.com/cberez/write-to-hbase-example/blob/master/src/main/scala/com/cberez/scala/hbase/HBaseWriter.scala
class TableWriter(table : Table) extends Serializable {
  val log: Log = LogFactory.getLog(this.getClass.getName)

  var enableDedup: String = ""
  var dedupColFamily: String = ""
  var dedupColName: String = ""
  var dedupLookback: String = ""

  var tableName: String = table.getName.getNameWithNamespaceInclAsString

  var currentBatchRowKeyDedupKeyMap = scala.collection.mutable.HashMap.empty[String, String]

  var putList : ArrayBuffer[Put] = ArrayBuffer.empty[Put]

  def insertLines(rk: String, cf: String, content: Map[String, String], dedupKey: String): Int = {

    log.trace("Writing multiple lines to table")
    val puts : List[Put] = content.map(x => new Put(Bytes.toBytes(rk)).addColumn(Bytes.toBytes(cf), Bytes.toBytes(x._1), Bytes.toBytes(x._2))).toList
    putList ++= puts

    //add dedup key to the map
    if (enableDedup == "true") {

      // if dedupKey is empty or if we have seen the rk before, skip it
      if (dedupKey != ""
        && !(currentBatchRowKeyDedupKeyMap contains rk)) {

        //add dedupKey for insert into a specified columnfamily:colname
        putList += new Put(Bytes.toBytes(rk)).addColumn(Bytes.toBytes(dedupColFamily), Bytes.toBytes(dedupColName), Bytes.toBytes(dedupKey))

        //add rowkey-dedup key
        currentBatchRowKeyDedupKeyMap.put(rk, dedupKey)

      }

    }
    content.size
  }

  def close() : Unit = {
    this.table.close()
  }

  /** write the batch to database */
  def flush(): Unit = {

    val logMessagePrefix = s"flush: $tableName"
    log.info(s"$logMessagePrefix: start...")

    if (enableDedup == "true") {
      // remove dups
      removeDups()
    }

    val start = System.currentTimeMillis()
    val putSize = putList.size

    //write batch to hbase table
    this.table.put(putList.asJava)

    val duration = System.currentTimeMillis - start
    log.info(s"$logMessagePrefix flushed $putSize in $duration milliseconds. $tableName,$putSize,$duration")


    if (enableDedup == "true") {
      //clear rowkeyLookupMap
      currentBatchRowKeyDedupKeyMap.clear()
    }

    // reset the insert list
    putList = ArrayBuffer.empty[Put]

    log.info(s"$logMessagePrefix: end")
  }


  /** remove any duplicates */
  def removeDups(): Unit = {

    val start = System.currentTimeMillis()

    val logMessagePrefix = s"removeDups: DEDUP: $tableName: "
    log.info(s"$logMessagePrefix: start...")

    // get all the keys in the current batch
    val currentBatchRowKeys = currentBatchRowKeyDedupKeyMap.keySet.toList

    //if there are no keys in the current batch, just return
    if (currentBatchRowKeys.size == 0){
      log.info(s"""$logMessagePrefix: no candidate currentBatchRowKeys available for deduping, returning...""")
      //nothing to do, just return
      return
    }
    val hbaseRowDedupMap = fetchHbaseDedupKeys(currentBatchRowKeys, dedupColFamily, dedupColName, dedupLookback)

    val removeRowKeys = currentBatchRowKeys.filter(rk => {
      val currentDedupKey = currentBatchRowKeyDedupKeyMap.getOrElse(rk, "xx")
      val hbaseDedupKeyArray: Buffer[String] = hbaseRowDedupMap.getOrElse(rk, Buffer.empty[String])
      if ( hbaseDedupKeyArray contains currentDedupKey){
        // log.info(s"""$logMessagePrefix: rk: {$rk}, currentDedupKey: {$currentDedupKey}, hbaseDedupKey: {$hbaseDedupKeyArray} <-- FOUND DUPLICATE""")
        true
      } else {
        // log.info(s"""$logMessagePrefix: rk: {$rk}, currentDedupKey: {$currentDedupKey}, hbaseDedupKey: {$hbaseDedupKeyArray} <--NO DUPLICATE""")
        false
      }
    })

    // log.info(s"removeDups: DEDUP: $tableName: remove duplicate rowKeys: {"
    //   + removeRowKeys.mkString("\n")
    //   + "}")

    val beforeSize = putList.size

    // get rid of removeRowkeys from putList
    putList = putList.filter(putOne  => {

      // if rk in the list of removeRowKeys, then remove it
      if (removeRowKeys contains Bytes.toString(putOne.getRow())) {
        // log.info(s"$logMessagePrefix: $tableName: FOUND DUP: discard duplicate put: " + putOne.toJSON)
        false
      } else {
        true
      }
    })

    log.info(s"$logMessagePrefix: end")

    val duration = System.currentTimeMillis - start
    val afterSize = putList.size
    log.info(s"$logMessagePrefix deduped $beforeSize in $duration milliseconds. $tableName,$beforeSize,$afterSize,$duration")

  }

  /** get a list of dedup keys from table in hbase */
  def fetchHbaseDedupKeys(
    rowKeys: List[String],
    dedupColFamily: String,
    dedupKeyColumn: String,
    dedupLookback: String): HashMap[String, Buffer[String]] = {

    val logMessagePrefix = s"fetchHbaseDedupKeys: DEDUP: $tableName:"
    // rowkey-dedupkey map
    var rowkeyDedupKeyMap = scala.collection.mutable.HashMap.empty[String, Buffer[String]]

    val start = System.currentTimeMillis()
    val rowKeysListSize = rowKeys.size

    // create a list of Get's
    val dedupKeys: List[Get] = rowKeys.map(rk => {
      val getReq = new Get(Bytes.toBytes(rk)).addColumn(Bytes.toBytes(dedupColFamily), Bytes.toBytes(dedupKeyColumn))
      getReq.setMaxVersions(dedupLookback.toInt)
      getReq
    })

    val requestSize = dedupKeys.size

    // query hbase
    val resultListGet: Array[Result] =  this.table.get(dedupKeys.asJava)

    //log.info(s"$logMessagePrefix REQUEST size:{$requestSize}, RESPONSE size: {" + resultListGet.size + "}")

    resultListGet.foreach(result => {

      var rowKeyStr: String = ""
      val rowkeyMaybe = Option(result.getRow)
      rowkeyMaybe match {
        case Some(rk) => {
          rowKeyStr = Bytes.toString(rk)
          //log.info(s"$logMessagePrefix fetch rk: $rowKeyStr")

          val cellsMaybe = Option(result.listCells)
          cellsMaybe match {
            case Some(cells) => {
              val valueArray: Buffer[String] = asScalaBuffer(cells).map(cell => {Bytes.toString(CellUtil.cloneValue(cell))})
              rowkeyDedupKeyMap.put(rowKeyStr, valueArray)
              log.debug(s"$logMessagePrefix fetch rk: {$rowKeyStr}=>{$valueArray}, FOUND DEDUP KEY")
            }
            case None =>
              log.debug(s"$logMessagePrefix fetch rk: {$rowKeyStr}=>{}, NO DEDUP KEY")

          }

        }
        case None =>
          log.debug(s"$logMessagePrefix fetch rk: {Invalid rk}=>{}, NO ROW KEY, NO DEDUP KEY")
      }
    })

    val duration = System.currentTimeMillis - start
    log.info(s"$logMessagePrefix fetched $rowKeysListSize keys in $duration milliseconds. $rowKeysListSize,$duration")

    // return
    rowkeyDedupKeyMap
  }

  /** get size of the current batch*/
  def getCurrentBatchSize(): Int = {
    return putList.size
  }
}
