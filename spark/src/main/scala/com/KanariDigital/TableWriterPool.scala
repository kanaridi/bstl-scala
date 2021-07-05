/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital

import com.KanariDigital.hbase.{HBaseManager, TableWriter}
import org.apache.commons.logging.{Log, LogFactory}

import scala.collection.mutable.{Map => MMap}

class TableWriterPool(hbaseManager : HBaseManager) extends Serializable {
  val log: Log = LogFactory.getLog(this.getClass.getName)

  var tableWriterMap = MMap.empty[String,TableWriter]

  def token(nameSpace : String, tableName : String) : String = nameSpace.trim + ":" +  tableName.trim

  private def create(nameSpace: String, tableName: String) : TableWriter = {
    log.info(s"Creating tableWriter for $nameSpace : $tableName")
    val theTable = hbaseManager.getTable(nameSpace, tableName)
    val tableWriter = new TableWriter(theTable)
    tableWriterMap += token(nameSpace,tableName) -> tableWriter
    tableWriter
  }

  def getOrCreate(nameSpace: String, tableName: String) : TableWriter = {
    tableWriterMap.getOrElse(token(nameSpace,tableName), create(nameSpace, tableName))
  }

  def flush() : Unit = {
    log.info("Flushing tables: " + tableWriterMap.map(x => x._1).mkString(", "))
    tableWriterMap.values.foreach(x => x.flush())
  }

  def close() : Unit = {
    log.info("Closing tables: " + tableWriterMap.map(x => x._1).mkString(", "))
    tableWriterMap.values.foreach(x => x.close())
  }

}
