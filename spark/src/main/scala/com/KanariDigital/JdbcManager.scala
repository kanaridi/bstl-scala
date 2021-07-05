/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.jdbc

import org.apache.commons.logging.{Log, LogFactory}
import java.sql.{Connection, DriverManager, PreparedStatement, ResultSet, Types}
import java.util.Properties

import scala.util.{Failure, Success, Try}
import scala.collection.mutable.Queue
//I call out Maps specifically so i can use the
// mutable.Map without messing with immutable maps
import scala.collection.mutable
import scala.collection.immutable.Map

case class jdbcColumn(sqlType:Int, value:Any)
case class jdbcRow(columns:jdbcColumn*)

object jdbcColumnValue {
  val log: Log = LogFactory.getLog(this.getClass().getName)

  def create(sqlType:String, value:Any): Try[jdbcColumn] = {
    //https://www.tutorialspoint.com/jdbc/jdbc-data-types.htm
    val typeInt:Int = sqlType.toUpperCase() match  {
      case "VARCHAR" => Types.VARCHAR
      case "CHAR" => Types.CHAR
      case "LONGVARCHAR" => Types.LONGVARCHAR
      case "BIT" => Types.BIT
      case "NUMERIC" => Types.NUMERIC
      case "TINYINT" => Types.TINYINT
      case "SMALLINT" => Types.SMALLINT
      case "INTEGER" => Types.INTEGER
      case "BIGINT" => Types.BIGINT
      case "REAL" => Types.REAL
      case "FLOAT" => Types.FLOAT
      case "DOUBLE" => Types.DOUBLE
      case "VARBINARY" => Types.VARBINARY
      case "BINARY" => Types.BINARY
      case "DATE" => Types.DATE
      case "TIME" => Types.TIME
      case "TIME_WITH_TIMEZONE" => Types.TIME_WITH_TIMEZONE
      case "TIMESTAMP" => Types.TIMESTAMP
      case "TIMESTAMP_WITH_TIMEZONE" => Types.TIMESTAMP_WITH_TIMEZONE
      case "CLOB" => Types.CLOB
      case "BLOB" => Types.BLOB
      case "ARRAY" => Types.ARRAY
      case "REF" => Types.REF
      case "STRUCT" => Types.STRUCT
      case _=> throw new IllegalArgumentException("Bad sqlType passed into create")
    }
    Success(new jdbcColumn(typeInt, value))
  }
}

class JdbcManager extends Serializable {
  val log: Log = LogFactory.getLog(this.getClass().getName)
  var connection: Connection = null
  var connProps: Properties = new Properties()
  var jdbcDriver = ""
  var dbUrl = ""
  var batchSize = 1000L
  var tableMap = mutable.Map[String, Queue[jdbcRow]]() //Map of table names to queues

  def setDriver(driverTypeStr: String): Unit = {
    if (this.connection != null) {
      log.error(s"Attempt to change jdbc driver to $driverTypeStr after connection is a bad idea")
    }
    //Sql Bulk Copy Options is about 10x faster than normal insert operations and is best if we can use it.
    //this.sqlBulkEnabled = (driverTypeStr == "com.microsoft.sqlserver.jdbc.SQLServerDriver")
    this.jdbcDriver = driverTypeStr
    this.connection = null
  }
  def setDatabaseUrl(url: String): Unit =
    this.dbUrl = url

  def setDatabaseName(name: String): Unit =
    this.connProps.setProperty("database", name)

  def setConnection(url: String, username: String, password: String): Unit = {
    setDatabaseUrl(url)
    setSqlAuth(username, password)
  }

  def setSqlAuth(username: String, password: String): Unit = {
    this.connProps.setProperty("user", username)
    this.connProps.setProperty("password", password)
  }

  def setConnectionProperties(props: Map[String, String]): Unit = {
    val jdbcProps = new Properties()
    props.foreach { case (key, value) => jdbcProps.setProperty(key, value.toString) }
    connProps = jdbcProps
  }

  def setBatchSize(batchSize: Long): Unit =
    this.batchSize = batchSize

  def getConnection(): Try[Connection] = Try {
    if (this.connection == null) {
        Class.forName(jdbcDriver)
        connection = DriverManager.getConnection(dbUrl, this.connProps)
    }
    connection
  }

  def executeStatement(statement: String): Try[ResultSet] = Try {
    val conn = getConnection().get
    val stmnt = conn.createStatement()
    val rs = stmnt.executeQuery(statement)
    rs
  }

  def addToTableQueue(table:String, row:jdbcRow):Unit = {
    var q:Queue[jdbcRow] = tableMap.getOrElse(table, new Queue[jdbcRow]())
    q.enqueue(row)
    tableMap(table) = q
  }
  def addToTableQueue(table:String, columns: jdbcColumn*): Unit = {
    val row = new jdbcRow(columns: _*)
    this.addToTableQueue(table, row)
  }


  def flushTableQueue(table:String): Try[Int] = Try{
    val q = tableMap.getOrElse(table, null)
    if (q == null) {
      log.warn(s"flushTableQueue called for $table even though it was empty")
      return Success(0)
    }
    flushInsertQueue(table, q).get
  }

  def flushAll(): Try[Int] = Try{
    var res = 0
    tableMap.foreach{case (table, q) => {
      flushInsertQueue(table, q) match {
        case Success(count) => res += count
        case Failure(e) => {
          log.error(s"Failed to flush $table with exception ${e.toString}")
          throw e
        }
      }
    }}
    res
  }

  def flushInsertQueue(table:String, insertQueue:Queue[jdbcRow]): Try[Int] = Try {
    var successCount = 0
    if (insertQueue.isEmpty) {
      log.warn("flushWriteQueue called with nothing to flush")
      successCount
    } else {
      // Find the number of columns to use to setup the insert call
      // we need to build the query INSERT INTO mytable VALUES (?, ?, ?)
      val columnCount = insertQueue.head.columns.size
      var placeholderStr = "("
      for (i <- 1 until (columnCount-1)) {
        placeholderStr += "?,"
      }
      placeholderStr += "?)"
      val queryString = s"INSERT INTO $table VALUES $placeholderStr"

      //make the preparedstatement that we can execute a batch against
      val conn = this.getConnection().get
      val ps = conn.prepareStatement(queryString)

      var count = 0
      while (insertQueue.nonEmpty) {
        val row = insertQueue.dequeue()
        //add a complete row to the preparedStatement
        ps.clearParameters()
        row.columns.zipWithIndex.foreach { case (col, i) =>
          val columnNum = i + 1
          col.sqlType match {
            case Types.VARCHAR | Types.CHAR | Types.LONGVARCHAR => ps.setString(columnNum, col.value.toString)
            case Types.BIT => ps.setBoolean(columnNum, col.value.asInstanceOf)
            case Types.FLOAT | Types.REAL => ps.setFloat(columnNum, col.value.asInstanceOf)
            case Types.TIMESTAMP | Types.TIMESTAMP_WITH_TIMEZONE => ps.setTimestamp(columnNum, col.value.asInstanceOf)
            case Types.TIME | Types.TIME_WITH_TIMEZONE => ps.setTime(columnNum, col.value.asInstanceOf)
            case Types.DATE => ps.setDate(columnNum, col.value.asInstanceOf)
            case _ => throw new RuntimeException(s"unsupported type put into the insert queue for $table in column $col")
          }

          if (i + 1 > columnCount) {
            log.error("flushWriteQueue hit a row that was larger than expected")
          }
        }
        ps.addBatch()
        count += 1

        //if we reached the limit then kick off the batch write and keep going
        if (count >= batchSize) {
          ps.clearParameters()
          val results = ps.executeBatch()
          results.foreach(rr =>
            successCount+=rr)
          count = 0
        }
      }

      // finish off any final rows after we cleared out the queue
      if (count > 0) {
        ps.clearParameters()
        val results = ps.executeBatch()
        results.foreach(rr =>
          successCount+=rr)
        count = 0
      }

     successCount
    }
  }

  def close() : Unit = {
    if(this.connection != null) {
      this.connection.close()
      this.connection = null
    }
  }

}

object JdbcManager {
  val log: Log = LogFactory.getLog(JdbcManager.getClass().getName)

  def create(driver: String, url: String, dbName: String,
             username: String, password: String,
             batchSize:Long): JdbcManager = {
    log.info(s"Creating JdbcManager from config: drv:$driver, url:$url, db:$dbName, un:$username, pw:<redacted>")
    val mgr = new JdbcManager
    mgr.setDriver(driver)
    mgr.setDatabaseUrl(url)
    mgr.setDatabaseName(dbName)
    mgr.setSqlAuth(username, password)
    mgr.setBatchSize(batchSize)
    mgr
  }

  def create(driver: String, dbUrl: String, props: Map[String, String], batchSize:Long): JdbcManager = {
    val mgr = new JdbcManager
    mgr.setDriver(driver)
    mgr.setDatabaseUrl(dbUrl)
    mgr.setConnectionProperties(props)
    mgr.setBatchSize(batchSize)
    mgr
  }
}
