/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.hbase
import java.io.InputStream

import org.apache.hadoop.conf.Configuration
import com.KanariDigital.JsonConfiguration.JsonConfig
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.{FileSystem, Path}

import scala.util.{Success, Try}

class HBaseConfiguration extends Serializable {
  var quorum : Option[String] = None
  var zookeeperClientPort : Option[Long] = None
  var kerberosPrincipal : Option[String] = None
  var kerberosKeytab : Option[String] = None
  var distrubutedMode : Option[Boolean] = None
  var scannerCaching : Option[Long] = None

  override def toString() : String = {
    val undef = "Undefined"
    val qStr = quorum.getOrElse(undef)
    val zcp = zookeeperClientPort.getOrElse(undef)
    val kp = kerberosPrincipal.getOrElse(undef)
    val kkt = kerberosKeytab.getOrElse(undef)
    val dis = distrubutedMode.getOrElse(undef)
    val sc = scannerCaching.getOrElse(undef)
    s"""
       |HBaseConfiguration:{zookeeper.quorum->$qStr, zookeeper.client.port->$zcp, kerberos.principal->$kp,
       | kerberos.keytab->$kkt, distributed-mode->$dis, scanner-caching->$sc}
     """.stripMargin
  }
}


object HBaseConfiguration extends Serializable {
  val log = LogFactory.getLog(HBaseConfiguration.getClass.getName)

  def readJsonFile(inputStream: InputStream): Try[HBaseConfiguration] = {
    val jsonConfig = new JsonConfig
    jsonConfig.read(inputStream, Some("hbase"))
    var config = new HBaseConfiguration
    config.quorum = jsonConfig.getStringValue("zookeeper.quorum")
    config.zookeeperClientPort = jsonConfig.getLongValue("zookeeper.client.port")
    config.kerberosPrincipal = jsonConfig.getStringValue("kerberos.principal")
    config.kerberosKeytab = jsonConfig.getStringValue("kerberos.keytab")
    config.distrubutedMode = jsonConfig.getBooleanValue("distributed-mode")
    config.scannerCaching = jsonConfig.getLongValue("scanner-caching")
    Success(config)
  }

  def readJsonFile(path: String, config: Configuration): Try[HBaseConfiguration] = {
    log.info(s"Reading HBase Configuration from $path")
    val fileSystem = FileSystem.get(config)
    val inputStream = fileSystem.open(new Path(path))
    val hbaseConfig = readJsonFile(inputStream)
    inputStream.close()
    hbaseConfig
  }
}
