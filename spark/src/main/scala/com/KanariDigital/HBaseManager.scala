/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.hbase

import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.hbase.client.{Admin, Connection, ConnectionFactory, Table}
import org.apache.hadoop.hbase.{TableName, HBaseConfiguration => HadoopHbaseConfig}
import org.apache.hadoop.conf.Configuration
import org.apache.hadoop.fs.Path
import org.apache.hadoop.security.UserGroupInformation


class HBaseManager extends Serializable {
  @transient lazy val hBaseConf : Configuration = HadoopHbaseConfig.create()
  @transient var connection : Connection = null
  @transient var admin : Admin = null

  def withResource(path : Path) : HBaseManager = {
    hBaseConf.addResource(path)
    this
  }

  def withZookerQuorum(quorum : String) : HBaseManager = {
    hBaseConf.set("hbase.zookeeper.quorum", quorum)
    this
  }

  def withZookeperClientPort(port : Long) : HBaseManager = {
    hBaseConf.set("hbase.zookeeper.property.clientPort", port.toString())
    this
  }

  def withZookeeperZnodeParentUnsecure() : HBaseManager = {
    hBaseConf.set("zookeeper.znode.parent", "/hbase-unsecure")
    this
  }

  def withKerberos(principal : String, keytab : String) : HBaseManager  = {
    UserGroupInformation.setConfiguration(hBaseConf)
    UserGroupInformation.loginUserFromKeytab(principal, keytab)
    this
  }

  def withDistributedMode(dist : Boolean) : HBaseManager = {
    hBaseConf.setBoolean("hbase.cluster.distributed", dist)
    this
  }

  def withScannerCaching(cashingCount : Int) : HBaseManager = {
    hBaseConf.setInt("hbase.client.scanner.caching", cashingCount)
    this
  }


  def getConnection(): Connection = {
    if (this.connection == null) {
      this.connection = ConnectionFactory.createConnection(this.hBaseConf)
    }
    this.connection
  }

  def getAdmin() : Admin = {
    if (this.admin == null) {
      this.admin = getConnection().getAdmin()
    }
    this.admin
  }

  def tableExists(table : TableName) : Boolean = {
    getAdmin().tableExists(table)
  }

  // With namespace
  def tableExists(namespace: String, tableName : String) : Boolean  = {
    tableExists(TableName.valueOf(namespace, tableName))
  }

  // Without namespace (default)
  def tableExists(tableName : String) : Boolean = {
    tableExists(TableName.valueOf(tableName))
  }

  def getTable(tableName : TableName) : Table = {
    getConnection().getTable(tableName)
  }

  def getTable(namespace : String, tableName : String) : Table = {
     getConnection().getTable(TableName.valueOf(namespace, tableName))
  }

  def getTable(tableName : String) : Table = {
    getTable(TableName.valueOf(tableName))
  }

  def close() : Unit = {
    if(this.connection != null) {
      this.connection.close()
      this.connection = null
    }
  }
}

object HBaseManager {
  val log: Log = LogFactory.getLog(HBaseManager.getClass().getName)

  def create(config: HBaseConfiguration): HBaseManager = {
    log.info("Creating HBaseManager from config: " + config.toString())
    val mgr = new HBaseManager
    if (config.quorum.isDefined) mgr.withZookerQuorum(config.quorum.get)
    if (config.zookeeperClientPort.isDefined) mgr.withZookeperClientPort(config.zookeeperClientPort.get)
    if (config.kerberosPrincipal.isDefined || config.kerberosKeytab.isDefined) {
      if (config.kerberosPrincipal.isDefined == false) throw new KerberosPrincipalNotSpecified
      if (config.kerberosKeytab.isDefined == false) throw new KerberosKeyTabNotSpecified
      mgr.withKerberos(config.kerberosPrincipal.get, config.kerberosKeytab.get)
    }
    if (config.distrubutedMode.isDefined) mgr.withDistributedMode(config.distrubutedMode.get)
    if (config.scannerCaching.isDefined) mgr.withScannerCaching(config.scannerCaching.get.toInt)
    log.info("Completed creating the HBaseManager")
    mgr
  }

  /** create a hbase manager */
  def create(
    zookeeperQuorum: String,
    zookeeperClientPort: String,
    kerberosPrincipal: String,
    kerberosKeytab: String,
    distributedMode: String,
    scannerCaching: String): HBaseManager = {

    log.info(s"""Creating HBaseManager: [zookeeperQuorum: $zookeeperQuorum
                                         zookeeperClientPort: $zookeeperClientPort
                                         kerberosPrincipal: $kerberosPrincipal
                                         kerberosKeytab: $kerberosKeytab
                                         distributedMode: $distributedMode
                                         scannerCaching: $scannerCaching]""".stripMargin)
    val mgr = new HBaseManager

    if (zookeeperQuorum != "") mgr.withZookerQuorum(zookeeperQuorum)

    if (zookeeperClientPort != "") mgr.withZookeperClientPort(zookeeperClientPort.toInt)

    if (kerberosPrincipal != "" && kerberosKeytab != "") {
      mgr.withKerberos(kerberosPrincipal, kerberosKeytab)
    }

    if (distributedMode != "") {
      mgr.withDistributedMode(distributedMode.toBoolean)
    }

    if (scannerCaching != "") {
      mgr.withScannerCaching(scannerCaching.toInt)
    }

    log.info("Completed creating the HBaseManager")
    mgr
  }

}
