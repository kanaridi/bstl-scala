/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook

import java.io.ByteArrayInputStream

import com.KanariDigital.JsonConfiguration.JsonConfig
import com.KanariDigital.KafkaConsumer.KafkaConfiguration
import com.KanariDigital.app.AppConfig
import com.KanariDigital.app.AuditLog.HdfsLoggerConfig
import com.KanariDigital.hbase.HBaseConfiguration
import org.scalatest.FlatSpec

import scala.util.Try

class ConfigTest extends FlatSpec{
  "The Kafka audit-log configuration" should "parse using the nested JSON configuration" in {
    val inputStr =
      """
        | {
        | "audit-log" : {
        |   "file_pattern": "/EA/%s/%s"
        | } }
      """.stripMargin
    val bos = new ByteArrayInputStream(inputStr.getBytes())
    val jsonConfig = new JsonConfig()
    val json = jsonConfig.read(bos, Some("audit-log"))
    val fp = jsonConfig.getStringValue("file_pattern")
    assert(fp.isDefined && fp.get == "/EA/%s/%s")
  }

  val singleJsonStr =
    """
      | {
      |   "hbase" : {
      |      "zookeeper.quorum": "g4t7471.houston.hpecorp.net,g4t7478.houston.hpecorp.net,g4t7518.houston.hpecorp.net",
      |      "zookeeper.client.port": 2181,
      |      "kerberos.principal" : "srvc_egsc_hdpuser@EAHPEITG.HOUSTON.HPECORP.NET",
      |      "kerberos.keytab" : "/etc/security/keytabs/srvc_egsc_hdpuser.service.keytab",
      |      "distributed-mode" : "false",
      |      "scanner-caching" : 1000
      |   },
      |   "kafka" : {
      |      "topics": ["ORDRSP_SERP_hpit-ifsl", "ORDERS05_SERP_hpit-ifsl", "ORDERS_SERP_hpit-ifsl", "INVOIC_SERP_hpit-ifsl", "SHPMNT_SERP_hpit-ifsl", "DESADV_SERP_hpit-ifsl","ORDCHG_SERP_hpit-ifsl"],
      |      "group.id": "spark-executor-SERP-DataLake_hpit-ifsl_group",
      |      "key.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
      |      "value.deserializer": "org.apache.kafka.common.serialization.StringDeserializer",
      |      "brokers": "i1l00467g.dc01.its.hpecorp.net:443,i1l00468g.dc01.its.hpecorp.net:443,i1l00469g.dc01.its.hpecorp.net:443,i1l00470g.dc01.its.hpecorp.net:443,i1l00471g.dc01.its.hpecorp.net:443,i1l00472g.dc01.its.hpecorp.net:443",
      |      "auto.offset.reset": "earliest",
      |      "security.protocol": "SSL",
      |      "ssl.truststore.location": "/etc/security/certs/sc-raw-process-engine.jks",
      |      "ssl.keystore.location": "/etc/security/certs/sc-raw-process-engine.jks",
      |      "ssl.keystore.password": "password",
      |      "ssl.truststore.password": "password",
      |      "ssl.key.password": "password",
      |      "batchDuration": 15,
      |      "windowLength": 10,
      |      "slideInterval": 6,
      |      "outputDelimiter": ",",
      |      "checkpoint": "hdfs:///EA/supplychain/process/checkpoints/kanari"
      |   },
      |   "audit-log" : {
      |     "path":  "/EA/supplychain/process/logs/EAP2.0",
      |     "filename-prefix": "audit"
      |   }
      | }
    """.stripMargin

  "hbase configuration" should "parse in the large file" in {
    val bos = new ByteArrayInputStream(singleJsonStr.getBytes())
    val hbaseConfigTry = HBaseConfiguration.readJsonFile(bos)
    assert(hbaseConfigTry.isSuccess)
    val hc = hbaseConfigTry.get
    assert(hc.quorum.isDefined && hc.quorum.get.split(",").length == 3)
    assert(hc.zookeeperClientPort.isDefined && hc.zookeeperClientPort.get == 2181)
    assert(hc.kerberosPrincipal.isDefined && hc.kerberosPrincipal.get.contains("srvc_egsc_hdpuser"))
    assert(hc.distrubutedMode.isDefined && hc.distrubutedMode.get == false)
    assert(hc.scannerCaching.isDefined && hc.scannerCaching.get == 1000)
  }

  "kafka configuration" should "parse in the large file" in {
    val bos = new ByteArrayInputStream(singleJsonStr.getBytes())
    val kafkaConfigTry = KafkaConfiguration.readJsonFile(bos)
    assert(kafkaConfigTry.isSuccess)
    val c = kafkaConfigTry.get
    assert(c.topics.isDefined && c.topics.get.contains("ORDRSP_SERP_hpit-ifsl")
      && c.topics.get.contains("SHPMNT_SERP_hpit-ifsl"))
    assert(c.groupId.isDefined && c.groupId.get == "spark-executor-SERP-DataLake_hpit-ifsl_group")
    assert(c.keyDeserializer.isDefined && c.keyDeserializer.get == "org.apache.kafka.common.serialization.StringDeserializer")
    assert(c.valueDeserializer.isDefined && c.valueDeserializer.get == "org.apache.kafka.common.serialization.StringDeserializer")
    assert(c.brokers.isDefined && c.brokers.get.startsWith("i1l00467g.dc01.its.hpecorp.net:443,i1l00468g.dc01.its"))
    assert(c.autoOffsetReset.isDefined && c.autoOffsetReset.get == "earliest")
    assert(c.securityProtocol.isDefined && c.securityProtocol.get == "SSL")
    assert(c.sslTruststoreLocation.isDefined && c.sslTruststoreLocation.get == "/etc/security/certs/sc-raw-process-engine.jks")
    assert(c.sslKeystoreLocation.isDefined && c.sslKeystoreLocation.get == "/etc/security/certs/sc-raw-process-engine.jks")
    assert(c.sslKeystorePassword.isDefined && c.sslKeystorePassword.get == "password")
    assert(c.sslTruststorePassword.isDefined && c.sslTruststorePassword.get == "password")
    assert(c.batchDuration.isDefined && c.batchDuration.get == 15)
    assert(c.windowLength.isDefined && c.windowLength.get == 10)
    assert(c.slideInterval.isDefined && c.slideInterval.get == 6)
    assert(c.outputDelimiter.isDefined && c.outputDelimiter.get == ",")
    assert(c.streamingContextCheckpoint.isDefined && c.streamingContextCheckpoint.get.startsWith("hdfs:///EA/supplychain"))
  }

  "Audit logger configuration" should "succeed" in {
    val bos = new ByteArrayInputStream(singleJsonStr.getBytes())
    val act = HdfsLoggerConfig.readJsonFile(bos, "audit-log")
    assert(act.isSuccess && act.get.path == "/EA/supplychain/process/logs/EAP2.0")
    assert(act.get.fileNamePrefix == "audit")
  }

  "OB Rules config" should "succeed" in {
    val configStream = Try(getClass.getResourceAsStream("/test-ob.json"))
    val appName ="ob_rules"
    val appConfig = Try(AppConfig.readJsonFile(appName, configStream.get))

    println(appConfig)
    assert(appConfig.get.appConfigMap("log-level") == "WARN")
    assert(appConfig.get.ruletestConfigMap("root-path") == "abfs://bst-shares@kdpublic.dfs.core.windows.net/kanari/rules_test/")
    assert(appConfig.get.ruletestConfigMap("since") == "2020-04-20T00:00:00")
    assert(appConfig.get.rulesConfigMap("dependency-path") == "/kanari-user-storage/")
    assert(appConfig.get.rulesConfigMap("mode") == "files")
    assert(appConfig.get.graphiteConfigMap("host") == "10.0.0.4")
    assert(appConfig.get.graphiteConfigMap("port") == "2003")
    assert(appConfig.get.validationMapperConfigMap("path") == "validationMap.json")

    val foundRuleMap = appConfig.get.uberRulesConfigMap.getOrElse(OrderObject().toString(), Map[String, String]())
    assert(foundRuleMap != foundRuleMap.empty)
    assert(foundRuleMap("mode") == "files")
    assert(foundRuleMap("dependency-path") == "abfs://bst-shares@kdpublic.dfs.core.windows.net/kanari/rules_dev/dependencies/")
    assert(appConfig.get.uberRulesConfigMap(OrderObject.toString)("mode") == "files")
    assert(appConfig.get.uberRulesConfigMap(DeliveryObject.toString)("mode") == "files")
    assert(appConfig.get.uberRulesConfigMap(OrderObject.toString())("dependency-path") == "abfs://bst-shares@kdpublic.dfs.core.windows.net/kanari/rules_dev/dependencies/")
  }

}
