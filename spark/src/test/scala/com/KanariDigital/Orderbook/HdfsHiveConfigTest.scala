/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook

import java.io.ByteArrayInputStream

import com.KanariDigital.Orderbook.HdfsHiveWriter.HdfsHiveWriterConfig._

import org.scalatest.FlatSpec

class HdfsHiveConfigTest extends FlatSpec {

  "reading from a stream" should "succeed" in {
    val inputStr =
      """
        | {
        |   "hdfs-write": {
        |    "hive_mapping" : {
        |          "topic1": "mapped1",
        |          "topic2": "mapped2"
        |    },
        |    "hdfs-raw": "raw",
        |    "save-mode" : "text"
        | } }
      """.stripMargin

    val bos = new ByteArrayInputStream(inputStr.getBytes())
    val c = readJsonFile(bos)
    assert(c.isSuccess)
    val m1 = c.get.hiveTopicMapping("topic1")
    assert(m1.isDefined && m1.get == "mapped1")
    val m2 = c.get.hiveTopicMapping("topic2")
    assert(m2.isDefined && m2.get == "mapped2")
    assert(c.get.hiveTopicMapping("other").isDefined == false)
    assert(c.get.hdfsRaw.isDefined && c.get.hdfsRaw.get == "raw")
    assert(c.get.saveMode.isDefined && c.get.saveMode.get == "text")
  }

  "reading from resource file" should "succeed" in {
    val inputStream = getClass.getClassLoader.getResourceAsStream("ITG-HDFS-Hive.json")
    val c = readJsonFile(inputStream)
    assert(c.isSuccess)
  }

}
