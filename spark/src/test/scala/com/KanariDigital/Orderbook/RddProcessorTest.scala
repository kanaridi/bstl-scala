/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook

import scala.collection.immutable.Map
import scala.collection.mutable.ListBuffer
import scala.io.Source
import scala.reflect.ClassTag
import scala.util.{Success, Try, Failure}

import com.fasterxml.jackson.databind.{DeserializationFeature, ObjectMapper}
import com.fasterxml.jackson.module.scala.DefaultScalaModule
import com.fasterxml.jackson.dataformat.csv.{CsvMapper, CsvSchema, CsvParser}

import org.apache.spark.rdd.RDD

import org.scalatest._

import org.scalamock.scalatest.MockFactory

import com.holdenkarau.spark.testing.{RDDComparisons, SharedSparkContext}

import com.KanariDigital.Orderbook.HdfsHiveWriter._
import com.kanaridi.xform.{HBRow, MessageMapper, MessageMapperConfig, MessageMapperFactory, TransformResult}
import com.KanariDigital.app.{RddProcessor, AppContext, AppConfig}


case class ParsedTransformResult(
  docIDs: List[String],
  hbaseRows: List[ParsedHBaseRows])


case class ParsedHBaseRows(
  tableName: String,
  rowKey:String,
  family: String,
  cells: Map[String, String],
  namespace:String,
  deDupKey: String,
  sortKey: String)


class RddProcessorTest extends FunSuite with SharedSparkContext with RDDComparisons with MockFactory {

  /*print out transform result as json to stdout - for debugging only */
  def printTransformResultAsJSON(transformResultsRDD:RDD[TransformResult]): Unit = {
    println("START=================================>")
    val transformResults = transformResultsRDD.collect()
    for (transformResult: TransformResult <- transformResults) {
      //parse map as json
      val jsonCellsMapper = new ObjectMapper()
      jsonCellsMapper.registerModule(DefaultScalaModule)
      val transformResultJson = jsonCellsMapper.writeValueAsString(transformResult)
      println(transformResultJson.replace("\n",""))
    }
    println("END=================================>")
  }

  /** print out source data in csv format to stdout- for debugging only */
  def printSourceDataAsCSV(rows: List[(String,String)]): Unit = {
    println("START=================================>")
    rows.map( row => {

      val topic = row._1
      val jsonPayload = row._2

      //parse json payload
      val payloadMapper = new ObjectMapper()
      payloadMapper.registerModule(DefaultScalaModule)
      val payloadJson = payloadMapper.writeValueAsString(jsonPayload)

      val arrayRow = Array(topic, payloadJson)

      // convert cells Map type to json string
      val mapper = new CsvMapper()
      mapper.registerModule(DefaultScalaModule)
      val csvResult = mapper.writeValueAsString(arrayRow)
      println(csvResult.replace("\n",""))
    })
    println("END=================================>")
  }

  /** print out source data to stdout- for debugging only */
  def printSourceData(rows: List[(String,String)]): Unit = {
    println("START=================================>")
    rows.map( row => {
      val topic = row._1
      val jsonPayload = row._2
      println(s"$topic|$jsonPayload")
    })
    println("END=================================>")
  }

  /** print first mismatch when comparing RDD's */
  def printCompareMismatch[T: ClassTag](testCompare:Option[T]){
    testCompare match {
      case Some(mismatchTuple) => {
        val (key, expected_num, test_num) = mismatchTuple
        println(s"FIRST MISMATCH => key: {$key} occurs {$test_num} times and was expected {$expected_num} times")
      }
      case None => {
        // test passes
        // println("No differences")
      }
    }
  }

  /** read source json data from specified path
    */
  def readSourceData(topic:String, path:String): List[(String,String)] = {
    val lines = if (path != "" ) {
      val source = Source.fromURL(getClass().getResource(path))
      val lines = (for (line <- source.getLines()) yield (topic, line)).toList
      source.close()
      lines
    } else {
      List[(String, String)]()
    }
    lines
  }

  /** read expected result from text file and return an RDD for comparison  */
  def readExpectedTransformResult(topic: String, path: String): RDD[TransformResult] = {
    val lines = if (path != "" ) {
      val source = Source.fromURL(getClass().getResource(path))
      val lines = (for (line <- source.getLines()) yield line).toList
      source.close()
      lines
    } else {
      List[String]()
    }

    var transResultListBuffer = new ListBuffer[TransformResult]

    for (line <- lines) {

      //json to map
      val jsonMapper = new ObjectMapper
      jsonMapper.registerModule(DefaultScalaModule)
      jsonMapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false)

      val jobj = jsonMapper.readValue(line, classOf[ParsedTransformResult])
      val parsedDocIDs: Set[String] = jobj.docIDs.toSet

      val parsedHBaseRows: List[ParsedHBaseRows] = jobj.hbaseRows
      var hbrListBuffer = new ListBuffer[HBRow]
      for (parsedHBaseRow <- parsedHBaseRows){
        val tableName = parsedHBaseRow.tableName
        val rowKey = parsedHBaseRow.rowKey
        val family = parsedHBaseRow.family
        val namespace = parsedHBaseRow.namespace
        val cells = parsedHBaseRow.cells
        val deDupKey = parsedHBaseRow.deDupKey
        val sortKey = parsedHBaseRow.sortKey
        val hbr =  new HBRow(tableName, rowKey, family, cells, namespace, deDupKey, sortKey)

        hbrListBuffer += hbr
      }

      //transformed result
      val transRes = new TransformResult(hbrListBuffer.toList, parsedDocIDs, topic)
      transResultListBuffer += transRes
    }

    val transResultRDD = sc.parallelize(transResultListBuffer.toList)
    transResultRDD
  }

  /** read mapper config file and initialize the message mapper */
  def initMessageMapper(path : String) : Option[MessageMapper] = {
    println(s"Reading test mapper configuration: " + path)
    //val mmcStream = fileSystem.open(new Path(path.get))
    val mmcStream = getClass().getResourceAsStream(path)
    val mmcfg = MessageMapperConfig(mmcStream)
    val xform = Some(MessageMapperFactory(mmcfg))
    return xform
  }

  /** read mapper config file and initialize the message mapper */
  def initAppConfig(appName: String, path : String) : Try[AppConfig] = {
    println(s"Reading app configuration: " + path)
    //val mmcStream = fileSystem.open(new Path(path.get))
    val configStream = Try(getClass().getResourceAsStream(path))
    val appConfig = Try(AppConfig.readJsonFile(appName, configStream.get))
    appConfig
  }

  /** transform source data using mapping file and compare result to expected result
    */
  def checkTransform (appConfigPath: String,
    appName: String,
    topic: String,
    sourceDataPath: String,
    mappingPath: String,
    expectedResultPath: String): Unit = {

    val appConfig: AppConfig = initAppConfig(appName, appConfigPath) match {
      case Success(theConfig) => {
        val configStr = theConfig.toString()
        theConfig
      }
      case Failure(f) => {
        throw new Exception("cannot read config file")
      }
    }
    val xform:Option[MessageMapper] = initMessageMapper(mappingPath)
    val appContext = new AppContext(appConfig, xform.get)

    //get raw data
    val updateBatchData = readSourceData(topic, sourceDataPath)
    // printSourceDataAsCSV(updateBatchData)

    // convert source data into rdd
    val updateBatchDataRDD = sc.parallelize(updateBatchData)

    // create a rdd processor
    val rddProcessor = new RddProcessor(appContext, appName)

    // transform the source data
    val testRDD: RDD[TransformResult] = rddProcessor.transformMapping(xform.get, updateBatchDataRDD, System.currentTimeMillis())
    // printTransformResultAsJSON(testRDD)

    val expectedRDD: RDD[TransformResult] = readExpectedTransformResult(topic, expectedResultPath)
    // printTransformResultAsJSON(expectedRDD)

    val diffResult = compareRDD(
      expectedRDD:RDD[TransformResult],
      testRDD:RDD[TransformResult]
    )

    // test counts
    assert(expectedRDD.count() == testRDD.count())

    // test transform result
    assertResult(None){
      // printCompareMismatch(diffResult)
      diffResult
    }
  }

  test("check transform mapping for data at: spark/test/resources/data/20190510190505.txt, topic: ORDRSP_SERP_hpit-ifsl, mapping version: 0.2.1" ) {

    val appConfigPath = "/test-ob.json"
    val appName = "logistics_hdfs"
    val topic = "ORDRSP_SERP_hpit-ifsl"
    val mappingPath = "/test-ob-mappings.json"
    val sourceDataPath = "/data/20190510190505.txt"
    val expectedResultPath = "/data/20190510190505_transform_results.txt"

    checkTransform(
      appConfigPath,
      appName,
      topic,
      sourceDataPath,
      mappingPath,
      expectedResultPath)
  }

  test("check transform mapping for data at: spark/test/resources/data/20190510190505.txt, topic: ORDRSP_SERP_hpit-ifsl, mapping version: 0.3.1" ) {

    val appConfigPath = "/test-ob.json"
    val appName = "logistics_hdfs"
    val topic = "ORDRSP_SERP_hpit-ifsl"
    val mappingPath = "/test-ob-mappings-0.3.1.json"
    val sourceDataPath = "/data/20190510190505.txt"
    val expectedResultPath = "/data/20190510190505_transform_results.txt"

    checkTransform(
      appConfigPath,
      appName,
      topic,
      sourceDataPath,
      mappingPath,
      expectedResultPath)
  }

  test("check transform mapping for data at: spark/test/resources/data/20190510190505.txt, topic: ORDRSP_SERP_hpit-ifsl, mapping version: 0.4.1" ) {

    val appConfigPath = "/test-ob.json"
    val appName = "logistics_hdfs"
    val topic = "ORDRSP_SERP_hpit-ifsl"
    val mappingPath = "/test-ob-mappings-0.4.1.json"
    val sourceDataPath = "/data/20190510190505.txt"
    val expectedResultPath = "/data/20190510190505_transform_results.txt"

    checkTransform(
      appConfigPath,
      appName,
      topic,
      sourceDataPath,
      mappingPath,
      expectedResultPath)
  }

  test("check transform mapping for data at: spark/test/resources/data/logistics_sd_data.txt, topic: shipmentDetails, mapping version: 0.4.1" ) {

    val appConfigPath = "/test-ob.json"
    val appName = "logistics_hdfs"
    val topic = "shipmentDetails"
    val mappingPath = "/test-log-map-0.4.1.json"
    val sourceDataPath = "/data/logistics_sd_data.txt"
    val expectedResultPath = "/data/logistics_sd_transform_results.txt"

    checkTransform(
      appConfigPath,
      appName,
      topic,
      sourceDataPath,
      mappingPath,
      expectedResultPath)
  }

  test("check transform mapping for data at: spark/test/resources/data/logistics_ss_data.txt, topic: shipmentStatus, mapping version: 0.4.1" ) {

    val appConfigPath = "/test-ob.json"
    val appName = "logistics_hdfs"
    val topic = "shipmentStatus"
    val mappingPath = "/test-log-map-0.4.1.json"
    val sourceDataPath = "/data/logistics_ss_data.txt"
    val expectedResultPath = "/data/logistics_ss_transform_results.txt"

    checkTransform(
      appConfigPath,
      appName,
      topic,
      sourceDataPath,
      mappingPath,
      expectedResultPath)
  }

  test("check sourceMap when expression" ) {

    val appConfigPath = "/test-ob.json"
    val appName = "logistics_hdfs"
    val topic = "shipmentDetails"
    val mappingPath = "/test-log-map-when.json"
    val sourceDataPath = "/data/logistics_sd_data.txt"
    val expectedResultPath = "/data/logistics_sd_when_transform_results.txt"

    checkTransform(
      appConfigPath,
      appName,
      topic,
      sourceDataPath,
      mappingPath,
      expectedResultPath)
  }

  test("check sourceMap when expression with multiple matches" ) {

    val appConfigPath = "/test-ob.json"
    val appName = "logistics_hdfs"
    val topic = "shipmentDetails"
    val mappingPath = "/test-log-map-when-multi.json"
    val sourceDataPath = "/data/logistics_sd_data.txt"
    val expectedResultPath = "/data/logistics_sd_when_multi_transform_results.txt"

    checkTransform(
      appConfigPath,
      appName,
      topic,
      sourceDataPath,
      mappingPath,
      expectedResultPath)
  }

  test("check sourceMap with colFamily specified in col spec" ) {

    val appConfigPath = "/test-ob.json"
    val appName = "logistics_hdfs"
    val topic = "shipmentDetails"
    val mappingPath = "/test-log-map-colfam-col.json"
    val sourceDataPath = "/data/logistics_sd_data.txt"
    val expectedResultPath = "/data/logistics_sd_when_multi_transform_results.txt"

    checkTransform(
      appConfigPath,
      appName,
      topic,
      sourceDataPath,
      mappingPath,
      expectedResultPath)
  }

  test("check sourceMap with colFamily specified in row spec" ) {

    val appConfigPath = "/test-ob.json"
    val appName = "logistics_hdfs"
    val topic = "shipmentDetails"
    val mappingPath = "/test-log-map-colfam-row.json"
    val sourceDataPath = "/data/logistics_sd_data.txt"
    val expectedResultPath = "/data/logistics_sd_when_multi_transform_results.txt"

    checkTransform(
      appConfigPath,
      appName,
      topic,
      sourceDataPath,
      mappingPath,
      expectedResultPath)
  }

  test("check sourceMap with multiple colFamily" ) {

    val appConfigPath = "/test-ob.json"
    val appName = "logistics_hdfs"
    val topic = "shipmentDetails"
    val mappingPath = "/test-log-map-colfam-multiple.json"
    val sourceDataPath = "/data/logistics_sd_data.txt"
    val expectedResultPath = "/data/logistics_sd_colfam_multiple_transform_results.txt"

    checkTransform(
      appConfigPath,
      appName,
      topic,
      sourceDataPath,
      mappingPath,
      expectedResultPath)
  }

  test("check sourceMap when expression with no matches" ) {

    val appConfigPath = "/test-ob.json"
    val appName = "logistics_hdfs"
    val topic = "shipmentDetails"
    val mappingPath = "/test-log-map-when-nomatches.json"
    val sourceDataPath = "/data/logistics_sd_data.txt"
    val expectedResultPath = ""

    checkTransform(
      appConfigPath,
      appName,
      topic,
      sourceDataPath,
      mappingPath,
      expectedResultPath)
  }
}
