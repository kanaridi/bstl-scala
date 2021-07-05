/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.DataHandler

import com.KanariDigital.Orderbook.{BpRulesRunner, JsonProcessor, JythonScriptLanguage, OrderObject}
import com.KanariDigital.app.{AppConfig, AppContext}
import com.kanaridi.xform.{MessageMapper, MessageMapperConfig, MessageMapperFactory}
import org.apache.spark.sql.SparkSession
import org.mockito.{Mockito, MockitoSugar}
import org.scalatest.{BeforeAndAfterAll, FlatSpec, Matchers}

import scala.io.Source
import scala.util.Try
import scala.util.parsing.json.JSONObject

class DataHandlerTest extends FlatSpec with MockitoSugar with Matchers with BeforeAndAfterAll {

    override protected def beforeAll(): Unit = {
      sys.props("UnitTestRunning") = "true"
    }

    val configStream = Try(getClass().getResourceAsStream("/test-ob.json"))
    val appName ="ob_rules"
    val appConfig = AppConfig.readJsonFile(appName, configStream.get)

    "Datahandler" should "be accessible from Jython" in {
      val context = Map("myInt"->4)
      
      val strInput = JSONObject(Map("Test" -> "Fail", "age" -> 10)).toString()
      val src = s"""
        |from com.KanariDigital.app.DataHandler import UnitTestObject
        |def convertJsonToDict(context, jsonStr):
        |  myval = UnitTestObject.printMessage("this is a unit test message")
        |  return myval
      """.stripMargin
      val newProcessor = JsonProcessor("convertJsonToDict", Array(src), JythonScriptLanguage())
      val result = newProcessor.transform(context, strInput)
      result shouldBe "pass"
    }


    /* read mapper config file and initialize the message mapper */
    def initMessageMapper(path : String) : Option[MessageMapper] = {
      println(s"Reading test mapper configuration: " + path)
      val mmcStream = getClass().getResourceAsStream(path)
      val mmcfg = MessageMapperConfig(mmcStream)
      val xform = Some(MessageMapperFactory(mmcfg))
      xform
    }

    "Datahandler" should "not create new instance every time" in {
      val sparkSession = SparkSession.builder()
        .master("local")
        .config("", "")
        .getOrCreate()

      val xform:Option[MessageMapper] = initMessageMapper("/test-ob-mappings.json")
      val appContext = new AppContext(appConfig, xform.get)

      val stream2 = getClass.getResourceAsStream("/scripts/validationMap.json")
      val mapSrc = try Source.fromInputStream(stream2).mkString finally stream2.close()
      com.KanariDigital.app.DataHandler.Dictionary.setMockData("validationMap.json", mapSrc)

      val dataHandler = DataHandler.apply(sparkSession, appContext)
      val firstReader = dataHandler.getDataReader()
      val secondReader = dataHandler.getDataReader()
      val firstWriter = dataHandler.getDataWriter()
      val secondWriter = dataHandler.getDataWriter()
      assert(firstReader == secondReader)
      assert(firstWriter == secondWriter)
    }

    "DataReader" should "load and get HLI object" in {
      val sparkSession = SparkSession.builder.master("local").getOrCreate()

      val xform:Option[MessageMapper] = initMessageMapper("/test-ob-mappings.json")
      val appContext = new AppContext(appConfig, xform.get)

      val stream2 = getClass.getResourceAsStream("/scripts/validationMap.json")
      val mapSrc = try Source.fromInputStream(stream2).mkString finally stream2.close()
      com.KanariDigital.app.DataHandler.Dictionary.setMockData("validationMap.json", mapSrc)

      val dataHandler = DataHandler.apply(sparkSession, appContext)
      val dataReader = dataHandler.getDataReader()

      val mockBpRulesRunner = Mockito.mock(classOf[BpRulesRunner])
      val emptyDf = sparkSession.emptyDataFrame

      Mockito.when(mockBpRulesRunner.getUberObjectsByName("OrderObject")).thenReturn(emptyDf)

      dataReader.bpRunner = mockBpRulesRunner
      dataReader.load("OrderObject", "123")
      assert(dataReader.getHighLevelObjectDF() != null)
    }

    "Storage" should "provide instances correctly" in {
      ConfigHelper.fromAppConfig(appConfig ,OrderObject.toString())

      val userStorage = StorageFactory.get("/kanari-user-storage/")
      val sameUserStorage = StorageFactory.get("/kanari-user-storage/")
      val anotherUserStorage = StorageFactory.get("/test/path/")
      val anotherSameUserStorage = StorageFactory.get("/test/path/")

      val logStorage = StorageFactory.get("/this/log/storage/")

      val dependencyPath = userStorage.getRootPath()
      val logPath = logStorage.getRootPath()
      val anotherDependencyPath = anotherUserStorage.getRootPath()
      val anotherSameDependencyPath = anotherSameUserStorage.getRootPath()

      assert(dependencyPath.equals("/kanari-user-storage/"))
      assert(logPath == "/this/log/storage/")
      assert(anotherDependencyPath.equals("/test/path/"))
      assert(anotherSameDependencyPath.equals("/test/path/"))

      assert(sameUserStorage.getRootPath() == userStorage.getRootPath())
      assert(anotherUserStorage.getRootPath() != userStorage.getRootPath())
      assert(anotherSameUserStorage.getRootPath() == anotherUserStorage.getRootPath())
    }

    "ConfigHelper" should "provide the right paths" in {
      val configHelper = ConfigHelper.fromAppConfig(appConfig ,OrderObject.toString())
      val deploymentRoothPath = configHelper.getDeploymentRootPath()

      assert(deploymentRoothPath.equals("abfs://bst-shares@kdpublic.dfs.core.windows.net/kanari/rules_test/"))
    }

   // it should "allow mock data" in {

   //    val xform:Option[MessageMapper] = initMessageMapper("/test-ob-mappings.json")
   //    val appContext = new AppContext(appConfig, xform.get)
   //   val cfgHelper = ConfigHelper.fromAppConfig(appConfig, "OrderObject", true)

   //   val stream2 = getClass.getResourceAsStream("/scripts/ruleMap.json")
   //   val mapSrc = try Source.fromInputStream(stream2).mkString finally stream2.close()

   //    val userStorage = StorageFactory.get("/kanari-user-storage/")
   //    StorageFactory.setMockData("/scripts/ruleMap.json", mapSrc)
   //    val src = s"""
   //                |import os; os.environ['KANARI_SPARKMODE']='True';spark=False;
   //                |from KanariDigital import KanariDataHandler
   //                |def run(context, jsonStr):
   //                |  KanariDataHandler.init(spark, "OrderObject")
   //                |  mydict = KanariDataHandler.Loader.loadDictionary("/scripts/ruleMap.json")
   //                |  return mydict["itemcategory"]["Services"][0]
   //    """.stripMargin

   //   var newProcessor = JsonProcessor("run", Array(src), JythonScriptLanguage())
   //   var result = newProcessor.transform(Map("myInt"->4), "")
   //   assert(result == "ZFCC")


   //  }

   //  "Dictionary" should "load mock data" in {
   //    val stream2 = getClass.getResourceAsStream("/scripts/ruleMap.json")
   //    val mapSrc = try Source.fromInputStream(stream2).mkString finally stream2.close()
   //    var cfgHelper = ConfigHelper.fromAppConfig(appConfig, OrderObject.toString(), true)
   //    com.KanariDigital.app.DataHandler.Dictionary.setMockData("ruleMap.json", mapSrc)

   //    val dict = Dictionary("ruleMap.json")
   //    val content = dict.get("$.itemcategory.Services[0]")
   //    content should not be "ZFCC"
   //  }

}
