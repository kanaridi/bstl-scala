/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook
import com.KanariDigital.app.DataHandler.StorageFactory
import org.scalatest.{FlatSpec, Matchers}

import scala.io.Source
import scala.util.parsing.json.JSONObject

class JsonProcessorTest extends FlatSpec
    with Matchers{

    def searchUpDirectory(dirpath: String) : String = {
      var upstring = "."
      var currentDirectory = new java.io.File(upstring).getCanonicalPath
      do{
        
        var file = new java.io.File(currentDirectory+"/"+dirpath)
        if (file.exists) {
          return file.getAbsolutePath()
        }
        upstring = upstring + "/.."
        currentDirectory = new java.io.File(upstring).getCanonicalPath()
     } while (currentDirectory != "/")
      return ""
    }


    val KanariPythonLoadStr =  s"import os; os.environ['KANARI_SPARKMODE']='True';spark=False;"
    var ruleMapStream = getClass.getResourceAsStream("/scripts/ruleMap.json")
    var ruleMapSrc = try Source.fromInputStream(ruleMapStream).mkString finally ruleMapStream.close()

    var moduleStream = getClass.getResourceAsStream("/scripts/WorkingDays.py")
    var moduleSrc = try Source.fromInputStream(moduleStream).mkString finally moduleStream.close()
    //val path = searchUpDirectory("zeppelin/module/deployment")
    val path = Array("")

    val defaultContext = Map("myInt"->4)
    val defaultStr = "pass"

    "JsonProcessor" should "get processor" in {
        var newProcessor = JsonProcessor()
        newProcessor shouldBe a [JsonProcessor]
    }

    it should "be JavascriptJsonProcessor by default" in {
      var newProcessor = JsonProcessor()
      newProcessor shouldBe a [JavascriptJsonProcessor]
    }

    it should "return same json" in {
      var newProcessor = JsonProcessor()
      var json = "{'Test':'Pass'}"
      var result = newProcessor.transform(defaultContext, json)
      json shouldBe result
    }

    "JavascriptJsonProcessor" should "add one in custom script" in {
      
      var strInput = "4"
      val src = s"""
       |function addOne(context, inStr) {
       |  var newInt = Number(inStr);
       |  newInt++;
       |  return String(newInt);
       |};
       |""".stripMargin
       var newProcessor = new JavascriptJsonProcessor("addOne", Array(src))
       var result = newProcessor.transform(defaultContext, strInput)
       result shouldBe "5"
    }

    it should "make use of a context variable" in {
      var context = Map("myInt"->4)
      var inStr = "6"
      val src = s"""
       |function addToMyInt(context, inStr) {
       |  var inInt = Number(inStr);
       |  //context.get returns a scala option we need to call get upon
       |  var contextInt = context.get("myInt").get()
       |  return String(inInt + contextInt);
       |};
       |""".stripMargin
       var newProcessor = new JavascriptJsonProcessor("addToMyInt", Array(src))
       var result = newProcessor.transform(context, inStr)
       result shouldBe "10"
    }

    "JythonJsonProcessor" should "add on in a custom script" in {
      var strInput = "2"
      val src = s"""
        |def addOne(context, inStr):
        |  myInt = int(inStr)
        |  myInt += 1
        |  return str(myInt)
      """.stripMargin
      var newProcessor = new JythonJsonProcessor("addOne", Array(src))
      var result = newProcessor.transform(defaultContext, strInput)
      result shouldBe "3"
    }

    it should "use the context variable" in {
      var context = Map("myInt"->4)
      var strInput = "6"
      val src = s"""
        |def addToMyInt(context, inStr):
        |  inInt = int(inStr)
        |  contextInt = context.get("myInt").get()  
        |  return str(inInt + contextInt)
      """.stripMargin
      var newProcessor = new JythonJsonProcessor("addToMyInt", Array(src))
      var result = newProcessor.transform(context, strInput)
      result shouldBe "10"
    }

    it should "convert to dict object" in {
      var strInput = JSONObject(Map("Test" -> "Fail", "age" -> 10.0)).toString()
      val src = s"""
        |import json
        |def convertJsonToDict(context, jsonStr):
        |  print("starting param:" + jsonStr)
        |  objDict = json.loads(jsonStr)
        |  objDict["Test"] = "Pass"
        |  return objDict
      """.stripMargin
      var newProcessor = JsonProcessor("convertJsonToDict", Array(src), JythonScriptLanguage())
      var result = newProcessor.transform(defaultContext, strInput)
      result shouldBe a [String]
    }

    /*
    it should "share a runtime between processors" in {
      var context = Map("myInt"->4)
      var strInput = ""
      val src = s"""
        |scriptOneVal = 1
        |def outputVal(context, jsonStr):
        |  global scriptOneVal
        |  return scriptOneVal
      """.stripMargin

      val src2 = s"""
        |def addGlobals(context, jsonStr):
        |  global scriptOneVal
        |  return scriptOneVal + 2
      """.stripMargin
 
      var newProcessor = JsonProcessor("outputVal", src, JythonScriptLanguage())
      var newProcessor2 = JsonProcessor("addGlobals", src2, JythonScriptLanguage())
      var result = newProcessor.transform(context, strInput)
      println(s"result:$result")
      var result2 = newProcessor2.transform(context, strInput)
      println(s"result:$result2")
      result shouldBe 1
      result2 shouldBe 3
    }
    */

    
    it should "apply the path correctly" in {
      val src = s"""
        |import sys
        |def returnPath(context, jsonStr):
        |  return sys.path
      """.stripMargin

      path should not be ""
      var newProcessor = JsonProcessor("returnPath", Array(src), JythonScriptLanguage(), path)
      var result = newProcessor.transform(defaultContext, defaultStr)
      println("THE PATH IS:" + result)
      result should include (path.mkString(","))
    }

    // it should "let me call kanaridigital" in {
    //   val src = s"""
    //     |$KanariPythonLoadStr
    //     |from KanariDigital import KanariDataHandler, UberObject
    //     |def run(context, jsonStr):
    //     |  mydict = {"kanaridigitaltest":"pass"}
    //     |  UberObject.printDictObject(mydict)
    //   """.stripMargin
    //   //var stream = getClass.getResourceAsStream("/rules.py")
    //   //var src = Source.fromInputStream(stream).mkString
    //   //val path = searchUpDirectory("zeppelin/module/deployment")
    //   path should not be ""
    
    //   var newProcessor = JsonProcessor("run", Array(src), JythonScriptLanguage(), path)
    //   var result = newProcessor.transform(defaultContext, defaultStr)
    // }



    // it should "load rules python" in {
    //   var strInput = "pass"
    //   var stream = getClass.getResourceAsStream("/scripts/rules.py")
    //   val src = try Source.fromInputStream(stream).mkString finally stream.close()
    //     val completeSrc = s"""
    //       |$KanariPythonLoadStr
    //       |$src
    //       |#end
    //     """.stripMargin
    //   path should not be ""

    //   StorageFactory.setMockData("ruleMap.json", ruleMapSrc)
    //   StorageFactory.setMockData("/modules/WorkingDays.py", moduleSrc)
    //   var newProcessor = JsonProcessor("UnitTestMethod", Array(completeSrc), JythonScriptLanguage(), path)
    //   var result = newProcessor.transform(defaultContext, strInput)
    //   StorageFactory.clearMockData()
    //   result shouldBe "pass"
    // }

    // it should "run rules on a single item" in {
    //   path should not be ""

    //   var stream = getClass.getResourceAsStream("/scripts/rules.py")
    //   var rulesSrc = try Source.fromInputStream(stream).mkString finally stream.close()
    //   rulesSrc should not be ""

    //   val sparkInvoke = s"""
    //     |$KanariPythonLoadStr
    //     |from KanariDigital import KanariDataHandler
    //     |def run(context, strparam):
    //     |    returnVal = KanariDataHandler.TestRunner.sparkRun(context, strparam)
    //     |    return returnVal
    //     |$rulesSrc
    //   """.stripMargin

    //   //Load in the variables.
    //   var context = Map("myInt"->4,"uberType"->"OrderObject")
    //   var s4Stream = getClass.getResourceAsStream("/data/UberObject-S4.json")
    //   var s4Obj = try Source.fromInputStream(s4Stream).mkString finally s4Stream.close()
    //   s4Obj should not be ""

    //   //mock data
    //   StorageFactory.setMockData("ruleMap.json", ruleMapSrc)
    //   StorageFactory.setMockData("/modules/WorkingDays.py", moduleSrc)

    //   var newProcessor = JsonProcessor("run", Array(sparkInvoke), JythonScriptLanguage(), path)
    //   var result = newProcessor.transform(context, s4Obj)

    //   StorageFactory.clearMockData()

    //   import org.json.simple.parser.JSONParser
    //   val startObj = new JSONParser().parse(s4Obj)
    //   val resultObj = new JSONParser().parse(result)
    //   startObj should not be resultObj
    // }

  /*
    it should "convert to dict object and run a second rule" in {
      var context = Map("myInt"->4)
      
      var strInput = JSONObject(Map("Test" -> "Fail", "age" -> 10.0)).toString()
      val convertString = s"""
        |import json
        |def convertJsonToDict(context, jsonStr):
        |  return json.loads(jsonStr)
      """.stripMargin
      val convertString = s"""
        |def chageTestToPass(context, jsonStr):
        |  print(jsonStr)
        |  return 
      """.stripMargin
      
      var newProcessor = JsonProcessor("convertJsonToDict", convertString, JythonScriptLanguage())
      var result = newProcessor.transform(context, strInput)
      result shouldBe a [String]
    }
    */


}
