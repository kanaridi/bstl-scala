/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import javax.script.{Compilable, Invocable, ScriptEngine, ScriptEngineManager}
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.hadoop.fs.Path
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.hadoop.conf.{Configuration => HadoopConfiguration}
import scala.util.parsing.json.JSONObject



/***************
  * RuleScript takes a path to a script file that is formatted to take 2 string properties and must return a string
  *
  *   "operatorType": "ruleScript",
  *   "signals": {
  *     "sources": [{"signalId": "input"}],
  *     "sinks": [{"signalId": "output"}]
  *     },
  * " options": {
  *     "language": "javascript",
  *     "methodNames": ["rule1", "rule2"],
  *     "paths": ["abfs://users@kdpoc.dfs.core.windows.net/ruleScript/rule1.js",
  *               "abfs://users@kdpoc.dfs.core.windows.net/ruleScript/rule2.js"],
  *     "dependenciesFolders": ["abfs://users@kdpoc.dfs.core.windows.net/ruleScript/dependencies/"]
  *     "locationType": "base64" or "HDFS",
  *     "base64Script: "s3snbw1Sa3n..b64EncodedScriptContents"
  *     }
  *
  * The script itself inside of the paths contain method definitions. Above, rule1 and rule2 would be called
  * in order passing one piece to the other.
  *   function rule1(context, jsonObj) {
  *     json.field1 = "new stuff"
  *     return jsonObj
  *   }
  *   function rule2(context, jsonObj) {
  *     json.field2 = "more new stuff"
  *     return jsonObj
  *   }
  *******************/

class RuleScript(val spark: org.apache.spark.sql.SparkSession,
                 val upstream: Table,
                 val language: String,
                 val methodNames: Seq[String],
                 val locationType: String,
                 val paths: Seq[String],
                 val base64Code: String,
                 val dependenciesFolders: Seq[String]) extends SingleOp {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  @throws(classOf[Exception])
  override def operate1(upstreamDf: Option[DataFrame]): Option[DataFrame] = {
    if (upstreamDf.isEmpty)  None

    // If hdfs, lets read all the rules from hdfs and load them into a Seq of strings
    // If base64, lets decode the blob and turn it into a Seq of strings
    val rules:Seq[String] = locationType.toLowerCase() match {
      case "base64" => Seq(new String(java.util.Base64.getDecoder.decode(base64Code)))
      case "hdfs" =>
        for(fPath <- paths) yield {
          val path = new Path(fPath)
          val inStream = path.getFileSystem(new HadoopConfiguration).open(path)
          scala.io.Source.fromInputStream(inStream).mkString
        }
      case _ => throw new IllegalArgumentException(s"${locationType} is not a valid ruleScript locationType")
    }

    log.info(s"language=${language}, methodName=${methodNames}, locationType=${locationType}, " +
      s"paths = ${paths}, dependenciesFolder=${dependenciesFolders}, base64Script=${base64Code}")
    log.debug(s"Script Contents: ${rules.toString()}")

    // Lets grab the right json processor based on the language type we are trying to use
    val processor = language.toLowerCase() match {
      case "python" => new JythonJsonProcessor(rules.toArray)
      case "javascript" => new JavascriptJsonProcessor(rules.toArray)
      case "kanaripython" => new KanariJythonJsonProcessor(rules.toArray, dependenciesFolders.toArray)
      case _ => throw new IllegalArgumentException(s"${language} is not a valid ruleScript language")
    }

    // Need to convert the seq to an array since seq is not serializable
    val methodList = methodNames.toArray
    val context = Map("myInt"->4)

    // We need to convert the df to Json encoded strings, pass them into the chosen JsonProcessor method
    // and subsequent methods for transformation by the script language and then take that result
    // and dump it back to a single column dataframe
    import spark.implicits._
    val transformed = upstreamDf.get.toJSON.map(row => {
      val context =  JSONObject(Map("testInt" -> 4)).toString() //placeholder context, will need if for something surely
      var jsonItem:String = row
      methodList.foreach((method => {
        jsonItem = processor.transform(method, context, jsonItem)
      }))
      jsonItem
    }).toDF("jsonContent")

    // Now we need to take that single column dataframe and explode it back out to a full dataframe.
    val schema = spark.read.json(transformed.select("jsonContent").as[String]).schema
    val finalDf = transformed.select(from_json($"jsonContent", schema).as("jsonContent")).select("jsonContent.*")

    Some(finalDf)
  }
}


/* ScriptLanguage are just a handy mechanism to declare and compare types */
trait ScriptLanguage {
  def getEngineName():String = {"invalid script language"}
}
case class JavascriptScriptLanguage() extends ScriptLanguage {
  override def getEngineName():String = {"nashorn"}
}
case class JythonScriptLanguage() extends ScriptLanguage {
  override def getEngineName():String = {"jython"}
}


/**********************
  * Object processors will be used by passing a raw string of context and the json structure string.
  * No transforming into objects will be done automatically
  * @param fDef - the array of strings that are the script code
  */
abstract class ObjectProcessor(fDef: Array[String]) extends  Serializable {
  val log: Log = LogFactory.getLog(this.getClass.getName)
  def getLanguage():ScriptLanguage
  def transform(fName:String, context: String, json: String): String = {
    val engine = EngineFactory.getEngine(getLanguage(), fDef)
    val result = engine.invokeFunction(fName, context, json)
    s"$result"
  }
}
class JavascriptObjectProcessor(fDef: Array[String])
  extends ObjectProcessor(fDef) with Serializable {
  override def getLanguage():ScriptLanguage = JavascriptScriptLanguage()
}
class JythonObjectProcessor(fDef: Array[String], includePath: Array[String])
  extends ObjectProcessor(fDef) with Serializable {
  def getLanguage():ScriptLanguage = JythonScriptLanguage()
}

/**********************
  * JsonProcessor processors will do conversion into json structure automatically and hand to the script
  * @param fDef - the array of strings that are the script code
  */
abstract class JsonProcessor(fDef: Array[String])
  extends ObjectProcessor(fDef) with Serializable {
  def transformJsonWrapperFunctionScript(): String
  override def transform(fName:String, context: String, json: String): String = {
    val fDefWithJson = Array(transformJsonWrapperFunctionScript()) ++ fDef
    val engine = EngineFactory.getEngine(getLanguage(), fDefWithJson)
    val result = engine.invokeFunction("transformJsonWrapper", fName, context, json)
    s"$result"
  }
}
class JavascriptJsonProcessor(fDef: Array[String])
  extends JsonProcessor(fDef) with Serializable {
  def getLanguage():ScriptLanguage = JavascriptScriptLanguage()
  override def transformJsonWrapperFunctionScript(): String = {
    s"""function transformJsonWrapper(funcName, context, jsonString) {
       |  var jsonObj = JSON.parse(jsonString)
       |  var contextObj = JSON.parse(context)
       |  var evalStr = funcName + "(contextObj, jsonObj)"
       |  var transformedJsonObj = eval(evalStr)
       |  return JSON.stringify(transformedJsonObj)
       |}""".stripMargin
  }
}
class JythonJsonProcessor(fDef: Array[String])
  extends JsonProcessor(fDef) with Serializable {
  def getLanguage():ScriptLanguage = JythonScriptLanguage()
  override def transformJsonWrapperFunctionScript(): String = {
    s"""import json
       |def transformJsonWrapper(funcName, context, jsonString):
       |  jsonObj = json.loads(jsonString)
       |  contextObj = json.loads(context)
       |  evalStr = funcName + "(contextObj, jsonObj)"
       |  transformedJsonObj = eval(evalStr)
       |  return json.dumps(transformedJsonObj)""".stripMargin
  }
}
class KanariJythonJsonProcessor(fDef: Array[String], includePaths: Array[String])
  extends JythonJsonProcessor(fDef) with Serializable {
  override def getLanguage():ScriptLanguage = JythonScriptLanguage()
  override def transform(fName:String, context: String, json: String): String = {
    //NOTE: __pyclasspath__ turns out to be the root of the obspark jar file which
    //     is where the resources folder ends up. so __pyclasspath__/python/ maps
    //     to .../bstl-scala/spark/src/main/resources/python
    var includeStr = "'__pyclasspath__/python/','__pyclasspath__/python/modules/'"
    for (path <- includePaths) {
      includeStr += s"'$path'"
    }
    //TODO: this may be better to throw a custom exception that has all these details so scala can log things cleanly
    //TODO: This section is very much tied to the rules and DataHandler implementations. This should probably be defined
    // (at least partially) in the rules stuff and the datahandler package.
    val sparkInvoke = s"""
                         |import sys;sys.path.extend([$includeStr]);
                         |
                         |import os,sys,traceback
                         |from com.KanariDigital.app.util import LogUtils
                         |def sparkInvoke(context, strparam):
                         |  applicationId = context.get("applicationid").get()
                         |  LogUtils.logZeppelinInfo(applicationId, "Jython sparkInvoke called on function $fName", "ZeppelinLog");
                         |  returnVal = "script error"
                         |  try:
                         |    returnVal = $fName(context, strparam)
                         |  except Exception:
                         |     (exc_type, exc_obj, tb) = sys.exc_info()
                         |     traceLen = len(traceback.extract_tb(tb))
                         |     if traceLen > 0:
                         |       (filename, lineno, func, text) = traceback.extract_tb(tb)[traceLen-1]
                         |       lineno = lineno-2
                         |     else:
                         |       (filename, lineno, func, text) = traceback.extract_tb(tb)[0]
                         |       lineno = lineno-1
                         |     LogUtils.logZeppelinError(applicationId, ' ### Exception in func "{}" through spark ###'.format(func), "ZeppelinLog")
                         |     ########## NOTE THE LINE OFFSET BELOW ACCOUNTING FOR fDef PLACEMENT ABOVE
                         |     LogUtils.logZeppelinError(applicationId, '     <file/line {}:{}>'.format(filename,lineno), "ZeppelinLog")
                         |     LogUtils.logZeppelinError(applicationId, '     {}'.format(exc_type), "ZeppelinLog")
                         |     LogUtils.logZeppelinError(applicationId, '     <{}>'.format(exc_obj), "ZeppelinLog")
                         |     raise
                         |  return returnVal
                         |  #end of script
      """.stripMargin

    val fDefWithJson = Array(transformJsonWrapperFunctionScript(),sparkInvoke) ++ fDef
    val engine = EngineFactory.getEngine(getLanguage(), fDefWithJson)
    val result = try {
      engine.invokeFunction("sparkInvoke", context, json)
    } catch {
      case e: Exception => {
        log.error("Error with invokeFunction sparkInvoke: " + e.getMessage());
        e.printStackTrace
        log.error(s"input JSON: $json")
        return ""
        // throw e /*TODO:log metric here*/
      }
    }
    s"$result"
  }
}




object EngineFactory extends Serializable {
  val log: Log = LogFactory.getLog(this.getClass.getName)
  @transient var engineMap = scala.collection.mutable.Map[Int, ScriptEngine with Invocable with Compilable]()

  def getEngine(lang: ScriptLanguage, fDefs: Array[String]) : ScriptEngine with Invocable = this.synchronized {
    val fDefsHash = fDefs.toSeq.hashCode
    if (fDefs.length > 0 && engineMap.getOrElse(fDefsHash,None) != None) return engineMap(fDefsHash)

    val engine = new ScriptEngineManager().getEngineByName(lang.getEngineName()).asInstanceOf[ScriptEngine with Invocable with Compilable]
    if (engine == null) {
      log.error(s"getEngineByName failed for language: ${lang.getEngineName()}")
      log.error("You may need to include packages for your language")
      throw new RuntimeException(s"getEngineByName failed for language: ${lang.getEngineName()}. You may need to include packages at job startup.")
    }

    //Some engines (specifically jython) don't like getting huge files so we send in arrays of function definitions
    for(fDef <- fDefs) {
      try {
        engine.compile(fDef).eval()
      } catch {
        case e:Exception=> log.error("Exception compiling and evaluating  script: " + e.getMessage())
          throw e
      }
    }

    val engCount = engineMap.count(z=>true)
    if (engCount > 2) log.warn(s"engineMap contains ${engCount} unique engines")

    engineMap(fDefsHash) = engine
    engineMap(fDefsHash)
  }
}
