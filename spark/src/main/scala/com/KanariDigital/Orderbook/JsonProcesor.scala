/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook
import javax.script._
import org.apache.commons.logging.{Log, LogFactory}


trait ScriptLanguage {
  def getEngineName():String = {"invalid script language"}
}
case class JavascriptScriptLanguage() extends ScriptLanguage {
  override def getEngineName():String = {"nashorn"}
}
case class JythonScriptLanguage() extends ScriptLanguage {
  override def getEngineName():String = {"jython"}
}


/** returns a serialized JSON object by transforming a JSON object
  *  Can be used to process Orderbook process rules
  */


object EngineFactory extends Serializable {
  val log: Log = LogFactory.getLog(this.getClass.getName)
  @transient var engineMap = scala.collection.mutable.Map[Int, ScriptEngine with Invocable with Compilable]()

  def getEngine(lang: ScriptLanguage, fDefs: Array[String]) : ScriptEngine with Invocable = this.synchronized {
    val fDefsHash = fDefs.toSeq.hashCode
    if (fDefs.length > 0 && engineMap.getOrElse(fDefsHash,None) != None) return engineMap(fDefsHash)
    
    // engineMap(fDefsHash) = new ScriptEngineManager().getEngineByName(lang.getEngineName()).asInstanceOf[ScriptEngine with Invocable with Compilable]
    val engine = new ScriptEngineManager().getEngineByName(lang.getEngineName()).asInstanceOf[ScriptEngine with Invocable with Compilable]
    if (engine == null) {
      log.error("ScriptEngineManager().getEngineByName failed (did you use spark-submit '--packages org.python:jython-standalone:2.7.2b2,com.databricks:spark-avro_2.11:4.0.0'?) for language: " + lang.getEngineName())
      throw new RuntimeException("ScriptEngineManager().getEngineByName failed (did you use spark-submit '--packages org.python:jython-standalone:2.7.2b2,com.databricks:spark-avro_2.11:4.0.0'?) for language: " + lang.getEngineName())
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

    
    val engcount = engineMap.count(z=>true)
    //TODO: output metric here.
    if (engcount > 2)
      log.warn(s"engineMap contains ${engcount} unique engines")

    engineMap(fDefsHash) = engine
    return engineMap(fDefsHash)
  }
}

trait JsonProcessor extends Serializable {
  val log: Log = LogFactory.getLog(this.getClass.getName)
  def transform(context: Map[String, Any], json: String): String
}
/** a JsonProcessor that invokes the function named fName and defined in fDef
  */
case class JavascriptJsonProcessor(fName: String, fDef: Array[String], includePath: Array[String] = Array()) extends JsonProcessor with Serializable {
  override def transform(context: Map[String, Any], json: String): String = {
    val engine = EngineFactory.getEngine(JavascriptScriptLanguage(), fDef)
    val result1 = engine.invokeFunction(fName, context, json)
    println(s"result1 => $result1")
    s"$result1"
  }
}

//TODO: this is very tied into the datahandler package. perhaps this should be an override of a simpler jythonjsonprocessor so that we can keep the basic simplified usage of running a simple Python script
case class JythonJsonProcessor(fName: String, fDef: Array[String], includePaths: Array[String] = Array(),  sparkAppId: String = "NoAppId") extends JsonProcessor with Serializable {
  val appId = sparkAppId
  override def transform(context: Map[String, Any], json: String): String = {
    val transformContext = if (!context.contains("applicationid")) {
      context + ("applicationid" -> appId)
    } else {
      context
    }

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
        |from com.KanariDigital.app.util import ZeppelinLogUtils
        |def sparkInvoke(context, strparam):
        |  applicationId = context.get("applicationid").get()
        |  ZeppelinLogUtils.logZeppelinInfo(applicationId, "Jython sparkInvoke called on function $fName", "ZeppelinLog");
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
        |     ZeppelinLogUtils.logZeppelinError(applicationId, ' ### Exception in func "{}" through spark ###'.format(func), "ZeppelinLog")
        |     ########## NOTE THE LINE OFFSET BELOW ACCOUNTING FOR fDef PLACEMENT ABOVE
        |     ZeppelinLogUtils.logZeppelinError(applicationId, '     <file/line {}:{}>'.format(filename,lineno), "ZeppelinLog")
        |     ZeppelinLogUtils.logZeppelinError(applicationId, '     {}'.format(exc_type), "ZeppelinLog")
        |     ZeppelinLogUtils.logZeppelinError(applicationId, '     <{}>'.format(exc_obj), "ZeppelinLog")
        |     raise
        |  return returnVal
        |  #end of script
      """.stripMargin

    val engine = EngineFactory.getEngine(JythonScriptLanguage(), sparkInvoke +: fDef)
    val result1 = try {
      engine.invokeFunction("sparkInvoke", transformContext, json)
    } catch {
      case e: Exception => {
        log.error("Error with invokeFunction sparkInvoke: " + e.getMessage());
        e.printStackTrace
        log.error(s"input JSON: $json")
        return ""
        // throw e /*TODO:log metric here*/
      }
    }
    return s"$result1"
  }
}


object JsonProcessor extends Serializable {
  val src = s"""
       |function f(context, json) {
       |  return json;
       |};
       |""".stripMargin

  def apply(fName: String = "f", fDef: Array[String] = Array(src), lang: ScriptLanguage = JavascriptScriptLanguage(), includePath: Array[String] = Array()) : JsonProcessor = {
     lang match {
       case JavascriptScriptLanguage() => return new JavascriptJsonProcessor(fName, fDef, includePath)
       case JythonScriptLanguage() => return new JythonJsonProcessor(fName, fDef, includePath)
     }
    
  }
}
