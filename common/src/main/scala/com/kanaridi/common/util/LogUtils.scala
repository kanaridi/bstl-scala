/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.common.util

import scala.util.{Failure, Success, Try}
import com.fasterxml.jackson.databind.ObjectMapper
import com.kanaridi.common.util.LogUtils.BugState.BugState
import com.kanaridi.common.util.LogUtils.BugStatus.BugStatus
import com.kanaridi.common.util.LogUtils.BugType.BugType
import org.apache.commons.logging.{Log, LogFactory}
import org.apache.http.HttpStatus
import org.apache.http.client.methods.HttpPost
import org.apache.http.entity.{ContentType, StringEntity}
import org.apache.http.impl.client.DefaultHttpClient
import org.json4s.{DefaultFormats, JObject, NoTypeHints}
import org.json4s.jackson.Serialization
import org.json4s.jackson.Serialization.write
import org.json4s.jackson.JsonMethods.parse

import scala.concurrent.ExecutionContext.Implicits.global
import scala.concurrent.Future
import scala.io.Source
import scala.io.Source.fromInputStream

/** log utils
  */
object LogUtils {

  val log: Log = LogFactory.getLog(this.getClass.getName)

  val ERROR_MESSAGE_MAX_LENGTH = 1000

  val ERROR_STACK_SIZE = 20

  /** get exception message and stacktrace as a valid json string value */
  def getExceptionDetails(th: Throwable): Tuple2[String, String] = {

    Try {

      // error message
      val messageMayBe = Option(th.getMessage.toString)
      val exMessage = if (messageMayBe.isDefined) messageMayBe.get else ""
      val exMessageShort = if (exMessage.length > ERROR_MESSAGE_MAX_LENGTH)
        exMessage.substring(0, ERROR_MESSAGE_MAX_LENGTH) else exMessage
      val message = LogUtils.convToJsonString(exMessageShort)

      // errorStack
      val stackFrames = th.getStackTrace
      val selectStackFrames = if (stackFrames.length > ERROR_STACK_SIZE)
        stackFrames.take(ERROR_STACK_SIZE) else stackFrames
      val stackTrace = LogUtils.convToJsonString(selectStackFrames
        .map(_.toString).mkString(" "))

      (message, stackTrace)

    } match {
      case Success(x) => x
      case Failure(ex) => ("", "")
    }
  }

  /** get exception message and stacktrace as a valid json string value */
  def getExceptionDetailsWithType(th: Throwable): Tuple3[String, String, String] = {

    Try {

      //error type
      val errorType = th.getClass.getSimpleName

      // error message
      val messageMayBe = Option(th.getMessage.toString)
      val exMessage = if (messageMayBe.isDefined) messageMayBe.get else ""
      val exMessageShort = if (exMessage.length > ERROR_MESSAGE_MAX_LENGTH)
        exMessage.substring(0, ERROR_MESSAGE_MAX_LENGTH) else exMessage
      val message = LogUtils.convToJsonString(exMessageShort)

      // errorStack
      val stackFrames = th.getStackTrace
      val selectStackFrames = if (stackFrames.length > ERROR_STACK_SIZE)
        stackFrames.take(ERROR_STACK_SIZE) else stackFrames
      val stackTrace = LogUtils.convToJsonString(selectStackFrames
        .map(_.toString).mkString(" "))

      (errorType, message, stackTrace)

    } match {
      case Success(x) => x
      case Failure(ex) => ("", "", "")
    }
  }

  /** get string as  valid json string value */
  def convToJsonString(value: String): String = {
    Try {
      val message = (new ObjectMapper()).writeValueAsString(value)
      val messageWithoutStartEndQuotes = message
      .replaceAll("""^"""", "") // remove start double quote
      .replaceAll(""""$""", "") // remove end double quote
      .replaceAll(":", " ")
      .replaceAll(",", " ")
      messageWithoutStartEndQuotes
    } match {
      case Success(x) => x
      case Failure(ex) => ""
    }
  }

  /**
    * Request body sample
    * {
    *    "title": "Bug title",
    *    "description": "description",
    *    "bugType": "DataBug",
    *    "status": "open",
    *    "bugState": "started"
    *  }
    */
  case class DQAppStartPayload(title: String,
                               description: String,
                               bugType: String,
                               status: String,
                               bugState: String,
                               logUrl: String)


  object BugType extends Enumeration {
    type BugType = Value
    val Bug, DataBug, TestBug = Value
  }

  object BugStatus extends Enumeration {
    type BugStatus = Value
    val Open, InProgress, Done = Value
  }

  object BugState extends Enumeration {
    type BugState = Value
    val Started, Finished = Value
  }

  /**
   * Log the exception as a data bug by calling our DQ App API to record the log
   * Also save the file in embargo
   * @param bugType Type of the bug we want to log
   * @param title Title of the bug we want to log
   * @param detail: Detail of the bug
   * @param embargoFilePath temp file path that we will copy the data to when we are hitting data bug
   */
  def logBug(bugType: BugType, title: String, detail: String, embargoFilePath: String = ""): Unit = {
    implicit val formats = Serialization.formats(NoTypeHints)

    Future {
      val bugTitle = title
      val bugDescription = detail
      val bugStatus = BugStatus.Open
      val bugState = BugState.Finished
      val payload = DQAppStartPayload(bugTitle, bugDescription, bugType.toString, bugStatus.toString, bugState.toString, embargoFilePath)
      val payloadStr = write(payload)
      log.warn(s"startPayload: $payloadStr")
      startLog(payloadStr)
    }
  }

  private def startLog(payload: String): Unit = {
    implicit val formats = DefaultFormats
    //TODO: minhdong Move this to option in spec
    val startUrl = "https://dqapi.azurewebsites.net/api/startbugreport?code=4r3ASc1vNtwz4D4i519lalNuikl4c0hIMYpp93F5EmK95UUeI8Ng1A=="
    val httpClient = new DefaultHttpClient()

    try {
      val startCall = new HttpPost(startUrl)
      val entity = new StringEntity(payload, ContentType.APPLICATION_JSON)
      startCall.setEntity(entity)
      val response = httpClient.execute(startCall)
      val status = response.getStatusLine
      if(status.getStatusCode != HttpStatus.SC_CREATED && status.getStatusCode != HttpStatus.SC_OK) {
        log.warn(Source.fromInputStream(response.getEntity.getContent).mkString(""))
        log.warn(s"DQ App start API status code is not OK or CREATED: ${response.getStatusLine.getStatusCode}")
        None
      } else {
        // Get the ID
        var jsonResponse = "{}"
        if (entity != null) {
          val inputStream = entity.getContent()
          jsonResponse = fromInputStream(inputStream).getLines.mkString
          inputStream.close
        }
        log.warn(s"StartBug response: $jsonResponse")
      }
    } catch {
      case e: Exception => {
        log.error(s"Error sending log", e);
      }
    } finally {
      httpClient.getConnectionManager.shutdown()
    }
  }

}
