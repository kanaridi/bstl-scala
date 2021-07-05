/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.common.util

import scala.util.{Failure, Success, Try}

import org.apache.commons.logging.{Log, LogFactory}


/** util for retrying
  */
object RetryUtils {

  val log: Log = LogFactory.getLog(RetryUtils.getClass.getName)

  /** Retry function for given number of times with retry interval in seconds
    * @param fn function to call
    * @param maxAttempts maximum number of times function can be tried
    * @param retryInterval retry interval in seconds
    *
    */
  def retry[T](retryAttempts: Int, retryInterval: Long) (fn: => T): T = {

    Try { fn } match {

      case Success(x) => x

      case _ if retryAttempts > 1 => {
        log.info(s"retry: attempt {$retryAttempts} after $retryInterval...")
        Thread.sleep(retryInterval)
        retry(retryAttempts - 1, retryInterval)(fn)
      }

      case Failure(e) => throw e
    }
  }
}
