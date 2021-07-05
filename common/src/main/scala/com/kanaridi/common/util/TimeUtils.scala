/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.common.util

import java.util.concurrent.TimeUnit
import java.util.regex.Matcher
import java.util.regex.Pattern
import scala.collection.immutable.Map


object TimeUtils {

  val timeSuffixes: Map[String, TimeUnit] = Map(
    "us" -> TimeUnit.MICROSECONDS,
    "ms" -> TimeUnit.MILLISECONDS,
    "milliseconds" -> TimeUnit.MILLISECONDS,
    "s" -> TimeUnit.SECONDS,
    "seconds" -> TimeUnit.SECONDS,
    "m" -> TimeUnit.MINUTES,
    "min" -> TimeUnit.MINUTES,
    "minutes" -> TimeUnit.MINUTES,
    "h" -> TimeUnit.HOURS,
    "hours" -> TimeUnit.HOURS,
    "d"-> TimeUnit.DAYS,
    "days"-> TimeUnit.DAYS)

  /**
    * Convert a passed time string (e.g. 50s, 100ms, or 250us) to a time count for
    * internal use. If no suffix is provided a direct conversion is attempted.
    */
  def parseTimeString(timeStr: String, timeUnit: TimeUnit): Long = {

    val lower = timeStr.toLowerCase().trim();

    try {

      val m: Matcher = Pattern.compile("(-?[0-9]+)\\s?([a-z]+)?").matcher(lower)

      if (!m.matches()) {
        throw new NumberFormatException(s"Failed to parse time string: $timeStr")
      }

      val timeVal: Long = m.group(1).toLong
      val suffix: String = m.group(2)

      // Check for invalid suffixes
      if (suffix != null && !timeSuffixes.contains(suffix)) {
        throw new NumberFormatException("Invalid suffix: \"" + suffix + "\"")
      }

      // If suffix is valid use that, otherwise none was provided and use the default passed
      val convertSuffix = timeSuffixes.get(suffix) match {
        case Some(x) => x
        case None => timeUnit
      }
      return timeUnit.convert(timeVal, convertSuffix)

    } catch {

      case ex: NumberFormatException => {
        val timeError = s"Time $timeStr must be specified as seconds (s or seconds), " +
        "milliseconds (ms), microseconds (us), minutes (m or min or minutes), " +
        "hour (h or hours), or day (d or days). " +
        "e.g. 50s, 100ms, or 250us."
        throw new NumberFormatException(timeError + "\n" + ex.getMessage())
      }
    }
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to milliseconds. If
   * no suffix is provided, the passed number is assumed to be in ms.
   */
  def timeStringAsMs(timeStr: String): Long = {
    return parseTimeString(timeStr, TimeUnit.MILLISECONDS)
  }

  /**
   * Convert a time parameter such as (50s, 100ms, or 250us) to seconds. If
   * no suffix is provided, the passed number is assumed to be in seconds.
   */
  def timeStringAsSec(timeStr: String): Long = {
    return parseTimeString(timeStr, TimeUnit.SECONDS)
  }

}
