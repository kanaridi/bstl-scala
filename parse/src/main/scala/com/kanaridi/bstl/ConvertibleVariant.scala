/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.bstl

trait ConvertibleVariant {
  def convertToString(): Option[String] = { None }
  def convertToDouble(): Option[Double] = { None }
  def convertToLong(): Option[Long] = { None }
  def convertToBoolean(): Option[Boolean] = { None }
  def convertToMap(): Option[Map[String, Any]] = { None }
  def convertToArray(): Option[Vector[Any]] = { None }
  def isNumber(): Boolean = { false }
  def isDouble(): Boolean = { false }
  def isLong(): Boolean = { false }
  def isString(): Boolean = { false }
  def isBoolean(): Boolean = { false }
  def isMap(): Boolean = { false }
  def isArray(): Boolean = { false }
}

trait ConvertibleNumberVariant extends ConvertibleVariant

case class ConvertibleDoubleVariant(value: Double) extends ConvertibleNumberVariant {
  override def convertToString(): Option[String] = { Some(s"$value") }
  override def convertToDouble(): Option[Double] = { Some(value) }
  override def convertToLong(): Option[Long] = { Some(value.toLong) }
  override def convertToBoolean(): Option[Boolean] = { Some((value != 0.0)) }
  override def isNumber() = { true }
  override def isDouble() = { true }
}

case class ConvertibleLongVariant(value: Long) extends ConvertibleNumberVariant {
  override def convertToString(): Option[String] = { Some(s"$value") }
  override def convertToDouble(): Option[Double] = { Some(value.toDouble) }
  override def convertToLong(): Option[Long] = { Some(value) }
  override def convertToBoolean(): Option[Boolean] = { Some((value != 0)) }
  override def isNumber() = { true }
  override def isLong() = { true }
}

trait ConvertibleBooleanVariant extends ConvertibleVariant {
  def value: Boolean
  override def convertToString(): Option[String] = { Some(s"$value") }
  override def convertToDouble(): Option[Double] = { Some(if (value) 1.0 else 0.0) }
  override def convertToLong(): Option[Long] = { Some(if (value) 1 else 0) }
  override def convertToBoolean(): Option[Boolean] = { Some(value) }
  override def isBoolean() = { true }
}

case class ConvertibleStringVariant(value: String) extends ConvertibleVariant {
  override def convertToString(): Option[String] = { Some(value) }
  override def convertToDouble(): Option[Double] = {
    try {
      Some(value.toDouble)
    } catch {
      case e: java.lang.NumberFormatException => None
    }
  }
  override def convertToLong(): Option[Long] = {
    try {
      Some(value.toLong)
    } catch {
      case e: java.lang.NumberFormatException => None
    }
  }
  override def convertToBoolean(): Option[Boolean] = { Some((value.length > 0)) }
  override def isString() = { true }
}

case class ConvertibleMapVariant(value: Map[String, Any]) extends ConvertibleVariant {
  override def convertToMap(): Option[Map[String,Any]] = { Some(value) }
  override def isMap() = { true }
}

case class ConvertibleArrayVariant(value: Vector[Any]) extends ConvertibleVariant {
  override def convertToArray(): Option[Vector[Any]] = { Some(value) }
  override def isArray() = { true }
}

// Boolean 
case object BstlTrue extends ConvertibleBooleanVariant { val value = true }
case object BstlFalse extends ConvertibleBooleanVariant { val value = false }

