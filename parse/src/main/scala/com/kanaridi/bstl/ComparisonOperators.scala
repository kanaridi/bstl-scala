/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.bstl

sealed trait ComparisonOperator {
  def apply(lhs: Any, rhs: Any): Boolean
}

// Comparison operators
sealed trait ComparisonWithOrderingOperator extends ComparisonOperator {

  protected def compare[T: Ordering](lhs: T, rhs: T): Boolean

  def apply(lhs: Any, rhs: Any): Boolean = lhs match {
    case s1: String => rhs match {
      case s2: String => compare(s1, s2)
      case _          => false
    }
    case b1: Boolean => rhs match {
      case b2: Boolean => compare(b1, b2)
      case _           => false
    }
    case i1: Int => rhs match {
      case i2: Int    => compare(i1, i2)
      case i2: Long   => compare(i1, i2)
      case i2: Double => compare(i1, i2)
      case i2: Float  => compare(i1, i2)
      case _          => false
    }
    case i1: Long => rhs match {
      case i2: Int    => compare(i1, i2)
      case i2: Long   => compare(i1, i2)
      case i2: Double => compare(i1, i2)
      case i2: Float  => compare(i1, i2)
      case _          => false
    }
    case i1: Double => rhs match {
      case i2: Int    => compare(i1, i2)
      case i2: Long   => compare(i1, i2)
      case i2: Double => compare(i1, i2)
      case i2: Float  => compare(i1, i2)
      case _          => false
    }
    case i1: Float => rhs match {
      case i2: Int    => compare(i1, i2)
      case i2: Long   => compare(i1, i2)
      case i2: Double => compare(i1, i2)
      case i2: Float  => compare(i1, i2)
      case _          => false
    }
    case _ => false
  }
}

case object EqOperator extends ComparisonOperator {
  override def apply(lhs: Any, rhs: Any): Boolean = lhs == rhs
}

case object NotEqOperator extends ComparisonOperator {
  override def apply(lhs: Any, rhs: Any): Boolean = lhs != rhs
}

case object LessOperator extends ComparisonWithOrderingOperator {
  override protected def compare[T: Ordering](lhs: T, rhs: T): Boolean = Ordering[T].lt(lhs, rhs)
}

case object GreaterOperator extends ComparisonWithOrderingOperator {
  override protected def compare[T: Ordering](lhs: T, rhs: T): Boolean = Ordering[T].gt(lhs, rhs)
}

case object LessOrEqOperator extends ComparisonWithOrderingOperator {
  override protected def compare[T: Ordering](lhs: T, rhs: T): Boolean = Ordering[T].lteq(lhs, rhs)
}

case object GreaterOrEqOperator extends ComparisonWithOrderingOperator {
  override protected def compare[T: Ordering](lhs: T, rhs: T): Boolean = Ordering[T].gteq(lhs, rhs)
}

// Binary boolean operators
sealed trait BinaryBooleanOperator {
  def apply(lhs: Boolean, rhs: Boolean): Boolean
}

case object AndOperator extends BinaryBooleanOperator {
  override def apply(lhs: Boolean, rhs: Boolean): Boolean = lhs && rhs
}

case object OrOperator extends BinaryBooleanOperator {
  override def apply(lhs: Boolean, rhs: Boolean): Boolean = lhs || rhs
}
