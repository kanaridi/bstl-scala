/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.app.AuditLog

class UnitOfWork(val uofCounter : Long) extends Serializable {
  override def toString() : String = {
    s"UOW_${uofCounter.toString()}"
  }
}

object UnitOfWork {
  var nextUnitOfWork = 1L
  def createUnitOfWork() : UnitOfWork = {
    val uow = new UnitOfWork(nextUnitOfWork)
    nextUnitOfWork += 1
    uow
  }
}
