/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook.HdfsHiveWriter

trait HdfsHiveWriterFactoryTrait extends Serializable {
  def createHdfsHiveWriter() : Option[HdfsHiveWriter]
}
