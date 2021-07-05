/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.hbase

import org.apache.hadoop.hbase.client.{Get, Result, Table}
import org.apache.hadoop.hbase.util.Bytes

class TableReader(table : Table) {
  def getRow(key : String) : Result = {
    val get : Get = new Get(Bytes.toBytes(key))
    table.get(get)
  }

  //  Just to check if it is there, as in the de-dup case
  def hasKey(key : String) : Boolean = {
    val get : Get = new Get(Bytes.toBytes(key))
    table.get(get).size() > 0
  }
}
