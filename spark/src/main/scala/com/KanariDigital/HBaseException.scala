/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.hbase

abstract class HBaseException(message : String) extends RuntimeException(message) {
}

final case class KerberosPrincipalNotSpecified() extends  HBaseException("Kerberos Principal not specified")
final case class KerberosKeyTabNotSpecified() extends  HBaseException("Kerberos Principal not specified")
