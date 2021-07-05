/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.Orderbook
import com.KanariDigital.Orderbook.Main
import org.scalatest.FlatSpec


class CommandLineArgTest extends FlatSpec {

  ignore should "work" in  {
    val testArg = "-config=file:///foo.json"
    val success = Main.processArg(testArg)
    assert(success)
    assert(Main.hBaseConfiguration.isDefined)
  }

}
