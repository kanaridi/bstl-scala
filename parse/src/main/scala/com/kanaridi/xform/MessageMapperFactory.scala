/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.xform
import org.apache.commons.logging.Log
import org.apache.commons.logging.LogFactory


object MessageMapperFactory extends Serializable {

  def apply(config: MessageMapperConfig) : MessageMapper = {
    new MessageMapper(config)
  }
}
