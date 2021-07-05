/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.datasource.hdfs.rdd

/** Interface which provides a sequence of filepaths */
trait HasFilePaths {

  /** sequence of file paths */
  def filePaths: Seq[String]

}
