/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.KanariDigital.datasource.hdfs.rdd

import org.apache.spark.rdd.RDD
import org.apache.spark.rdd.UnionRDD

import org.apache.spark.{Dependency, Partition, RangeDependency, SparkContext, TaskContext}

import scala.reflect.ClassTag

/** Union RDD that implements {{HasFilePaths}} trait.
  * {{HasFilePaths}} provides list of files, which 
  *  is the source of data represented by this RDD
  */
class UnionRDDWithMetadata[T: ClassTag](
  sc: SparkContext,
  val fileRDDs: Seq[RDD[T]],
  val filePaths: Seq[String])
    extends UnionRDD(sc, fileRDDs)
    with HasFilePaths {
}
