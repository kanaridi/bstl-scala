/* Copyright (c) 2020 Kanari Digital, Inc. */

package com.kanaridi.dsql

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.types._

trait Table {
  def all() : Option[DataFrame]
  def newest(till: String) = all
  def initial(from: String, till: String) : Option[DataFrame] = all
  def run(): Unit = {} 
}

trait Writer {
  def write(rows: DataFrame, options: Map[String, String]) : Long
}

class SimpleTable(df: DataFrame) extends Table {
  override def all(): Option[DataFrame] = Some(df)
}

class JoinOp(left: Table, right: Table, joinCols: Seq[String], joinType: String ) extends Table {

  def operate2(ldf: Option[DataFrame], rdf: Option[DataFrame]): Option[DataFrame] = {
    ldf match {
      case Some(l) => {
        rdf match {
          case Some(r) => Some(l.join(r, joinCols, joinType))
          case None => None
        }
      }
      case None => None
    }
  }
  override def all() = {
    operate2(left.all, right.all)
  }
  override def initial(from: String, till: String) : Option[DataFrame] = {
    operate2(left.initial(from, till), right.initial(from, till))
  }
  override def newest(till: String) : Option[DataFrame] = {
    val p1 = operate2(left.all, right.newest(till))
    val p2 = operate2(left.newest(till), right.all)

    if (p1.isEmpty) p2 else if (p2.isEmpty) None else Some(p1.get.union(p2.get))
         
  }

}


class UnionOp(left: Table, right: Table) extends Table {

  def operate2(ldf: Option[DataFrame], rdf: Option[DataFrame]): Option[DataFrame] = {
    ldf match {
      case Some(l) => {
        rdf match {
          case Some(r) => Some(l.union(r))
          case None => Some(l)
        }
      }
      case None => {
        rdf match {
          case Some(r) => Some(r)
          case None => None
        }
       }
    }
  }

  override def all() = {
    operate2(left.all, right.all)
  }

  override def initial(from: String, till: String) : Option[DataFrame] = {
    operate2(left.initial(from, till), right.initial(from, till))
  }

  override def newest(till: String) : Option[DataFrame] = {
    operate2(left.newest(till), right.newest(till))
  }

}

/** base class for operators consuming one source, and providing one sink */
trait SingleOp extends Table {
  val upstream: Table
  def operate1(upstreamDf: Option[DataFrame]) : Option[DataFrame] = upstreamDf
  override def all() = {
    operate1(upstream.all)
  }
  override def newest(till: String) = operate1(upstream.newest(till))
  override def initial(from: String, till: String) = operate1(upstream.initial(from, till))
}

/** filters out rows based on provided sql expression */
class FilterOp(val upstream: Table, val expression: String ) extends SingleOp {
  override def operate1(upstreamDf: Option[DataFrame]) : Option[DataFrame] = {
    if (upstreamDf.isEmpty) None else if(expression.length < 1) upstreamDf else Some(upstreamDf.get.filter(expression))
  }
}

/** filters out columns from provided list */
class ColsFilterOp(val upstream: Table, columns: List[String] ) extends SingleOp {
  override def operate1(upstreamDf: Option[DataFrame]) : Option[DataFrame] = {
    if (upstreamDf.isEmpty) None else Some(upstreamDf.get.drop(columns:_*))
  }
}

class SortOp(val upstream: Table, expression: String ) extends SingleOp {
  override def operate1(upstreamDf: Option[DataFrame]) = {
    if (upstreamDf.isEmpty) None else Some (upstreamDf.get.sort(expression))
  }
}

class WithColumnOp(val upstream: Table, colName: String, expression: String) extends SingleOp {

  override def operate1(upstreamDf: Option[DataFrame]) = {
    if (upstreamDf.isEmpty) None else Some(upstreamDf.get.withColumn(colName, expr(expression)))
  }

}

/** adds a list of columns to DataFrame */
class WithColumnsOp(val upstream: Table, cols: Seq[Tuple2[String, String]]) extends SingleOp {

  override def operate1(upstreamDf: Option[DataFrame]) = {
    if (upstreamDf.isEmpty)
      None
    else
      Some(cols.foldLeft(upstreamDf.get) { (dfa, tup2) =>
        dfa.withColumn(tup2._1, expr(tup2._2))
      })
  }
}

class WithColumnRenamedOp(val upstream: Table, from: String, to: String) extends SingleOp {

  override def operate1(upstreamDf: Option[DataFrame]) = {
    if (upstreamDf.isEmpty) None else Some(upstreamDf.get.withColumnRenamed(from, to))
  }

}

class ProjectOp(val upstream: Table, from: List[String], to: List[String]) extends SingleOp {

  override def operate1(upstreamDf: Option[DataFrame]) = {
    if (upstreamDf.isEmpty) None else Some(upstreamDf.get.select(from.head, from.tail:_*).toDF(to:_*))
  }

}

class LimitOp(val upstream: Table, limit: Int) extends SingleOp {

  override def operate1(upstreamDf: Option[DataFrame]) = {
    if (upstreamDf.isEmpty) None else Some(upstreamDf.get.limit(limit))
  }

}

/** relational projection -- selects named columns from */
class SelectOp(val upstream: Table, cols: Seq[String]) extends SingleOp {

  override def operate1(upstreamDf: Option[DataFrame]) = {
    if (upstreamDf.isEmpty) None else Some(upstreamDf.get.select(cols.head, cols.tail:_*))
  }

}

