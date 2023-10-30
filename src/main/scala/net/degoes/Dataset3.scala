package net.degoes

import zio.*
import scala.collection.mutable.Map as MutableMap

object dataset3 {

  sealed trait Value
  object Value {
    final case class Text(value: String)    extends Value
    final case class Integer(value: Long)   extends Value
    final case class Decimal(value: Double) extends Value
    case object NA                          extends Value
  }

  final case class Field(name: String) extends AnyVal

  final case class Row(map: Map[String, Value]) {
    def apply(field: Field): Value = map(field.name)
  }

  object Dataset {

    def fromRows(rows: Chunk[Row]): Dataset = {

      val columns = MutableMap.empty[Field, AnyRef]

      def updateColumns(field: Field, row: Row, rowIndex: Int): Unit = {

        val fieldValue: Value = row.map.getOrElse(field.name, null)
        if (fieldValue eq null) return

        def makeColumn: AnyRef = {
          val column = fieldValue match {
            case Value.Text(_)    => Array.ofDim[String](rows.length)
            case Value.Integer(_) => Array.fill(rows.length)(Long.MinValue)
            case Value.Decimal(_) => Array.fill(rows.length)(Double.MinValue)
            case Value.NA         => Array.ofDim[Value.NA.type](rows.length)
          }
          columns.put(field, column)
          column
        }

        val column = columns.getOrElse(field, makeColumn)

        fieldValue match {
          case Value.Text(value)    => column.asInstanceOf[Array[String]](rowIndex) = value
          case Value.Integer(value) => column.asInstanceOf[Array[Long]](rowIndex) = value
          case Value.Decimal(value) => column.asInstanceOf[Array[Double]](rowIndex) = value
          case Value.NA             => column.asInstanceOf[Array[Value.NA.type]](rowIndex) = Value.NA
        }
      }

      for {
        (row, index) <- rows.zipWithIndex
        fieldName    <- row.map.keys
        field         = Field(fieldName)
      } yield updateColumns(field, row, index)

      new Dataset(columns.toMap, rows.length)
    }
  }

  final class Dataset(
    private val data: Map[Field, AnyRef],
    private val size: Int
  ) { self =>

    def apply(field: Field): Dataset =
      new Dataset(Map(field -> data.getOrElse(field, Array.empty)), size)

    lazy val rows: Chunk[Row] = {

      val rows     = Array.ofDim[Row](size)
      var rowIndex = 0
      while (rowIndex < size) {
        val row = MutableMap.empty[String, Value]
        data.foreach { case (field, column) =>
          if (column.isInstanceOf[Array[Long]]) {
            val fieldValue = column.asInstanceOf[Array[Long]](rowIndex)
            if (fieldValue != Long.MinValue) row.put(field.name, Value.Integer(fieldValue))
          } else if (column.isInstanceOf[Array[Double]]) {
            val fieldValue = column.asInstanceOf[Array[Double]](rowIndex)
            if (fieldValue != Double.MinValue) row.put(field.name, Value.Decimal(fieldValue))
          } else if (column.isInstanceOf[Array[String]]) {
            val fieldValue = column.asInstanceOf[Array[String]](rowIndex)
            if (fieldValue ne null) row.put(field.name, Value.Text(fieldValue))
          } else if (column.isInstanceOf[Array[Value.NA.type]]) {
            val fieldValue = column.asInstanceOf[Array[Value.NA.type]](rowIndex)
            if (fieldValue ne null) row.put(field.name, Value.NA)
          }
        }
        rows(rowIndex) = Row(row.toMap)
        rowIndex += 1
      }

      Chunk.fromArray(rows)
    }

    def *(that: Dataset): Dataset = self.binary(that, '*')
    def +(that: Dataset): Dataset = self.binary(that, '+')
    def -(that: Dataset): Dataset = self.binary(that, '-')
    def /(that: Dataset): Dataset = self.binary(that, '/')

    private def binary(that: Dataset, symbol: Char) = {

      val columns: Map[Field, AnyRef] =
        for {
          (leftField, leftColumn)   <- self.data
          (rightField, rightColumn) <- that.data
        } yield {

          val fieldName = s"${leftField.name} $symbol ${rightField.name}"

          val column: AnyRef =
            if (leftColumn.isInstanceOf[Array[Long]] && rightColumn.isInstanceOf[Array[Long]]) {
              op(leftColumn.asInstanceOf[Array[Long]], rightColumn.asInstanceOf[Array[Long]], symbol)
            } else if (leftColumn.isInstanceOf[Array[Long]] && rightColumn.isInstanceOf[Array[Double]]) {
              op(leftColumn.asInstanceOf[Array[Long]], rightColumn.asInstanceOf[Array[Double]], symbol)
            } else if (leftColumn.isInstanceOf[Array[Double]] && rightColumn.isInstanceOf[Array[Long]]) {
              op(leftColumn.asInstanceOf[Array[Double]], rightColumn.asInstanceOf[Array[Long]], symbol)
            } else if (leftColumn.isInstanceOf[Array[Double]] && rightColumn.isInstanceOf[Array[Double]]) {
              op(leftColumn.asInstanceOf[Array[Double]], rightColumn.asInstanceOf[Array[Double]], symbol)
            } else {
              val leftColumnNA  = leftColumn.asInstanceOf[Array[?]]
              val rightColumnNA = rightColumn.asInstanceOf[Array[?]]
              Array.fill(leftColumnNA.length max rightColumnNA.length)(Value.NA)
            }

          Field(fieldName) -> column
        }

      new Dataset(columns, self.size max that.size)
    }

    private def op(i1: Long, i2: Long, symbol: Char): Long =
      if (symbol == '+') i1 + i2
      else if (symbol == '-') i1 - i2
      else if (symbol == '*') i1 * i2
      else if (symbol == '/') i1 / i2
      else Long.MinValue

    private def op(d1: Double, d2: Double, symbol: Char): Double =
      if (symbol == '+') d1 + d2
      else if (symbol == '-') d1 - d2
      else if (symbol == '*') d1 * d2
      else if (symbol == '/') d1 / d2
      else Double.MinValue

    private def op(i1: Long, d2: Double, symbol: Char): Double = op(i1.toDouble, d2, symbol)
    private def op(i1: Double, d2: Long, symbol: Char): Double = op(i1, d2.toDouble, symbol)

    private def op(a1: Array[Long], a2: Array[Long], symbol: Char): Array[Long] = {
      val result   = Array.fill[Long](a1.length max a2.length)(Long.MinValue)
      var rowIndex = 0
      while (rowIndex < a1.length && rowIndex < a2.length) {
        result(rowIndex) = op(a1(rowIndex), a2(rowIndex), symbol)
        rowIndex += 1
      }
      result
    }

    private def op(a1: Array[Long], a2: Array[Double], symbol: Char): Array[Double] = {
      val result   = Array.fill[Double](a1.length max a2.length)(Double.MinValue)
      var rowIndex = 0
      while (rowIndex < a1.length && rowIndex < a2.length) {
        result(rowIndex) = op(a1(rowIndex), a2(rowIndex), symbol)
        rowIndex += 1
      }
      result
    }

    private def op(a1: Array[Double], a2: Array[Long], symbol: Char): Array[Double] = {
      val result   = Array.fill[Double](a1.length max a2.length)(Double.MinValue)
      var rowIndex = 0
      while (rowIndex < a1.length && rowIndex < a2.length) {
        result(rowIndex) = op(a1(rowIndex), a2(rowIndex), symbol)
        rowIndex += 1
      }
      result
    }

    private def op(a1: Array[Double], a2: Array[Double], symbol: Char): Array[Double] = {
      val result   = Array.fill[Double](a1.length max a2.length)(Double.MinValue)
      var rowIndex = 0
      while (rowIndex < a1.length && rowIndex < a2.length) {
        result(rowIndex) = op(a1(rowIndex), a2(rowIndex), symbol)
        rowIndex += 1
      }
      result
    }
  }
}
