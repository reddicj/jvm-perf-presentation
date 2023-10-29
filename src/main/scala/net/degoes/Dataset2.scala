package net.degoes

import zio.*
import scala.collection.mutable.Map as MutableMap

object dataset2 {
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

      val columns = MutableMap.empty[Field, Array[Value]]

      def updateColumns(field: Field, row: Row, rowIndex: Int): Unit = {

        val fieldValue: Value = row.map.getOrElse(field.name, null)
        if (fieldValue eq null) return

        var column = columns.getOrElse(field, null)
        if (column eq null) {
          column = Array.ofDim[Value](rows.length)
          columns.put(field, column)
        }

        column(rowIndex) = fieldValue
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
    private val data: Map[Field, Array[Value]],
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
          val fieldValue = column(rowIndex)
          if (fieldValue ne null) row.put(field.name, fieldValue)
        }
        rows(rowIndex) = Row(row.toMap)
        rowIndex += 1
      }

      Chunk.fromArray(rows)
    }

    def *(that: Dataset): Dataset =
      self.binary(that, "*") {
        case (Value.Integer(left), Value.Integer(right)) => Value.Integer(left * right)
        case (Value.Integer(left), Value.Decimal(right)) => Value.Decimal(left * right)
        case (Value.Decimal(left), Value.Decimal(right)) => Value.Decimal(left * right)
        case (Value.Decimal(left), Value.Integer(right)) => Value.Decimal(left * right)
        case (_, _)                                      => Value.NA
      }

    def +(that: Dataset): Dataset =
      self.binary(that, "+") {
        case (Value.Integer(left), Value.Integer(right)) => Value.Integer(left + right)
        case (Value.Integer(left), Value.Decimal(right)) => Value.Decimal(left + right)
        case (Value.Decimal(left), Value.Decimal(right)) => Value.Decimal(left + right)
        case (Value.Decimal(left), Value.Integer(right)) => Value.Decimal(left + right)
        case (_, _)                                      => Value.NA
      }

    def -(that: Dataset): Dataset =
      self.binary(that, "-") {
        case (Value.Integer(left), Value.Integer(right)) => Value.Integer(left - right)
        case (Value.Integer(left), Value.Decimal(right)) => Value.Decimal(left - right)
        case (Value.Decimal(left), Value.Decimal(right)) => Value.Decimal(left - right)
        case (Value.Decimal(left), Value.Integer(right)) => Value.Decimal(left - right)
        case (_, _)                                      => Value.NA
      }

    def /(that: Dataset): Dataset =
      self.binary(that, "/") {
        case (Value.Integer(left), Value.Integer(right)) => Value.Integer(left / right)
        case (Value.Integer(left), Value.Decimal(right)) => Value.Decimal(left / right)
        case (Value.Decimal(left), Value.Decimal(right)) => Value.Decimal(left / right)
        case (Value.Decimal(left), Value.Integer(right)) => Value.Decimal(left / right)
        case (_, _)                                      => Value.NA
      }

    private def binary(that: Dataset, symbol: String)(
      f: PartialFunction[(Value, Value), Value]
    ): Dataset = {

      val columns: Map[Field, Array[Value]] =
        for {
          (leftField, leftColumn)   <- self.data
          (rightField, rightColumn) <- that.data
        } yield {
          val fieldName = s"${leftField.name} $symbol ${rightField.name}"
          var rowIndex  = 0
          val result    = Array.fill[Value](leftColumn.length max rightColumn.length)(Value.NA)
          while (rowIndex < leftColumn.length && rowIndex < rightColumn.length) {
            result(rowIndex) = f(leftColumn(rowIndex), rightColumn(rowIndex))
            rowIndex += 1
          }
          Field(fieldName) -> result
        }

      new Dataset(columns, self.size max that.size)
    }
  }
}
