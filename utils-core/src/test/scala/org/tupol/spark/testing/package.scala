/*
MIT License

Copyright (c) 2018 Tupol (github.com/tupol)

Permission is hereby granted, free of charge, to any person obtaining a copy
of this software and associated documentation files (the "Software"), to deal
in the Software without restriction, including without limitation the rights
to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
copies of the Software, and to permit persons to whom the Software is
furnished to do so, subject to the following conditions:

The above copyright notice and this permission notice shall be included in all
copies or substantial portions of the Software.

THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
SOFTWARE.
*/
package org.tupol.spark

import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions.col

package object testing {

  class DataFrameCompareResult private (columnsOnlyInLeft: Seq[String], columnsOnlyInRight: Seq[String],
    dataOnlyInLeft: DataFrame, dataOnlyInRight: DataFrame) {

    lazy val countOnlyInLeft = dataOnlyInLeft.count
    lazy val countOnlyInRight = dataOnlyInRight.count

    def areEqual(verbose: Boolean = false) = {
      val areEqual = countOnlyInLeft == 0 && countOnlyInRight == 0 &&
        countOnlyInLeft == 0 && countOnlyInRight == 0
      if (verbose) show(areEqual)
      areEqual
    }

    def show: Unit = show(areEqual(false))

    private def show(areEqual: Boolean): Unit =
      if (!areEqual) {
        val cols = dataOnlyInLeft.columns.map(col(_))
        println(s"The data frames are different.")
        if (columnsOnlyInLeft.size > 0)
          println(s"- Column(s) ${columnsOnlyInLeft.mkString("'", "', '", "'")} appear only in the left data frame.")
        if (columnsOnlyInRight.size > 0)
          println(s"- Columns(s) ${columnsOnlyInRight.mkString("'", "', '", "'")} appear only in the right data frame.")
        if (countOnlyInLeft > 0) {
          println(s"- There are $countOnlyInLeft records only in the left data frame:")
          dataOnlyInLeft.orderBy(cols: _*).show(false)
        }
        if (countOnlyInRight > 0) {
          println(s"- There are $countOnlyInRight records only in the right data frame:")
          dataOnlyInRight.orderBy(cols: _*).show(false)
        }
      }
  }
  object DataFrameCompareResult {
    def apply(left: DataFrame, right: DataFrame, joinColumns: Seq[String] = Seq()): DataFrameCompareResult = {
      val leftCols = left.columns
      val rightCols = right.columns
      val onlyLeftCols = leftCols.filterNot(rightCols.contains)
      val onlyRightCols = rightCols.filterNot(leftCols.contains)
      val joinCols = if (joinColumns.isEmpty) leftCols.toSeq else joinColumns

      new DataFrameCompareResult(onlyLeftCols, onlyRightCols,
        left.join(right, joinCols, "left")
          .except(left.join(right, joinCols, "right")),
        left.join(right, joinCols, "right")
          .except(left.join(right, joinCols, "left")))
    }

  }

  /**
   * Naive comparison function for two data frames using the join columns.
   * The tests are superficial and do not go deep enough, but they will do for now.
   * In order to see the actual test results one needs to examine the output, [[DataFrameCompareResult]]
   * @param left `DataFrame`
   * @param right `DataFrame`
   * @param joinColumns the column names used to join the `left` and right` `DataFrame`s
   * @return `DataFrameCompareResult`
   */
  def compareDataFrames(left: DataFrame, right: DataFrame, joinColumns: Seq[String] = Seq()): DataFrameCompareResult =
    DataFrameCompareResult(left, right, joinColumns)

  implicit class DataFrameComparator(dataframe: DataFrame) {
    def compareWith(that: DataFrame, joinColumns: Seq[String] = Seq()): DataFrameCompareResult = {
      compareDataFrames(dataframe, that, joinColumns)
    }
  }

}
