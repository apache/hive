/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hadoop.hive.tools;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;

import org.apache.tools.ant.BuildException;
import org.apache.tools.ant.Task;

/**
 * This class generates java classes from the templates.
 */
public class GenVectorCode extends Task {

  private static String [][] templateExpansions =
    {

      /**
       * date is stored in a LongColumnVector as epochDays
       * interval_year_month is stored in a LongColumnVector as epochMonths
       *
       * interval_day_time and timestamp are stored in a TimestampColumnVector (2 longs to hold
       *     very large number of nanoseconds)
       *
       * date – date --> type: interval_day_time
       * timestamp – date --> type: interval_day_time
       * date – timestamp --> type: interval_day_time
       * timestamp – timestamp --> type: interval_day_time
       *
       * date +|- interval_day_time --> type: timestamp
       * interval_day_time + date --> type: timestamp
       *
       * timestamp +|- interval_day_time --> type: timestamp
       * interval_day_time +|- timestamp --> type: timestamp
       *
       * date +|- interval_year_month --> type: date
       * interval_year_month + date --> type: date
       *
       * timestamp +|- interval_year_month --> type: timestamp
       * interval_year_month + timestamp --> type: timestamp
       *
       * Adding/Subtracting months done with Calendar object
       *
       * Timestamp Compare with Long with long interpreted as seconds
       * Timestamp Compare with Double with double interpreted as seconds with fractional nanoseconds
       *
       */

      // The following datetime/interval arithmetic operations can be done using the vectorized values.
      // Type interval_year_month (LongColumnVector storing months).
      {"DTIColumnArithmeticDTIScalarNoConvert", "Add", "interval_year_month", "interval_year_month", "+"},
      {"DTIScalarArithmeticDTIColumnNoConvert", "Add", "interval_year_month", "interval_year_month", "+"},
      {"DTIColumnArithmeticDTIColumnNoConvert", "Add", "interval_year_month", "interval_year_month", "+"},

      {"DTIColumnArithmeticDTIScalarNoConvert", "Subtract", "interval_year_month", "interval_year_month", "-"},
      {"DTIScalarArithmeticDTIColumnNoConvert", "Subtract", "interval_year_month", "interval_year_month", "-"},
      {"DTIColumnArithmeticDTIColumnNoConvert", "Subtract", "interval_year_month", "interval_year_month", "-"},

      // Arithmetic on two type interval_day_time (TimestampColumnVector storing nanosecond interval
      // in 2 longs) produces a interval_day_time.
      {"TimestampArithmeticTimestamp", "Add", "interval_day_time", "Col", "interval_day_time", "Scalar"},
      {"TimestampArithmeticTimestamp", "Add", "interval_day_time", "Scalar", "interval_day_time", "Column"},
      {"TimestampArithmeticTimestamp", "Add", "interval_day_time", "Col", "interval_day_time", "Column"},

      {"TimestampArithmeticTimestamp", "Subtract", "interval_day_time", "Col", "interval_day_time", "Scalar"},
      {"TimestampArithmeticTimestamp", "Subtract", "interval_day_time", "Scalar", "interval_day_time", "Column"},
      {"TimestampArithmeticTimestamp", "Subtract", "interval_day_time", "Col", "interval_day_time", "Column"},

      // A type timestamp (TimestampColumnVector) plus/minus a type interval_day_time (TimestampColumnVector
      // storing nanosecond interval in 2 longs) produces a timestamp.
      {"TimestampArithmeticTimestamp", "Add", "interval_day_time", "Col", "timestamp", "Scalar"},
      {"TimestampArithmeticTimestamp", "Add", "interval_day_time", "Scalar", "timestamp", "Column"},
      {"TimestampArithmeticTimestamp", "Add", "interval_day_time", "Col", "timestamp", "Column"},

      {"TimestampArithmeticTimestamp", "Add", "timestamp", "Col", "interval_day_time", "Scalar"},
      {"TimestampArithmeticTimestamp", "Add", "timestamp", "Scalar", "interval_day_time", "Column"},
      {"TimestampArithmeticTimestamp", "Add", "timestamp", "Col", "interval_day_time", "Column"},

      {"TimestampArithmeticTimestamp", "Subtract", "timestamp", "Col", "interval_day_time", "Scalar"},
      {"TimestampArithmeticTimestamp", "Subtract", "timestamp", "Scalar", "interval_day_time", "Column"},
      {"TimestampArithmeticTimestamp", "Subtract", "timestamp", "Col", "interval_day_time", "Column"},

      // A type timestamp (TimestampColumnVector) minus a type timestamp produces a
      // type interval_day_time (IntervalDayTimeColumnVector storing nanosecond interval in 2 primitives).
      {"TimestampArithmeticTimestamp", "Subtract", "timestamp", "Col", "timestamp", "Scalar"},
      {"TimestampArithmeticTimestamp", "Subtract", "timestamp", "Scalar", "timestamp", "Column"},
      {"TimestampArithmeticTimestamp", "Subtract", "timestamp", "Col", "timestamp", "Column"},

      // Arithmetic with a type date (LongColumnVector storing epoch days) and type interval_day_time (IntervalDayTimeColumnVector storing
      // nanosecond interval in 2 primitives) produces a type timestamp (TimestampColumnVector).
      {"DateArithmeticTimestamp", "Add", "date", "Col", "interval_day_time", "Column"},
      {"DateArithmeticTimestamp", "Add", "date", "Scalar", "interval_day_time", "Column"},
      {"DateArithmeticTimestamp", "Add", "date", "Col", "interval_day_time", "Scalar"},

      {"DateArithmeticTimestamp", "Subtract", "date", "Col", "interval_day_time", "Column"},
      {"DateArithmeticTimestamp", "Subtract", "date", "Scalar", "interval_day_time", "Column"},
      {"DateArithmeticTimestamp", "Subtract", "date", "Col", "interval_day_time", "Scalar"},

      {"TimestampArithmeticDate", "Add", "interval_day_time", "Col", "date", "Column"},
      {"TimestampArithmeticDate", "Add", "interval_day_time", "Scalar", "date", "Column"},
      {"TimestampArithmeticDate", "Add", "interval_day_time", "Col", "date", "Scalar"},

      // Subtraction with a type date (LongColumnVector storing days) and type timestamp produces a
      // type interval_day_time (IntervalDayTimeColumnVector).
      {"DateArithmeticTimestamp", "Subtract", "date", "Col", "timestamp", "Column"},
      {"DateArithmeticTimestamp", "Subtract", "date", "Scalar", "timestamp", "Column"},
      {"DateArithmeticTimestamp", "Subtract", "date", "Col", "timestamp", "Scalar"},

      {"TimestampArithmeticDate", "Subtract", "timestamp", "Col", "date", "Column"},
      {"TimestampArithmeticDate", "Subtract", "timestamp", "Scalar", "date", "Column"},
      {"TimestampArithmeticDate", "Subtract", "timestamp", "Col", "date", "Scalar"},

      // Arithmetic with a type date (LongColumnVector storing epoch days) and type interval_year_month (LongColumnVector storing
      // months) produces a type date via a calendar calculation.
      {"DateArithmeticIntervalYearMonth", "Add", "+", "date", "Col", "interval_year_month", "Column"},
      {"DateArithmeticIntervalYearMonth", "Add", "+", "date", "Scalar", "interval_year_month", "Column"},
      {"DateArithmeticIntervalYearMonth", "Add", "+", "date", "Col", "interval_year_month", "Scalar"},

      {"DateArithmeticIntervalYearMonth", "Subtract", "-", "date", "Col", "interval_year_month", "Column"},
      {"DateArithmeticIntervalYearMonth", "Subtract", "-", "date", "Scalar", "interval_year_month", "Column"},
      {"DateArithmeticIntervalYearMonth", "Subtract", "-", "date", "Col", "interval_year_month", "Scalar"},

      {"IntervalYearMonthArithmeticDate", "Add", "+", "interval_year_month", "Col", "date", "Column"},
      {"IntervalYearMonthArithmeticDate", "Add", "+", "interval_year_month", "Scalar", "date", "Column"},
      {"IntervalYearMonthArithmeticDate", "Add", "+", "interval_year_month", "Col", "date", "Scalar"},

      // Arithmetic with a type timestamp (TimestampColumnVector) and type interval_year_month (LongColumnVector storing
      // months) produces a type timestamp via a calendar calculation.
      {"TimestampArithmeticIntervalYearMonth", "Add", "+", "timestamp", "Col", "interval_year_month", "Column"},
      {"TimestampArithmeticIntervalYearMonth", "Add", "+", "timestamp", "Scalar", "interval_year_month", "Column"},
      {"TimestampArithmeticIntervalYearMonth", "Add", "+", "timestamp", "Col", "interval_year_month", "Scalar"},

      {"TimestampArithmeticIntervalYearMonth", "Subtract", "-", "timestamp", "Col", "interval_year_month", "Column"},
      {"TimestampArithmeticIntervalYearMonth", "Subtract", "-", "timestamp", "Scalar", "interval_year_month", "Column"},
      {"TimestampArithmeticIntervalYearMonth", "Subtract", "-", "timestamp", "Col", "interval_year_month", "Scalar"},

      {"IntervalYearMonthArithmeticTimestamp", "Add","+", "interval_year_month", "Col", "timestamp", "Column"},
      {"IntervalYearMonthArithmeticTimestamp", "Add","+", "interval_year_month", "Scalar", "timestamp", "Column"},
      {"IntervalYearMonthArithmeticTimestamp", "Add","+", "interval_year_month", "Col", "timestamp", "Scalar"},

      // Long/double arithmetic
      {"ColumnArithmeticScalar", "Add", "long", "long", "+"},
      {"ColumnArithmeticScalar", "Subtract", "long", "long", "-"},
      {"ColumnArithmeticScalar", "Multiply", "long", "long", "*"},

      {"ColumnArithmeticScalar", "Add", "long", "double", "+"},
      {"ColumnArithmeticScalar", "Subtract", "long", "double", "-"},
      {"ColumnArithmeticScalar", "Multiply", "long", "double", "*"},

      {"ColumnArithmeticScalar", "Add", "double", "long", "+"},
      {"ColumnArithmeticScalar", "Subtract", "double", "long", "-"},
      {"ColumnArithmeticScalar", "Multiply", "double", "long", "*"},

      {"ColumnArithmeticScalar", "Add", "double", "double", "+"},
      {"ColumnArithmeticScalar", "Subtract", "double", "double", "-"},
      {"ColumnArithmeticScalar", "Multiply", "double", "double", "*"},

      {"ScalarArithmeticColumn", "Add", "long", "long", "+"},
      {"ScalarArithmeticColumn", "Subtract", "long", "long", "-"},
      {"ScalarArithmeticColumn", "Multiply", "long", "long", "*"},

      {"ScalarArithmeticColumn", "Add", "long", "double", "+"},
      {"ScalarArithmeticColumn", "Subtract", "long", "double", "-"},
      {"ScalarArithmeticColumn", "Multiply", "long", "double", "*"},

      {"ScalarArithmeticColumn", "Add", "double", "long", "+"},
      {"ScalarArithmeticColumn", "Subtract", "double", "long", "-"},
      {"ScalarArithmeticColumn", "Multiply", "double", "long", "*"},

      {"ScalarArithmeticColumn", "Add", "double", "double", "+"},
      {"ScalarArithmeticColumn", "Subtract", "double", "double", "-"},
      {"ScalarArithmeticColumn", "Multiply", "double", "double", "*"},

      {"ColumnArithmeticColumn", "Add", "long", "long", "+"},
      {"ColumnArithmeticColumn", "Subtract", "long", "long", "-"},
      {"ColumnArithmeticColumn", "Multiply", "long", "long", "*"},

      {"ColumnArithmeticColumn", "Add", "long", "double", "+"},
      {"ColumnArithmeticColumn", "Subtract", "long", "double", "-"},
      {"ColumnArithmeticColumn", "Multiply", "long", "double", "*"},

      {"ColumnArithmeticColumn", "Add", "double", "long", "+"},
      {"ColumnArithmeticColumn", "Subtract", "double", "long", "-"},
      {"ColumnArithmeticColumn", "Multiply", "double", "long", "*"},

      {"ColumnArithmeticColumn", "Add", "double", "double", "+"},
      {"ColumnArithmeticColumn", "Subtract", "double", "double", "-"},
      {"ColumnArithmeticColumn", "Multiply", "double", "double", "*"},


      {"ColumnDivideScalar", "Divide", "long", "double", "/"},
      {"ColumnDivideScalar", "Divide", "double", "long", "/"},
      {"ColumnDivideScalar", "Divide", "double", "double", "/"},
      {"ScalarDivideColumn", "Divide", "long", "double", "/"},
      {"ScalarDivideColumn", "Divide", "double", "long", "/"},
      {"ScalarDivideColumn", "Divide", "double", "double", "/"},
      {"ColumnDivideColumn", "Divide", "long", "double", "/"},
      {"ColumnDivideColumn", "Divide", "double", "long", "/"},
      {"ColumnDivideColumn", "Divide", "double", "double", "/"},

      {"ColumnDivideScalar", "Modulo", "long", "long", "%"},
      {"ColumnDivideScalar", "Modulo", "long", "double", "%"},
      {"ColumnDivideScalar", "Modulo", "double", "long", "%"},
      {"ColumnDivideScalar", "Modulo", "double", "double", "%"},
      {"ScalarDivideColumn", "Modulo", "long", "long", "%"},
      {"ScalarDivideColumn", "Modulo", "long", "double", "%"},
      {"ScalarDivideColumn", "Modulo", "double", "long", "%"},
      {"ScalarDivideColumn", "Modulo", "double", "double", "%"},
      {"ColumnDivideColumn", "Modulo", "long", "long", "%"},
      {"ColumnDivideColumn", "Modulo", "long", "double", "%"},
      {"ColumnDivideColumn", "Modulo", "double", "long", "%"},
      {"ColumnDivideColumn", "Modulo", "double", "double", "%"},

      {"ColumnArithmeticScalarDecimal", "Add"},
      {"ColumnArithmeticScalarDecimal", "Subtract"},
      {"ColumnArithmeticScalarDecimal", "Multiply"},

      {"ScalarArithmeticColumnDecimal", "Add"},
      {"ScalarArithmeticColumnDecimal", "Subtract"},
      {"ScalarArithmeticColumnDecimal", "Multiply"},

      {"ColumnArithmeticColumnDecimal", "Add"},
      {"ColumnArithmeticColumnDecimal", "Subtract"},
      {"ColumnArithmeticColumnDecimal", "Multiply"},

      {"ColumnDivideScalarDecimal", "Divide"},
      {"ColumnDivideScalarDecimal", "Modulo"},

      {"ScalarDivideColumnDecimal", "Divide"},
      {"ScalarDivideColumnDecimal", "Modulo"},

      {"ColumnDivideColumnDecimal", "Divide"},
      {"ColumnDivideColumnDecimal", "Modulo"},

      {"ColumnCompareScalar", "Equal", "long", "double", "=="},
      {"ColumnCompareScalar", "Equal", "double", "double", "=="},
      {"ColumnCompareScalar", "NotEqual", "long", "double", "!="},
      {"ColumnCompareScalar", "NotEqual", "double", "double", "!="},
      {"ColumnCompareScalar", "Less", "long", "double", "<"},
      {"ColumnCompareScalar", "Less", "double", "double", "<"},
      {"ColumnCompareScalar", "LessEqual", "long", "double", "<="},
      {"ColumnCompareScalar", "LessEqual", "double", "double", "<="},
      {"ColumnCompareScalar", "Greater", "long", "double", ">"},
      {"ColumnCompareScalar", "Greater", "double", "double", ">"},
      {"ColumnCompareScalar", "GreaterEqual", "long", "double", ">="},
      {"ColumnCompareScalar", "GreaterEqual", "double", "double", ">="},

      {"ColumnCompareScalar", "Equal", "double", "long", "=="},
      {"ColumnCompareScalar", "NotEqual", "double", "long", "!="},
      {"ColumnCompareScalar", "Less", "double", "long", "<"},
      {"ColumnCompareScalar", "LessEqual", "double", "long", "<="},
      {"ColumnCompareScalar", "Greater", "double", "long", ">"},
      {"ColumnCompareScalar", "GreaterEqual", "double", "long", ">="},

      {"ScalarCompareColumn", "Equal", "long", "double", "=="},
      {"ScalarCompareColumn", "Equal", "double", "double", "=="},
      {"ScalarCompareColumn", "NotEqual", "long", "double", "!="},
      {"ScalarCompareColumn", "NotEqual", "double", "double", "!="},
      {"ScalarCompareColumn", "Less", "long", "double", "<"},
      {"ScalarCompareColumn", "Less", "double", "double", "<"},
      {"ScalarCompareColumn", "LessEqual", "long", "double", "<="},
      {"ScalarCompareColumn", "LessEqual", "double", "double", "<="},
      {"ScalarCompareColumn", "Greater", "long", "double", ">"},
      {"ScalarCompareColumn", "Greater", "double", "double", ">"},
      {"ScalarCompareColumn", "GreaterEqual", "long", "double", ">="},
      {"ScalarCompareColumn", "GreaterEqual", "double", "double", ">="},

      {"ScalarCompareColumn", "Equal", "double", "long", "=="},
      {"ScalarCompareColumn", "NotEqual", "double", "long", "!="},
      {"ScalarCompareColumn", "Less", "double", "long", "<"},
      {"ScalarCompareColumn", "LessEqual", "double", "long", "<="},
      {"ScalarCompareColumn", "Greater", "double", "long", ">"},
      {"ScalarCompareColumn", "GreaterEqual", "double", "long", ">="},

      // Compare timestamp to timestamp.
      {"TimestampCompareTimestamp", "Equal", "==", "timestamp", "Col", "Column"},
      {"TimestampCompareTimestamp", "NotEqual", "!=", "timestamp", "Col", "Column"},
      {"TimestampCompareTimestamp", "Less", "<", "timestamp", "Col", "Column"},
      {"TimestampCompareTimestamp", "LessEqual", "<=", "timestamp", "Col", "Column"},
      {"TimestampCompareTimestamp", "Greater", ">", "timestamp", "Col", "Column"},
      {"TimestampCompareTimestamp", "GreaterEqual", ">=", "timestamp", "Col", "Column"},

      {"TimestampCompareTimestamp", "Equal", "==", "timestamp", "Col", "Scalar"},
      {"TimestampCompareTimestamp", "NotEqual", "!=", "timestamp", "Col", "Scalar"},
      {"TimestampCompareTimestamp", "Less", "<", "timestamp", "Col", "Scalar"},
      {"TimestampCompareTimestamp", "LessEqual", "<=", "timestamp", "Col", "Scalar"},
      {"TimestampCompareTimestamp", "Greater", ">", "timestamp", "Col", "Scalar"},
      {"TimestampCompareTimestamp", "GreaterEqual", ">=", "timestamp", "Col", "Scalar"},

      {"TimestampCompareTimestamp", "Equal", "==", "timestamp", "Scalar", "Column"},
      {"TimestampCompareTimestamp", "NotEqual", "!=", "timestamp", "Scalar", "Column"},
      {"TimestampCompareTimestamp", "Less", "<", "timestamp", "Scalar", "Column"},
      {"TimestampCompareTimestamp", "LessEqual", "<=", "timestamp", "Scalar", "Column"},
      {"TimestampCompareTimestamp", "Greater", ">", "timestamp", "Scalar", "Column"},
      {"TimestampCompareTimestamp", "GreaterEqual", ">=", "timestamp", "Scalar", "Column"},

      {"TimestampCompareTimestamp", "Equal", "==", "interval_day_time", "Col", "Column"},
      {"TimestampCompareTimestamp", "NotEqual", "!=", "interval_day_time", "Col", "Column"},
      {"TimestampCompareTimestamp", "Less", "<", "interval_day_time", "Col", "Column"},
      {"TimestampCompareTimestamp", "LessEqual", "<=", "interval_day_time", "Col", "Column"},
      {"TimestampCompareTimestamp", "Greater", ">", "interval_day_time", "Col", "Column"},
      {"TimestampCompareTimestamp", "GreaterEqual", ">=", "interval_day_time", "Col", "Column"},

      {"TimestampCompareTimestamp", "Equal", "==", "interval_day_time", "Col", "Scalar"},
      {"TimestampCompareTimestamp", "NotEqual", "!=", "interval_day_time", "Col", "Scalar"},
      {"TimestampCompareTimestamp", "Less", "<", "interval_day_time", "Col", "Scalar"},
      {"TimestampCompareTimestamp", "LessEqual", "<=", "interval_day_time", "Col", "Scalar"},
      {"TimestampCompareTimestamp", "Greater", ">", "interval_day_time", "Col", "Scalar"},
      {"TimestampCompareTimestamp", "GreaterEqual", ">=", "interval_day_time", "Col", "Scalar"},

      {"TimestampCompareTimestamp", "Equal", "==", "interval_day_time", "Scalar", "Column"},
      {"TimestampCompareTimestamp", "NotEqual", "!=", "interval_day_time", "Scalar", "Column"},
      {"TimestampCompareTimestamp", "Less", "<", "interval_day_time", "Scalar", "Column"},
      {"TimestampCompareTimestamp", "LessEqual", "<=", "interval_day_time", "Scalar", "Column"},
      {"TimestampCompareTimestamp", "Greater", ">", "interval_day_time", "Scalar", "Column"},
      {"TimestampCompareTimestamp", "GreaterEqual", ">=", "interval_day_time", "Scalar", "Column"},

      // Compare timestamp to integer seconds or double seconds with fractional nanoseonds.
      {"TimestampCompareLongDouble", "Equal", "long", "==", "Col", "Column"},
      {"TimestampCompareLongDouble", "Equal", "double", "==", "Col", "Column"},
      {"TimestampCompareLongDouble", "NotEqual", "long", "!=", "Col", "Column"},
      {"TimestampCompareLongDouble", "NotEqual", "double", "!=", "Col", "Column"},
      {"TimestampCompareLongDouble", "Less", "long", "<", "Col", "Column"},
      {"TimestampCompareLongDouble", "Less", "double", "<", "Col", "Column"},
      {"TimestampCompareLongDouble", "LessEqual", "long", "<=", "Col", "Column"},
      {"TimestampCompareLongDouble", "LessEqual", "double", "<=", "Col", "Column"},
      {"TimestampCompareLongDouble", "Greater", "long", ">", "Col", "Column"},
      {"TimestampCompareLongDouble", "Greater", "double", ">", "Col", "Column"},
      {"TimestampCompareLongDouble", "GreaterEqual", "long", ">=", "Col", "Column"},
      {"TimestampCompareLongDouble", "GreaterEqual", "double", ">=", "Col", "Column"},

      {"LongDoubleCompareTimestamp", "Equal", "long", "==", "Col", "Column"},
      {"LongDoubleCompareTimestamp", "Equal", "double", "==", "Col", "Column"},
      {"LongDoubleCompareTimestamp", "NotEqual", "long", "!=", "Col", "Column"},
      {"LongDoubleCompareTimestamp", "NotEqual", "double", "!=", "Col", "Column"},
      {"LongDoubleCompareTimestamp", "Less", "long", "<", "Col", "Column"},
      {"LongDoubleCompareTimestamp", "Less", "double", "<", "Col", "Column"},
      {"LongDoubleCompareTimestamp", "LessEqual", "long", "<=", "Col", "Column"},
      {"LongDoubleCompareTimestamp", "LessEqual", "double", "<=", "Col", "Column"},
      {"LongDoubleCompareTimestamp", "Greater", "long", ">", "Col", "Column"},
      {"LongDoubleCompareTimestamp", "Greater", "double", ">", "Col", "Column"},
      {"LongDoubleCompareTimestamp", "GreaterEqual", "long", ">=", "Col", "Column"},
      {"LongDoubleCompareTimestamp", "GreaterEqual", "double", ">=", "Col", "Column"},

      {"TimestampCompareLongDouble", "Equal", "long", "==", "Col", "Scalar"},
      {"TimestampCompareLongDouble", "Equal", "double", "==", "Col", "Scalar"},
      {"TimestampCompareLongDouble", "NotEqual", "long", "!=", "Col", "Scalar"},
      {"TimestampCompareLongDouble", "NotEqual", "double", "!=", "Col", "Scalar"},
      {"TimestampCompareLongDouble", "Less", "long", "<", "Col", "Scalar"},
      {"TimestampCompareLongDouble", "Less", "double", "<", "Col", "Scalar"},
      {"TimestampCompareLongDouble", "LessEqual", "long", "<=", "Col", "Scalar"},
      {"TimestampCompareLongDouble", "LessEqual", "double", "<=", "Col", "Scalar"},
      {"TimestampCompareLongDouble", "Greater", "long", ">", "Col", "Scalar"},
      {"TimestampCompareLongDouble", "Greater", "double", ">", "Col", "Scalar"},
      {"TimestampCompareLongDouble", "GreaterEqual", "long", ">=", "Col", "Scalar"},
      {"TimestampCompareLongDouble", "GreaterEqual", "double", ">=", "Col", "Scalar"},

      {"LongDoubleCompareTimestamp", "Equal", "long", "==", "Col", "Scalar"},
      {"LongDoubleCompareTimestamp", "Equal", "double", "==", "Col", "Scalar"},
      {"LongDoubleCompareTimestamp", "NotEqual", "long", "!=", "Col", "Scalar"},
      {"LongDoubleCompareTimestamp", "NotEqual", "double", "!=", "Col", "Scalar"},
      {"LongDoubleCompareTimestamp", "Less", "long", "<", "Col", "Scalar"},
      {"LongDoubleCompareTimestamp", "Less", "double", "<", "Col", "Scalar"},
      {"LongDoubleCompareTimestamp", "LessEqual", "long", "<=", "Col", "Scalar"},
      {"LongDoubleCompareTimestamp", "LessEqual", "double", "<=", "Col", "Scalar"},
      {"LongDoubleCompareTimestamp", "Greater", "long", ">", "Col", "Scalar"},
      {"LongDoubleCompareTimestamp", "Greater", "double", ">", "Col", "Scalar"},
      {"LongDoubleCompareTimestamp", "GreaterEqual", "long", ">=", "Col", "Scalar"},
      {"LongDoubleCompareTimestamp", "GreaterEqual", "double", ">=", "Col", "Scalar"},

      {"TimestampCompareLongDouble", "Equal", "long", "==", "Scalar", "Column"},
      {"TimestampCompareLongDouble", "Equal", "double", "==", "Scalar", "Column"},
      {"TimestampCompareLongDouble", "NotEqual", "long", "!=", "Scalar", "Column"},
      {"TimestampCompareLongDouble", "NotEqual", "double", "!=", "Scalar", "Column"},
      {"TimestampCompareLongDouble", "Less", "long", "<", "Scalar", "Column"},
      {"TimestampCompareLongDouble", "Less", "double", "<", "Scalar", "Column"},
      {"TimestampCompareLongDouble", "LessEqual", "long", "<=", "Scalar", "Column"},
      {"TimestampCompareLongDouble", "LessEqual", "double", "<=", "Scalar", "Column"},
      {"TimestampCompareLongDouble", "Greater", "long", ">", "Scalar", "Column"},
      {"TimestampCompareLongDouble", "Greater", "double", ">", "Scalar", "Column"},
      {"TimestampCompareLongDouble", "GreaterEqual", "long", ">=", "Scalar", "Column"},
      {"TimestampCompareLongDouble", "GreaterEqual", "double", ">=", "Scalar", "Column"},

      {"LongDoubleCompareTimestamp", "Equal", "long", "==", "Scalar", "Column"},
      {"LongDoubleCompareTimestamp", "Equal", "double", "==", "Scalar", "Column"},
      {"LongDoubleCompareTimestamp", "NotEqual", "long", "!=", "Scalar", "Column"},
      {"LongDoubleCompareTimestamp", "NotEqual", "double", "!=", "Scalar", "Column"},
      {"LongDoubleCompareTimestamp", "Less", "long", "<", "Scalar", "Column"},
      {"LongDoubleCompareTimestamp", "Less", "double", "<", "Scalar", "Column"},
      {"LongDoubleCompareTimestamp", "LessEqual", "long", "<=", "Scalar", "Column"},
      {"LongDoubleCompareTimestamp", "LessEqual", "double", "<=", "Scalar", "Column"},
      {"LongDoubleCompareTimestamp", "Greater", "long", ">", "Scalar", "Column"},
      {"LongDoubleCompareTimestamp", "Greater", "double", ">", "Scalar", "Column"},
      {"LongDoubleCompareTimestamp", "GreaterEqual", "long", ">=", "Scalar", "Column"},
      {"LongDoubleCompareTimestamp", "GreaterEqual", "double", ">=", "Scalar", "Column"},

      // Filter long/double.
      {"FilterColumnCompareScalar", "Equal", "long", "double", "=="},
      {"FilterColumnCompareScalar", "Equal", "double", "double", "=="},
      {"FilterColumnCompareScalar", "NotEqual", "long", "double", "!="},
      {"FilterColumnCompareScalar", "NotEqual", "double", "double", "!="},
      {"FilterColumnCompareScalar", "Less", "long", "double", "<"},
      {"FilterColumnCompareScalar", "Less", "double", "double", "<"},
      {"FilterColumnCompareScalar", "LessEqual", "long", "double", "<="},
      {"FilterColumnCompareScalar", "LessEqual", "double", "double", "<="},
      {"FilterColumnCompareScalar", "Greater", "long", "double", ">"},
      {"FilterColumnCompareScalar", "Greater", "double", "double", ">"},
      {"FilterColumnCompareScalar", "GreaterEqual", "long", "double", ">="},
      {"FilterColumnCompareScalar", "GreaterEqual", "double", "double", ">="},

      {"FilterColumnCompareScalar", "Equal", "long", "long", "=="},
      {"FilterColumnCompareScalar", "Equal", "double", "long", "=="},
      {"FilterColumnCompareScalar", "NotEqual", "long", "long", "!="},
      {"FilterColumnCompareScalar", "NotEqual", "double", "long", "!="},
      {"FilterColumnCompareScalar", "Less", "long", "long", "<"},
      {"FilterColumnCompareScalar", "Less", "double", "long", "<"},
      {"FilterColumnCompareScalar", "LessEqual", "long", "long", "<="},
      {"FilterColumnCompareScalar", "LessEqual", "double", "long", "<="},
      {"FilterColumnCompareScalar", "Greater", "long", "long", ">"},
      {"FilterColumnCompareScalar", "Greater", "double", "long", ">"},
      {"FilterColumnCompareScalar", "GreaterEqual", "long", "long", ">="},
      {"FilterColumnCompareScalar", "GreaterEqual", "double", "long", ">="},

      {"FilterScalarCompareColumn", "Equal", "long", "double", "=="},
      {"FilterScalarCompareColumn", "Equal", "double", "double", "=="},
      {"FilterScalarCompareColumn", "NotEqual", "long", "double", "!="},
      {"FilterScalarCompareColumn", "NotEqual", "double", "double", "!="},
      {"FilterScalarCompareColumn", "Less", "long", "double", "<"},
      {"FilterScalarCompareColumn", "Less", "double", "double", "<"},
      {"FilterScalarCompareColumn", "LessEqual", "long", "double", "<="},
      {"FilterScalarCompareColumn", "LessEqual", "double", "double", "<="},
      {"FilterScalarCompareColumn", "Greater", "long", "double", ">"},
      {"FilterScalarCompareColumn", "Greater", "double", "double", ">"},
      {"FilterScalarCompareColumn", "GreaterEqual", "long", "double", ">="},
      {"FilterScalarCompareColumn", "GreaterEqual", "double", "double", ">="},

      {"FilterScalarCompareColumn", "Equal", "long", "long", "=="},
      {"FilterScalarCompareColumn", "Equal", "double", "long", "=="},
      {"FilterScalarCompareColumn", "NotEqual", "long", "long", "!="},
      {"FilterScalarCompareColumn", "NotEqual", "double", "long", "!="},
      {"FilterScalarCompareColumn", "Less", "long", "long", "<"},
      {"FilterScalarCompareColumn", "Less", "double", "long", "<"},
      {"FilterScalarCompareColumn", "LessEqual", "long", "long", "<="},
      {"FilterScalarCompareColumn", "LessEqual", "double", "long", "<="},
      {"FilterScalarCompareColumn", "Greater", "long", "long", ">"},
      {"FilterScalarCompareColumn", "Greater", "double", "long", ">"},
      {"FilterScalarCompareColumn", "GreaterEqual", "long", "long", ">="},
      {"FilterScalarCompareColumn", "GreaterEqual", "double", "long", ">="},

      // Filter timestamp against timestamp, or interval day time against interval day time.

      {"FilterTimestampCompareTimestamp", "Equal", "==", "timestamp", "Col", "Column"},
      {"FilterTimestampCompareTimestamp", "NotEqual", "!=", "timestamp", "Col", "Column"},
      {"FilterTimestampCompareTimestamp", "Less", "<", "timestamp", "Col", "Column"},
      {"FilterTimestampCompareTimestamp", "LessEqual", "<=", "timestamp", "Col", "Column"},
      {"FilterTimestampCompareTimestamp", "Greater", ">", "timestamp", "Col", "Column"},
      {"FilterTimestampCompareTimestamp", "GreaterEqual", ">=", "timestamp", "Col", "Column"},

      {"FilterTimestampCompareTimestamp", "Equal", "==", "timestamp", "Col", "Scalar"},
      {"FilterTimestampCompareTimestamp", "NotEqual", "!=", "timestamp", "Col", "Scalar"},
      {"FilterTimestampCompareTimestamp", "Less", "<", "timestamp", "Col", "Scalar"},
      {"FilterTimestampCompareTimestamp", "LessEqual", "<=", "timestamp", "Col", "Scalar"},
      {"FilterTimestampCompareTimestamp", "Greater", ">", "timestamp", "Col", "Scalar"},
      {"FilterTimestampCompareTimestamp", "GreaterEqual", ">=", "timestamp", "Col", "Scalar"},

      {"FilterTimestampCompareTimestamp", "Equal", "==", "timestamp", "Scalar", "Column"},
      {"FilterTimestampCompareTimestamp", "NotEqual", "!=", "timestamp", "Scalar", "Column"},
      {"FilterTimestampCompareTimestamp", "Less", "<", "timestamp", "Scalar", "Column"},
      {"FilterTimestampCompareTimestamp", "LessEqual", "<=", "timestamp", "Scalar", "Column"},
      {"FilterTimestampCompareTimestamp", "Greater", ">", "timestamp", "Scalar", "Column"},
      {"FilterTimestampCompareTimestamp", "GreaterEqual", ">=", "timestamp", "Scalar", "Column"},

      {"FilterTimestampCompareTimestamp", "Equal", "==", "interval_day_time", "Col", "Column"},
      {"FilterTimestampCompareTimestamp", "NotEqual", "!=", "interval_day_time", "Col", "Column"},
      {"FilterTimestampCompareTimestamp", "Less", "<", "interval_day_time", "Col", "Column"},
      {"FilterTimestampCompareTimestamp", "LessEqual", "<=", "interval_day_time", "Col", "Column"},
      {"FilterTimestampCompareTimestamp", "Greater", ">", "interval_day_time", "Col", "Column"},
      {"FilterTimestampCompareTimestamp", "GreaterEqual", ">=", "interval_day_time", "Col", "Column"},

      {"FilterTimestampCompareTimestamp", "Equal", "==", "interval_day_time", "Col", "Scalar"},
      {"FilterTimestampCompareTimestamp", "NotEqual", "!=", "interval_day_time", "Col", "Scalar"},
      {"FilterTimestampCompareTimestamp", "Less", "<", "interval_day_time", "Col", "Scalar"},
      {"FilterTimestampCompareTimestamp", "LessEqual", "<=", "interval_day_time", "Col", "Scalar"},
      {"FilterTimestampCompareTimestamp", "Greater", ">", "interval_day_time", "Col", "Scalar"},
      {"FilterTimestampCompareTimestamp", "GreaterEqual", ">=", "interval_day_time", "Col", "Scalar"},

      {"FilterTimestampCompareTimestamp", "Equal", "==", "interval_day_time", "Scalar", "Column"},
      {"FilterTimestampCompareTimestamp", "NotEqual", "!=", "interval_day_time", "Scalar", "Column"},
      {"FilterTimestampCompareTimestamp", "Less", "<", "interval_day_time", "Scalar", "Column"},
      {"FilterTimestampCompareTimestamp", "LessEqual", "<=", "interval_day_time", "Scalar", "Column"},
      {"FilterTimestampCompareTimestamp", "Greater", ">", "interval_day_time", "Scalar", "Column"},
      {"FilterTimestampCompareTimestamp", "GreaterEqual", ">=", "interval_day_time", "Scalar", "Column"},

      // Filter timestamp against long (seconds) or double (seconds with fractional
      // nanoseconds).

      {"FilterTimestampCompareLongDouble", "Equal", "long", "==", "Col", "Column"},
      {"FilterTimestampCompareLongDouble", "Equal", "double", "==", "Col", "Column"},
      {"FilterTimestampCompareLongDouble", "NotEqual", "long", "!=", "Col", "Column"},
      {"FilterTimestampCompareLongDouble", "NotEqual", "double", "!=", "Col", "Column"},
      {"FilterTimestampCompareLongDouble", "Less", "long", "<", "Col", "Column"},
      {"FilterTimestampCompareLongDouble", "Less", "double", "<", "Col", "Column"},
      {"FilterTimestampCompareLongDouble", "LessEqual", "long", "<=", "Col", "Column"},
      {"FilterTimestampCompareLongDouble", "LessEqual", "double", "<=", "Col", "Column"},
      {"FilterTimestampCompareLongDouble", "Greater", "long", ">", "Col", "Column"},
      {"FilterTimestampCompareLongDouble", "Greater", "double", ">", "Col", "Column"},
      {"FilterTimestampCompareLongDouble", "GreaterEqual", "long", ">=", "Col", "Column"},
      {"FilterTimestampCompareLongDouble", "GreaterEqual", "double", ">=", "Col", "Column"},

      {"FilterLongDoubleCompareTimestamp", "Equal", "long", "==", "Col", "Column"},
      {"FilterLongDoubleCompareTimestamp", "Equal", "double", "==", "Col", "Column"},
      {"FilterLongDoubleCompareTimestamp", "NotEqual", "long", "!=", "Col", "Column"},
      {"FilterLongDoubleCompareTimestamp", "NotEqual", "double", "!=", "Col", "Column"},
      {"FilterLongDoubleCompareTimestamp", "Less", "long", "<", "Col", "Column"},
      {"FilterLongDoubleCompareTimestamp", "Less", "double", "<", "Col", "Column"},
      {"FilterLongDoubleCompareTimestamp", "LessEqual", "long", "<=", "Col", "Column"},
      {"FilterLongDoubleCompareTimestamp", "LessEqual", "double", "<=", "Col", "Column"},
      {"FilterLongDoubleCompareTimestamp", "Greater", "long", ">", "Col", "Column"},
      {"FilterLongDoubleCompareTimestamp", "Greater", "double", ">", "Col", "Column"},
      {"FilterLongDoubleCompareTimestamp", "GreaterEqual", "long", ">=", "Col", "Column"},
      {"FilterLongDoubleCompareTimestamp", "GreaterEqual", "double", ">=", "Col", "Column"},

      {"FilterTimestampCompareLongDouble", "Equal", "long", "==", "Col", "Scalar"},
      {"FilterTimestampCompareLongDouble", "Equal", "double", "==", "Col", "Scalar"},
      {"FilterTimestampCompareLongDouble", "NotEqual", "long", "!=", "Col", "Scalar"},
      {"FilterTimestampCompareLongDouble", "NotEqual", "double", "!=", "Col", "Scalar"},
      {"FilterTimestampCompareLongDouble", "Less", "long", "<", "Col", "Scalar"},
      {"FilterTimestampCompareLongDouble", "Less", "double", "<", "Col", "Scalar"},
      {"FilterTimestampCompareLongDouble", "LessEqual", "long", "<=", "Col", "Scalar"},
      {"FilterTimestampCompareLongDouble", "LessEqual", "double", "<=", "Col", "Scalar"},
      {"FilterTimestampCompareLongDouble", "Greater", "long", ">", "Col", "Scalar"},
      {"FilterTimestampCompareLongDouble", "Greater", "double", ">", "Col", "Scalar"},
      {"FilterTimestampCompareLongDouble", "GreaterEqual", "long", ">=", "Col", "Scalar"},
      {"FilterTimestampCompareLongDouble", "GreaterEqual", "double", ">=", "Col", "Scalar"},

      {"FilterLongDoubleCompareTimestamp", "Equal", "long", "==", "Col", "Scalar"},
      {"FilterLongDoubleCompareTimestamp", "Equal", "double", "==", "Col", "Scalar"},
      {"FilterLongDoubleCompareTimestamp", "NotEqual", "long", "!=", "Col", "Scalar"},
      {"FilterLongDoubleCompareTimestamp", "NotEqual", "double", "!=", "Col", "Scalar"},
      {"FilterLongDoubleCompareTimestamp", "Less", "long", "<", "Col", "Scalar"},
      {"FilterLongDoubleCompareTimestamp", "Less", "double", "<", "Col", "Scalar"},
      {"FilterLongDoubleCompareTimestamp", "LessEqual", "long", "<=", "Col", "Scalar"},
      {"FilterLongDoubleCompareTimestamp", "LessEqual", "double", "<=", "Col", "Scalar"},
      {"FilterLongDoubleCompareTimestamp", "Greater", "long", ">", "Col", "Scalar"},
      {"FilterLongDoubleCompareTimestamp", "Greater", "double", ">", "Col", "Scalar"},
      {"FilterLongDoubleCompareTimestamp", "GreaterEqual", "long", ">=", "Col", "Scalar"},
      {"FilterLongDoubleCompareTimestamp", "GreaterEqual", "double", ">=", "Col", "Scalar"},

      {"FilterTimestampCompareLongDouble", "Equal", "long", "==", "Scalar", "Column"},
      {"FilterTimestampCompareLongDouble", "Equal", "double", "==", "Scalar", "Column"},
      {"FilterTimestampCompareLongDouble", "NotEqual", "long", "!=", "Scalar", "Column"},
      {"FilterTimestampCompareLongDouble", "NotEqual", "double", "!=", "Scalar", "Column"},
      {"FilterTimestampCompareLongDouble", "Less", "long", "<", "Scalar", "Column"},
      {"FilterTimestampCompareLongDouble", "Less", "double", "<", "Scalar", "Column"},
      {"FilterTimestampCompareLongDouble", "LessEqual", "long", "<=", "Scalar", "Column"},
      {"FilterTimestampCompareLongDouble", "LessEqual", "double", "<=", "Scalar", "Column"},
      {"FilterTimestampCompareLongDouble", "Greater", "long", ">", "Scalar", "Column"},
      {"FilterTimestampCompareLongDouble", "Greater", "double", ">", "Scalar", "Column"},
      {"FilterTimestampCompareLongDouble", "GreaterEqual", "long", ">=", "Scalar", "Column"},
      {"FilterTimestampCompareLongDouble", "GreaterEqual", "double", ">=", "Scalar", "Column"},

      {"FilterLongDoubleCompareTimestamp", "Equal", "long", "==", "Scalar", "Column"},
      {"FilterLongDoubleCompareTimestamp", "Equal", "double", "==", "Scalar", "Column"},
      {"FilterLongDoubleCompareTimestamp", "NotEqual", "long", "!=", "Scalar", "Column"},
      {"FilterLongDoubleCompareTimestamp", "NotEqual", "double", "!=", "Scalar", "Column"},
      {"FilterLongDoubleCompareTimestamp", "Less", "long", "<", "Scalar", "Column"},
      {"FilterLongDoubleCompareTimestamp", "Less", "double", "<", "Scalar", "Column"},
      {"FilterLongDoubleCompareTimestamp", "LessEqual", "long", "<=", "Scalar", "Column"},
      {"FilterLongDoubleCompareTimestamp", "LessEqual", "double", "<=", "Scalar", "Column"},
      {"FilterLongDoubleCompareTimestamp", "Greater", "long", ">", "Scalar", "Column"},
      {"FilterLongDoubleCompareTimestamp", "Greater", "double", ">", "Scalar", "Column"},
      {"FilterLongDoubleCompareTimestamp", "GreaterEqual", "long", ">=", "Scalar", "Column"},
      {"FilterLongDoubleCompareTimestamp", "GreaterEqual", "double", ">=", "Scalar", "Column"},

      // String group comparison.
      {"FilterStringGroupColumnCompareStringGroupScalarBase", "Equal", "=="},
      {"FilterStringGroupColumnCompareStringGroupScalarBase", "NotEqual", "!="},
      {"FilterStringGroupColumnCompareStringGroupScalarBase", "Less", "<"},
      {"FilterStringGroupColumnCompareStringGroupScalarBase", "LessEqual", "<="},
      {"FilterStringGroupColumnCompareStringGroupScalarBase", "Greater", ">"},
      {"FilterStringGroupColumnCompareStringGroupScalarBase", "GreaterEqual", ">="},

      {"FilterStringGroupColumnCompareStringScalar", "Equal", "=="},
      {"FilterStringGroupColumnCompareStringScalar", "NotEqual", "!="},
      {"FilterStringGroupColumnCompareStringScalar", "Less", "<"},
      {"FilterStringGroupColumnCompareStringScalar", "LessEqual", "<="},
      {"FilterStringGroupColumnCompareStringScalar", "Greater", ">"},
      {"FilterStringGroupColumnCompareStringScalar", "GreaterEqual", ">="},

      {"FilterStringGroupColumnCompareTruncStringScalar", "VarChar", "Equal", "=="},
      {"FilterStringGroupColumnCompareTruncStringScalar", "VarChar", "NotEqual", "!="},
      {"FilterStringGroupColumnCompareTruncStringScalar", "VarChar", "Less", "<"},
      {"FilterStringGroupColumnCompareTruncStringScalar", "VarChar", "LessEqual", "<="},
      {"FilterStringGroupColumnCompareTruncStringScalar", "VarChar", "Greater", ">"},
      {"FilterStringGroupColumnCompareTruncStringScalar", "VarChar", "GreaterEqual", ">="},

      {"FilterStringGroupColumnCompareTruncStringScalar", "Char", "Equal", "=="},
      {"FilterStringGroupColumnCompareTruncStringScalar", "Char", "NotEqual", "!="},
      {"FilterStringGroupColumnCompareTruncStringScalar", "Char", "Less", "<"},
      {"FilterStringGroupColumnCompareTruncStringScalar", "Char", "LessEqual", "<="},
      {"FilterStringGroupColumnCompareTruncStringScalar", "Char", "Greater", ">"},
      {"FilterStringGroupColumnCompareTruncStringScalar", "Char", "GreaterEqual", ">="},

      {"FilterStringColumnBetween", ""},
      {"FilterStringColumnBetween", "!"},

      {"FilterTruncStringColumnBetween", "VarChar", ""},
      {"FilterTruncStringColumnBetween", "VarChar", "!"},

      {"FilterTruncStringColumnBetween", "Char", ""},
      {"FilterTruncStringColumnBetween", "Char", "!"},

      {"StringGroupColumnCompareStringGroupScalarBase", "Equal", "=="},
      {"StringGroupColumnCompareStringGroupScalarBase", "NotEqual", "!="},
      {"StringGroupColumnCompareStringGroupScalarBase", "Less", "<"},
      {"StringGroupColumnCompareStringGroupScalarBase", "LessEqual", "<="},
      {"StringGroupColumnCompareStringGroupScalarBase", "Greater", ">"},
      {"StringGroupColumnCompareStringGroupScalarBase", "GreaterEqual", ">="},

      {"StringGroupColumnCompareStringScalar", "Equal", "=="},
      {"StringGroupColumnCompareStringScalar", "NotEqual", "!="},
      {"StringGroupColumnCompareStringScalar", "Less", "<"},
      {"StringGroupColumnCompareStringScalar", "LessEqual", "<="},
      {"StringGroupColumnCompareStringScalar", "Greater", ">"},
      {"StringGroupColumnCompareStringScalar", "GreaterEqual", ">="},

      {"StringGroupColumnCompareTruncStringScalar", "VarChar", "Equal", "=="},
      {"StringGroupColumnCompareTruncStringScalar", "VarChar", "NotEqual", "!="},
      {"StringGroupColumnCompareTruncStringScalar", "VarChar", "Less", "<"},
      {"StringGroupColumnCompareTruncStringScalar", "VarChar", "LessEqual", "<="},
      {"StringGroupColumnCompareTruncStringScalar", "VarChar", "Greater", ">"},
      {"StringGroupColumnCompareTruncStringScalar", "VarChar", "GreaterEqual", ">="},

      {"StringGroupColumnCompareTruncStringScalar", "Char", "Equal", "=="},
      {"StringGroupColumnCompareTruncStringScalar", "Char", "NotEqual", "!="},
      {"StringGroupColumnCompareTruncStringScalar", "Char", "Less", "<"},
      {"StringGroupColumnCompareTruncStringScalar", "Char", "LessEqual", "<="},
      {"StringGroupColumnCompareTruncStringScalar", "Char", "Greater", ">"},
      {"StringGroupColumnCompareTruncStringScalar", "Char", "GreaterEqual", ">="},

      {"FilterStringGroupScalarCompareStringGroupColumnBase", "Equal", "=="},
      {"FilterStringGroupScalarCompareStringGroupColumnBase", "NotEqual", "!="},
      {"FilterStringGroupScalarCompareStringGroupColumnBase", "Less", "<"},
      {"FilterStringGroupScalarCompareStringGroupColumnBase", "LessEqual", "<="},
      {"FilterStringGroupScalarCompareStringGroupColumnBase", "Greater", ">"},
      {"FilterStringGroupScalarCompareStringGroupColumnBase", "GreaterEqual", ">="},

      {"FilterStringScalarCompareStringGroupColumn", "Equal", "=="},
      {"FilterStringScalarCompareStringGroupColumn", "NotEqual", "!="},
      {"FilterStringScalarCompareStringGroupColumn", "Less", "<"},
      {"FilterStringScalarCompareStringGroupColumn", "LessEqual", "<="},
      {"FilterStringScalarCompareStringGroupColumn", "Greater", ">"},
      {"FilterStringScalarCompareStringGroupColumn", "GreaterEqual", ">="},

      {"FilterTruncStringScalarCompareStringGroupColumn", "VarChar", "Equal", "=="},
      {"FilterTruncStringScalarCompareStringGroupColumn", "VarChar", "NotEqual", "!="},
      {"FilterTruncStringScalarCompareStringGroupColumn", "VarChar", "Less", "<"},
      {"FilterTruncStringScalarCompareStringGroupColumn", "VarChar", "LessEqual", "<="},
      {"FilterTruncStringScalarCompareStringGroupColumn", "VarChar", "Greater", ">"},
      {"FilterTruncStringScalarCompareStringGroupColumn", "VarChar", "GreaterEqual", ">="},

      {"FilterTruncStringScalarCompareStringGroupColumn", "Char", "Equal", "=="},
      {"FilterTruncStringScalarCompareStringGroupColumn", "Char", "NotEqual", "!="},
      {"FilterTruncStringScalarCompareStringGroupColumn", "Char", "Less", "<"},
      {"FilterTruncStringScalarCompareStringGroupColumn", "Char", "LessEqual", "<="},
      {"FilterTruncStringScalarCompareStringGroupColumn", "Char", "Greater", ">"},
      {"FilterTruncStringScalarCompareStringGroupColumn", "Char", "GreaterEqual", ">="},


      {"FilterDecimalColumnCompareDecimalScalar", "Equal", "=="},
      {"FilterDecimalColumnCompareDecimalScalar", "NotEqual", "!="},
      {"FilterDecimalColumnCompareDecimalScalar", "Less", "<"},
      {"FilterDecimalColumnCompareDecimalScalar", "LessEqual", "<="},
      {"FilterDecimalColumnCompareDecimalScalar", "Greater", ">"},
      {"FilterDecimalColumnCompareDecimalScalar", "GreaterEqual", ">="},

      {"FilterDecimalScalarCompareDecimalColumn", "Equal", "=="},
      {"FilterDecimalScalarCompareDecimalColumn", "NotEqual", "!="},
      {"FilterDecimalScalarCompareDecimalColumn", "Less", "<"},
      {"FilterDecimalScalarCompareDecimalColumn", "LessEqual", "<="},
      {"FilterDecimalScalarCompareDecimalColumn", "Greater", ">"},
      {"FilterDecimalScalarCompareDecimalColumn", "GreaterEqual", ">="},

      {"FilterDecimalColumnCompareDecimalColumn", "Equal", "=="},
      {"FilterDecimalColumnCompareDecimalColumn", "NotEqual", "!="},
      {"FilterDecimalColumnCompareDecimalColumn", "Less", "<"},
      {"FilterDecimalColumnCompareDecimalColumn", "LessEqual", "<="},
      {"FilterDecimalColumnCompareDecimalColumn", "Greater", ">"},
      {"FilterDecimalColumnCompareDecimalColumn", "GreaterEqual", ">="},


      {"StringGroupScalarCompareStringGroupColumnBase", "Equal", "=="},
      {"StringGroupScalarCompareStringGroupColumnBase", "NotEqual", "!="},
      {"StringGroupScalarCompareStringGroupColumnBase", "Less", "<"},
      {"StringGroupScalarCompareStringGroupColumnBase", "LessEqual", "<="},
      {"StringGroupScalarCompareStringGroupColumnBase", "Greater", ">"},
      {"StringGroupScalarCompareStringGroupColumnBase", "GreaterEqual", ">="},

      {"StringScalarCompareStringGroupColumn", "Equal", "=="},
      {"StringScalarCompareStringGroupColumn", "NotEqual", "!="},
      {"StringScalarCompareStringGroupColumn", "Less", "<"},
      {"StringScalarCompareStringGroupColumn", "LessEqual", "<="},
      {"StringScalarCompareStringGroupColumn", "Greater", ">"},
      {"StringScalarCompareStringGroupColumn", "GreaterEqual", ">="},

      {"TruncStringScalarCompareStringGroupColumn", "VarChar", "Equal", "=="},
      {"TruncStringScalarCompareStringGroupColumn", "VarChar", "NotEqual", "!="},
      {"TruncStringScalarCompareStringGroupColumn", "VarChar", "Less", "<"},
      {"TruncStringScalarCompareStringGroupColumn", "VarChar", "LessEqual", "<="},
      {"TruncStringScalarCompareStringGroupColumn", "VarChar", "Greater", ">"},
      {"TruncStringScalarCompareStringGroupColumn", "VarChar", "GreaterEqual", ">="},

      {"TruncStringScalarCompareStringGroupColumn", "Char", "Equal", "=="},
      {"TruncStringScalarCompareStringGroupColumn", "Char", "NotEqual", "!="},
      {"TruncStringScalarCompareStringGroupColumn", "Char", "Less", "<"},
      {"TruncStringScalarCompareStringGroupColumn", "Char", "LessEqual", "<="},
      {"TruncStringScalarCompareStringGroupColumn", "Char", "Greater", ">"},
      {"TruncStringScalarCompareStringGroupColumn", "Char", "GreaterEqual", ">="},

      {"FilterStringGroupColumnCompareStringGroupColumn", "Equal", "=="},
      {"FilterStringGroupColumnCompareStringGroupColumn", "NotEqual", "!="},
      {"FilterStringGroupColumnCompareStringGroupColumn", "Less", "<"},
      {"FilterStringGroupColumnCompareStringGroupColumn", "LessEqual", "<="},
      {"FilterStringGroupColumnCompareStringGroupColumn", "Greater", ">"},
      {"FilterStringGroupColumnCompareStringGroupColumn", "GreaterEqual", ">="},

      {"StringGroupColumnCompareStringGroupColumn", "Equal", "=="},
      {"StringGroupColumnCompareStringGroupColumn", "NotEqual", "!="},
      {"StringGroupColumnCompareStringGroupColumn", "Less", "<"},
      {"StringGroupColumnCompareStringGroupColumn", "LessEqual", "<="},
      {"StringGroupColumnCompareStringGroupColumn", "Greater", ">"},
      {"StringGroupColumnCompareStringGroupColumn", "GreaterEqual", ">="},

      {"FilterColumnCompareColumn", "Equal", "long", "double", "=="},
      {"FilterColumnCompareColumn", "Equal", "double", "double", "=="},
      {"FilterColumnCompareColumn", "NotEqual", "long", "double", "!="},
      {"FilterColumnCompareColumn", "NotEqual", "double", "double", "!="},
      {"FilterColumnCompareColumn", "Less", "long", "double", "<"},
      {"FilterColumnCompareColumn", "Less", "double", "double", "<"},
      {"FilterColumnCompareColumn", "LessEqual", "long", "double", "<="},
      {"FilterColumnCompareColumn", "LessEqual", "double", "double", "<="},
      {"FilterColumnCompareColumn", "Greater", "long", "double", ">"},
      {"FilterColumnCompareColumn", "Greater", "double", "double", ">"},
      {"FilterColumnCompareColumn", "GreaterEqual", "long", "double", ">="},
      {"FilterColumnCompareColumn", "GreaterEqual", "double", "double", ">="},

      {"FilterColumnCompareColumn", "Equal", "long", "long", "=="},
      {"FilterColumnCompareColumn", "Equal", "double", "long", "=="},
      {"FilterColumnCompareColumn", "NotEqual", "long", "long", "!="},
      {"FilterColumnCompareColumn", "NotEqual", "double", "long", "!="},
      {"FilterColumnCompareColumn", "Less", "long", "long", "<"},
      {"FilterColumnCompareColumn", "Less", "double", "long", "<"},
      {"FilterColumnCompareColumn", "LessEqual", "long", "long", "<="},
      {"FilterColumnCompareColumn", "LessEqual", "double", "long", "<="},
      {"FilterColumnCompareColumn", "Greater", "long", "long", ">"},
      {"FilterColumnCompareColumn", "Greater", "double", "long", ">"},
      {"FilterColumnCompareColumn", "GreaterEqual", "long", "long", ">="},
      {"FilterColumnCompareColumn", "GreaterEqual", "double", "long", ">="},

      {"FilterColumnBetween", "long", ""},
      {"FilterColumnBetween", "double", ""},
      {"FilterColumnBetween", "long", "!"},
      {"FilterColumnBetween", "double", "!"},

      {"FilterDecimalColumnBetween", ""},
      {"FilterDecimalColumnBetween", "!"},

      {"FilterTimestampColumnBetween", ""},
      {"FilterTimestampColumnBetween", "!"},

      // This is for runtime min/max pushdown - don't need to do NOT BETWEEN
      {"FilterColumnBetweenDynamicValue", "long", ""},
      {"FilterColumnBetweenDynamicValue", "double", ""},
      {"FilterColumnBetweenDynamicValue", "decimal", ""},
      {"FilterColumnBetweenDynamicValue", "string", ""},
      {"FilterColumnBetweenDynamicValue", "char", ""},
      {"FilterColumnBetweenDynamicValue", "varchar", ""},
      {"FilterColumnBetweenDynamicValue", "date", ""},
      {"FilterColumnBetweenDynamicValue", "timestamp", ""},

      {"ColumnCompareColumn", "Equal", "long", "double", "=="},
      {"ColumnCompareColumn", "Equal", "double", "double", "=="},
      {"ColumnCompareColumn", "NotEqual", "long", "double", "!="},
      {"ColumnCompareColumn", "NotEqual", "double", "double", "!="},
      {"ColumnCompareColumn", "Less", "long", "double", "<"},
      {"ColumnCompareColumn", "Less", "double", "double", "<"},
      {"ColumnCompareColumn", "LessEqual", "long", "double", "<="},
      {"ColumnCompareColumn", "LessEqual", "double", "double", "<="},
      {"ColumnCompareColumn", "Greater", "long", "double", ">"},
      {"ColumnCompareColumn", "Greater", "double", "double", ">"},
      {"ColumnCompareColumn", "GreaterEqual", "long", "double", ">="},
      {"ColumnCompareColumn", "GreaterEqual", "double", "double", ">="},

      {"ColumnCompareColumn", "Equal", "double", "long", "=="},
      {"ColumnCompareColumn", "NotEqual", "double", "long", "!="},
      {"ColumnCompareColumn", "Less", "double", "long", "<"},
      {"ColumnCompareColumn", "LessEqual", "double", "long", "<="},
      {"ColumnCompareColumn", "Greater", "double", "long", ">"},
      {"ColumnCompareColumn", "GreaterEqual", "double", "long", ">="},

      // Interval year month comparisons
      {"DTIScalarCompareColumn", "Equal", "interval_year_month"},
      {"DTIScalarCompareColumn", "NotEqual", "interval_year_month"},
      {"DTIScalarCompareColumn", "Less", "interval_year_month"},
      {"DTIScalarCompareColumn", "LessEqual", "interval_year_month"},
      {"DTIScalarCompareColumn", "Greater", "interval_year_month"},
      {"DTIScalarCompareColumn", "GreaterEqual", "interval_year_month"},

      {"DTIColumnCompareScalar", "Equal", "interval_year_month"},
      {"DTIColumnCompareScalar", "NotEqual", "interval_year_month"},
      {"DTIColumnCompareScalar", "Less", "interval_year_month"},
      {"DTIColumnCompareScalar", "LessEqual", "interval_year_month"},
      {"DTIColumnCompareScalar", "Greater", "interval_year_month"},
      {"DTIColumnCompareScalar", "GreaterEqual", "interval_year_month"},

      {"FilterDTIScalarCompareColumn", "Equal", "interval_year_month"},
      {"FilterDTIScalarCompareColumn", "NotEqual", "interval_year_month"},
      {"FilterDTIScalarCompareColumn", "Less", "interval_year_month"},
      {"FilterDTIScalarCompareColumn", "LessEqual", "interval_year_month"},
      {"FilterDTIScalarCompareColumn", "Greater", "interval_year_month"},
      {"FilterDTIScalarCompareColumn", "GreaterEqual", "interval_year_month"},

      {"FilterDTIColumnCompareScalar", "Equal", "interval_year_month"},
      {"FilterDTIColumnCompareScalar", "NotEqual", "interval_year_month"},
      {"FilterDTIColumnCompareScalar", "Less", "interval_year_month"},
      {"FilterDTIColumnCompareScalar", "LessEqual", "interval_year_month"},
      {"FilterDTIColumnCompareScalar", "Greater", "interval_year_month"},
      {"FilterDTIColumnCompareScalar", "GreaterEqual", "interval_year_month"},

      // Date comparisons
      {"DTIScalarCompareColumn", "Equal", "date"},
      {"DTIScalarCompareColumn", "NotEqual", "date"},
      {"DTIScalarCompareColumn", "Less", "date"},
      {"DTIScalarCompareColumn", "LessEqual", "date"},
      {"DTIScalarCompareColumn", "Greater", "date"},
      {"DTIScalarCompareColumn", "GreaterEqual", "date"},

      {"DTIColumnCompareScalar", "Equal", "date"},
      {"DTIColumnCompareScalar", "NotEqual", "date"},
      {"DTIColumnCompareScalar", "Less", "date"},
      {"DTIColumnCompareScalar", "LessEqual", "date"},
      {"DTIColumnCompareScalar", "Greater", "date"},
      {"DTIColumnCompareScalar", "GreaterEqual", "date"},

      {"FilterDTIScalarCompareColumn", "Equal", "date"},
      {"FilterDTIScalarCompareColumn", "NotEqual", "date"},
      {"FilterDTIScalarCompareColumn", "Less", "date"},
      {"FilterDTIScalarCompareColumn", "LessEqual", "date"},
      {"FilterDTIScalarCompareColumn", "Greater", "date"},
      {"FilterDTIScalarCompareColumn", "GreaterEqual", "date"},

      {"FilterDTIColumnCompareScalar", "Equal", "date"},
      {"FilterDTIColumnCompareScalar", "NotEqual", "date"},
      {"FilterDTIColumnCompareScalar", "Less", "date"},
      {"FilterDTIColumnCompareScalar", "LessEqual", "date"},
      {"FilterDTIColumnCompareScalar", "Greater", "date"},
      {"FilterDTIColumnCompareScalar", "GreaterEqual", "date"},

      // template, <ClassNamePrefix>, <ReturnType>, <OperandType>, <FuncName>, <OperandCast>,
      //   <ResultCast>, <Cleanup> <VectorExprArgType>
      {"ColumnUnaryFunc", "FuncRound", "double", "double", "MathExpr.round", "", "", "", ""},
      {"ColumnUnaryFunc", "FuncBRound", "double", "double", "MathExpr.bround", "", "", "", ""},
      // round(longCol) returns a long and is a no-op. So it will not be implemented here.
      // round(Col, N) is a special case and will be implemented separately from this template
      {"ColumnUnaryFunc", "FuncFloor", "long", "double", "Math.floor", "", "(long)", "", ""},
      // Floor on an integer argument is a noop, but it is less code to handle it this way.
      {"ColumnUnaryFunc", "FuncFloor", "long", "long", "Math.floor", "", "(long)", "", ""},
      {"ColumnUnaryFunc", "FuncCeil", "long", "double", "Math.ceil", "", "(long)", "", ""},
      // Ceil on an integer argument is a noop, but it is less code to handle it this way.
      {"ColumnUnaryFunc", "FuncCeil", "long", "long", "Math.ceil", "", "(long)", "", ""},
      {"ColumnUnaryFunc", "FuncExp", "double", "double", "Math.exp", "", "", "", ""},
      {"ColumnUnaryFunc", "FuncExp", "double", "long", "Math.exp", "(double)", "", "", ""},
      {"ColumnUnaryFunc", "FuncLn", "double", "double", "Math.log", "", "",
        "MathExpr.NaNToNull(outputColVector, sel, batch.selectedInUse, n, true);", ""},
      {"ColumnUnaryFunc", "FuncLn", "double", "long", "Math.log", "(double)", "",
        "MathExpr.NaNToNull(outputColVector, sel, batch.selectedInUse, n, true);", ""},
      {"ColumnUnaryFunc", "FuncLog10", "double", "double", "Math.log10", "", "",
        "MathExpr.NaNToNull(outputColVector, sel, batch.selectedInUse, n, true);", ""},
      {"ColumnUnaryFunc", "FuncLog10", "double", "long", "Math.log10", "(double)", "",
        "MathExpr.NaNToNull(outputColVector, sel, batch.selectedInUse, n, true);", ""},
      // The MathExpr class contains helper functions for cases when existing library
      // routines can't be used directly.
      {"ColumnUnaryFunc", "FuncLog2", "double", "double", "MathExpr.log2", "", "",
        "MathExpr.NaNToNull(outputColVector, sel, batch.selectedInUse, n, true);", ""},
      {"ColumnUnaryFunc", "FuncLog2", "double", "long", "MathExpr.log2", "(double)", "",
        "MathExpr.NaNToNull(outputColVector, sel, batch.selectedInUse, n, true);", ""},
      // Log(base, Col) is a special case and will be implemented separately from this template
      // Pow(col, P) and Power(col, P) are special cases implemented separately from this template
      {"ColumnUnaryFunc", "FuncSqrt", "double", "double", "Math.sqrt", "", "",
        "MathExpr.NaNToNull(outputColVector, sel, batch.selectedInUse, n);", ""},
      {"ColumnUnaryFunc", "FuncSqrt", "double", "long", "Math.sqrt", "(double)", "",
        "MathExpr.NaNToNull(outputColVector, sel, batch.selectedInUse, n);", ""},
      {"ColumnUnaryFunc", "FuncAbs", "double", "double", "Math.abs", "", "", "", ""},
      {"ColumnUnaryFunc", "FuncAbs", "long", "long", "MathExpr.abs", "", "", "", ""},
      {"ColumnUnaryFunc", "FuncSin", "double", "double", "Math.sin", "", "", "", ""},
      {"ColumnUnaryFunc", "FuncSin", "double", "long", "Math.sin", "(double)", "", "", ""},
      {"ColumnUnaryFunc", "FuncASin", "double", "double", "Math.asin", "", "", "", ""},
      {"ColumnUnaryFunc", "FuncASin", "double", "long", "Math.asin", "(double)", "", "", ""},
      {"ColumnUnaryFunc", "FuncCos", "double", "double", "Math.cos", "", "", "", ""},
      {"ColumnUnaryFunc", "FuncCos", "double", "long", "Math.cos", "(double)", "", "", ""},
      {"ColumnUnaryFunc", "FuncACos", "double", "double", "Math.acos", "", "", "", ""},
      {"ColumnUnaryFunc", "FuncACos", "double", "long", "Math.acos", "(double)", "", "", ""},
      {"ColumnUnaryFunc", "FuncTan", "double", "double", "Math.tan", "", "", "", ""},
      {"ColumnUnaryFunc", "FuncTan", "double", "long", "Math.tan", "(double)", "", "", ""},
      {"ColumnUnaryFunc", "FuncATan", "double", "double", "Math.atan", "", "", "", ""},
      {"ColumnUnaryFunc", "FuncATan", "double", "long", "Math.atan", "(double)", "", "", ""},
      {"ColumnUnaryFunc", "FuncDegrees", "double", "double", "Math.toDegrees", "", "", "", ""},
      {"ColumnUnaryFunc", "FuncDegrees", "double", "long", "Math.toDegrees", "(double)", "", "", ""},
      {"ColumnUnaryFunc", "FuncRadians", "double", "double", "Math.toRadians", "", "", "", ""},
      {"ColumnUnaryFunc", "FuncRadians", "double", "long", "Math.toRadians", "(double)", "", "", ""},
      {"ColumnUnaryFunc", "FuncSign", "double", "double", "MathExpr.sign", "", "", "", ""},
      {"ColumnUnaryFunc", "FuncSign", "double", "long", "MathExpr.sign", "(double)", "", "", ""},

      {"DecimalColumnUnaryFunc", "FuncFloor", "decimal", "DecimalUtil.floor"},
      {"DecimalColumnUnaryFunc", "FuncCeil", "decimal", "DecimalUtil.ceiling"},
      {"DecimalColumnUnaryFunc", "FuncAbs", "decimal", "DecimalUtil.abs"},
      {"DecimalColumnUnaryFunc", "FuncSign", "long", "DecimalUtil.sign"},
      {"DecimalColumnUnaryFunc", "FuncRound", "decimal", "DecimalUtil.round"},
      {"DecimalColumnUnaryFunc", "FuncBRound", "decimal", "DecimalUtil.bround"},
      {"DecimalColumnUnaryFunc", "FuncNegate", "decimal", "DecimalUtil.negate"},

      // Casts
      {"ColumnUnaryFunc", "Cast", "long", "double", "", "", "(long)", "", ""},
      {"ColumnUnaryFunc", "Cast", "double", "long", "", "", "(double)", "", ""},
      {"ColumnUnaryFunc", "CastLongToFloatVia", "double", "long", "", "", "(float)", "", ""},
      {"ColumnUnaryFunc", "CastDoubleToBooleanVia", "long", "double", "MathExpr.toBool", "",
        "", "", ""},
      {"ColumnUnaryFunc", "CastLongToBooleanVia", "long", "long", "MathExpr.toBool", "",
        "", "", ""},
      {"ColumnUnaryFunc", "CastDateToBooleanVia", "long", "long", "MathExpr.toBool", "",
            "", "", "date"},

      // Boolean to long is done with an IdentityExpression
      // Boolean to double is done with standard Long to Double cast
      // See org.apache.hadoop.hive.ql.exec.vector.expressions for remaining cast VectorExpression
      // classes

      {"ColumnUnaryMinus", "long"},
      {"ColumnUnaryMinus", "double"},

      // IF conditional expression
      // fileHeader, resultType, arg2Type, arg3Type
      {"IfExprColumnScalar", "long", "long"},
      {"IfExprColumnScalar", "double", "long"},
      {"IfExprColumnScalar", "long", "double"},
      {"IfExprColumnScalar", "double", "double"},
      {"IfExprScalarColumn", "long", "long"},
      {"IfExprScalarColumn", "double", "long"},
      {"IfExprScalarColumn", "long", "double"},
      {"IfExprScalarColumn", "double", "double"},
      {"IfExprScalarScalar", "long", "long"},
      {"IfExprScalarScalar", "double", "long"},
      {"IfExprScalarScalar", "long", "double"},
      {"IfExprScalarScalar", "double", "double"},

      // template, <ClassName>, <ValueType>, <OperatorSymbol>, <DescriptionName>, <DescriptionValue>
      {"VectorUDAFMinMax", "VectorUDAFMinLong", "long", "<", "min",
          "_FUNC_(expr) - Returns the minimum value of expr (vectorized, type: long)"},
      {"VectorUDAFMinMax", "VectorUDAFMinDouble", "double", "<", "min",
          "_FUNC_(expr) - Returns the minimum value of expr (vectorized, type: double)"},
      {"VectorUDAFMinMax", "VectorUDAFMaxLong", "long", ">", "max",
          "_FUNC_(expr) - Returns the maximum value of expr (vectorized, type: long)"},
      {"VectorUDAFMinMax", "VectorUDAFMaxDouble", "double", ">", "max",
          "_FUNC_(expr) - Returns the maximum value of expr (vectorized, type: double)"},

      {"VectorUDAFMinMaxDecimal", "VectorUDAFMaxDecimal", "<", "max",
          "_FUNC_(expr) - Returns the maximum value of expr (vectorized, type: decimal)"},
      {"VectorUDAFMinMaxDecimal", "VectorUDAFMinDecimal", ">", "min",
          "_FUNC_(expr) - Returns the minimum value of expr (vectorized, type: decimal)"},

      {"VectorUDAFMinMaxString", "VectorUDAFMinString", "<", "min",
          "_FUNC_(expr) - Returns the minimum value of expr (vectorized, type: string)"},
      {"VectorUDAFMinMaxString", "VectorUDAFMaxString", ">", "max",
          "_FUNC_(expr) - Returns the minimum value of expr (vectorized, type: string)"},

      {"VectorUDAFMinMaxTimestamp", "VectorUDAFMaxTimestamp", "<", "max",
          "_FUNC_(expr) - Returns the maximum value of expr (vectorized, type: timestamp)"},
      {"VectorUDAFMinMaxTimestamp", "VectorUDAFMinTimestamp", ">", "min",
          "_FUNC_(expr) - Returns the minimum value of expr (vectorized, type: timestamp)"},

      {"VectorUDAFMinMaxIntervalDayTime", "VectorUDAFMaxIntervalDayTime", "<", "max",
          "_FUNC_(expr) - Returns the maximum value of expr (vectorized, type: interval_day_time)"},
      {"VectorUDAFMinMaxIntervalDayTime", "VectorUDAFMinIntervalDayTime", ">", "min",
          "_FUNC_(expr) - Returns the minimum value of expr (vectorized, type: interval_day_time)"},

        //template, <ClassName>, <ValueType>
        {"VectorUDAFSum", "VectorUDAFSumLong", "long"},
        {"VectorUDAFSum", "VectorUDAFSumDouble", "double"},
        {"VectorUDAFAvg", "VectorUDAFAvgLong", "long"},
        {"VectorUDAFAvg", "VectorUDAFAvgDouble", "double"},

      // template, <ClassName>, <ValueType>, <VarianceFormula>, <DescriptionName>,
      // <DescriptionValue>
      {"VectorUDAFVar", "VectorUDAFVarPopLong", "long", "myagg.variance / myagg.count",
          "variance, var_pop",
          "_FUNC_(x) - Returns the variance of a set of numbers (vectorized, long)"},
      {"VectorUDAFVar", "VectorUDAFVarPopDouble", "double", "myagg.variance / myagg.count",
          "variance, var_pop",
          "_FUNC_(x) - Returns the variance of a set of numbers (vectorized, double)"},
      {"VectorUDAFVarDecimal", "VectorUDAFVarPopDecimal", "myagg.variance / myagg.count",
          "variance, var_pop",
          "_FUNC_(x) - Returns the variance of a set of numbers (vectorized, decimal)"},
      {"VectorUDAFVar", "VectorUDAFVarSampLong", "long", "myagg.variance / (myagg.count-1.0)",
          "var_samp",
          "_FUNC_(x) - Returns the sample variance of a set of numbers (vectorized, long)"},
      {"VectorUDAFVar", "VectorUDAFVarSampDouble", "double", "myagg.variance / (myagg.count-1.0)",
          "var_samp",
          "_FUNC_(x) - Returns the sample variance of a set of numbers (vectorized, double)"},
      {"VectorUDAFVarDecimal", "VectorUDAFVarSampDecimal", "myagg.variance / (myagg.count-1.0)",
          "var_samp",
          "_FUNC_(x) - Returns the sample variance of a set of numbers (vectorized, decimal)"},
      {"VectorUDAFVar", "VectorUDAFStdPopLong", "long",
          "Math.sqrt(myagg.variance / (myagg.count))", "std,stddev,stddev_pop",
          "_FUNC_(x) - Returns the standard deviation of a set of numbers (vectorized, long)"},
      {"VectorUDAFVar", "VectorUDAFStdPopDouble", "double",
          "Math.sqrt(myagg.variance / (myagg.count))", "std,stddev,stddev_pop",
          "_FUNC_(x) - Returns the standard deviation of a set of numbers (vectorized, double)"},
      {"VectorUDAFVarDecimal", "VectorUDAFStdPopDecimal",
          "Math.sqrt(myagg.variance / (myagg.count))", "std,stddev,stddev_pop",
          "_FUNC_(x) - Returns the standard deviation of a set of numbers (vectorized, decimal)"},
      {"VectorUDAFVar", "VectorUDAFStdSampLong", "long",
          "Math.sqrt(myagg.variance / (myagg.count-1.0))", "stddev_samp",
          "_FUNC_(x) - Returns the sample standard deviation of a set of numbers (vectorized, long)"},
      {"VectorUDAFVar", "VectorUDAFStdSampDouble", "double",
          "Math.sqrt(myagg.variance / (myagg.count-1.0))", "stddev_samp",
          "_FUNC_(x) - Returns the sample standard deviation of a set of numbers (vectorized, double)"},
      {"VectorUDAFVarDecimal", "VectorUDAFStdSampDecimal",
          "Math.sqrt(myagg.variance / (myagg.count-1.0))", "stddev_samp",
          "_FUNC_(x) - Returns the sample standard deviation of a set of numbers (vectorized, decimal)"},

    };


  private String templateBaseDir;
  private String buildDir;

  private String expressionOutputDirectory;
  private String expressionClassesDirectory;
  private String expressionTemplateDirectory;
  private String udafOutputDirectory;
  private String udafClassesDirectory;
  private String udafTemplateDirectory;
  private GenVectorTestCode testCodeGen;

  static String joinPath(String...parts) {
    String path = parts[0];
    for (int i=1; i < parts.length; ++i) {
      path += File.separatorChar + parts[i];
    }
    return path;
  }

  public void init(String templateBaseDir, String buildDir) {
    File generationDirectory = new File(templateBaseDir);

    String buildPath = joinPath(buildDir, "generated-sources", "java");
    String compiledPath = joinPath(buildDir, "classes");

    String expression = joinPath("org", "apache", "hadoop",
        "hive", "ql", "exec", "vector", "expressions", "gen");
    File exprOutput = new File(joinPath(buildPath, expression));
    File exprClasses = new File(joinPath(compiledPath, expression));
    expressionOutputDirectory = exprOutput.getAbsolutePath();
    expressionClassesDirectory = exprClasses.getAbsolutePath();

    expressionTemplateDirectory =
        joinPath(generationDirectory.getAbsolutePath(), "ExpressionTemplates");

    String udaf = joinPath("org", "apache", "hadoop",
        "hive", "ql", "exec", "vector", "expressions", "aggregates", "gen");
    File udafOutput = new File(joinPath(buildPath, udaf));
    File udafClasses = new File(joinPath(compiledPath, udaf));
    udafOutputDirectory = udafOutput.getAbsolutePath();
    udafClassesDirectory = udafClasses.getAbsolutePath();

    udafTemplateDirectory =
        joinPath(generationDirectory.getAbsolutePath(), "UDAFTemplates");

    File testCodeOutput =
        new File(
            joinPath(buildDir, "generated-test-sources", "java", "org",
                "apache", "hadoop", "hive", "ql", "exec", "vector",
                "expressions", "gen"));
    testCodeGen = new GenVectorTestCode(testCodeOutput.getAbsolutePath(),
        joinPath(generationDirectory.getAbsolutePath(), "TestTemplates"));
  }

  /**
   * @param args
   * @throws Exception
   */
  public static void main(String[] args) throws Exception {
    GenVectorCode gen = new GenVectorCode();
    gen.init(System.getProperty("user.dir"),
        joinPath(System.getProperty("user.dir"), "..", "..", "..", "..", "build"));
    gen.generate();
  }

  @Override
  public void execute() throws BuildException {
    init(templateBaseDir, buildDir);
    try {
      this.generate();
    } catch (Exception e) {
      throw new BuildException(e);
    }
  }

  private void generate() throws Exception {
    System.out.println("Generating vector expression code");
    for (String [] tdesc : templateExpansions) {
      if (tdesc[0].equals("ColumnArithmeticScalar") || tdesc[0].equals("ColumnDivideScalar")) {
        generateColumnArithmeticScalar(tdesc);
      } else if (tdesc[0].equals("ColumnArithmeticScalarDecimal")) {
        generateColumnArithmeticScalarDecimal(tdesc);
      } else if (tdesc[0].equals("ScalarArithmeticColumnDecimal")) {
        generateScalarArithmeticColumnDecimal(tdesc);
      } else if (tdesc[0].equals("ColumnArithmeticColumnDecimal")) {
        generateColumnArithmeticColumnDecimal(tdesc);
      } else if (tdesc[0].equals("ColumnDivideScalarDecimal")) {
        generateColumnDivideScalarDecimal(tdesc);
      } else if (tdesc[0].equals("ScalarDivideColumnDecimal")) {
        generateScalarDivideColumnDecimal(tdesc);
      } else if (tdesc[0].equals("ColumnDivideColumnDecimal")) {
        generateColumnDivideColumnDecimal(tdesc);
      } else if (tdesc[0].equals("ColumnCompareScalar")) {
        generateColumnCompareScalar(tdesc);
      } else if (tdesc[0].equals("ScalarCompareColumn")) {
        generateScalarCompareColumn(tdesc);

      } else if (tdesc[0].equals("TimestampCompareTimestamp")) {
        generateTimestampCompareTimestamp(tdesc);

      } else if (tdesc[0].equals("TimestampCompareLongDouble")) {
        generateTimestampCompareLongDouble(tdesc);

      } else if (tdesc[0].equals("LongDoubleCompareTimestamp")) {
        generateLongDoubleCompareTimestamp(tdesc);

      } else if (tdesc[0].equals("FilterColumnCompareScalar")) {
        generateFilterColumnCompareScalar(tdesc);
      } else if (tdesc[0].equals("FilterScalarCompareColumn")) {
        generateFilterScalarCompareColumn(tdesc);

      } else if (tdesc[0].equals("FilterTimestampCompareTimestamp")) {
        generateFilterTimestampCompareTimestamp(tdesc);

      } else if (tdesc[0].equals("FilterTimestampCompareLongDouble")) {
        generateFilterTimestampCompareLongDouble(tdesc);

      } else if (tdesc[0].equals("FilterLongDoubleCompareTimestamp")) {
        generateFilterLongDoubleCompareTimestamp(tdesc);

      } else if (tdesc[0].equals("FilterColumnBetween")) {
        generateFilterColumnBetween(tdesc);
      } else if (tdesc[0].equals("FilterColumnBetweenDynamicValue")) {
        generateFilterColumnBetweenDynamicValue(tdesc);
      } else if (tdesc[0].equals("ScalarArithmeticColumn") || tdesc[0].equals("ScalarDivideColumn")) {
        generateScalarArithmeticColumn(tdesc);
      } else if (tdesc[0].equals("FilterColumnCompareColumn")) {
        generateFilterColumnCompareColumn(tdesc);
      } else if (tdesc[0].equals("ColumnCompareColumn")) {
        generateColumnCompareColumn(tdesc);
      } else if (tdesc[0].equals("ColumnArithmeticColumn") || tdesc[0].equals("ColumnDivideColumn")) {
        generateColumnArithmeticColumn(tdesc);
      } else if (tdesc[0].equals("ColumnUnaryMinus")) {
        generateColumnUnaryMinus(tdesc);
      } else if (tdesc[0].equals("ColumnUnaryFunc")) {
        generateColumnUnaryFunc(tdesc);
      } else if (tdesc[0].equals("DecimalColumnUnaryFunc")) {
        generateDecimalColumnUnaryFunc(tdesc);
      } else if (tdesc[0].equals("VectorUDAFMinMax")) {
        generateVectorUDAFMinMax(tdesc);
      } else if (tdesc[0].equals("VectorUDAFMinMaxString")) {
        generateVectorUDAFMinMaxString(tdesc);
      } else if (tdesc[0].equals("VectorUDAFMinMaxDecimal")) {
        generateVectorUDAFMinMaxObject(tdesc);
      } else if (tdesc[0].equals("VectorUDAFMinMaxTimestamp")) {
        generateVectorUDAFMinMaxObject(tdesc);
      } else if (tdesc[0].equals("VectorUDAFMinMaxIntervalDayTime")) {
        generateVectorUDAFMinMaxObject(tdesc);
      } else if (tdesc[0].equals("VectorUDAFSum")) {
        generateVectorUDAFSum(tdesc);
      } else if (tdesc[0].equals("VectorUDAFAvg")) {
        generateVectorUDAFAvg(tdesc);
      } else if (tdesc[0].equals("VectorUDAFVar")) {
        generateVectorUDAFVar(tdesc);
      } else if (tdesc[0].equals("VectorUDAFVarDecimal")) {
        generateVectorUDAFVarDecimal(tdesc);
      } else if (tdesc[0].equals("FilterStringGroupColumnCompareStringGroupScalarBase")) {
        generateFilterStringGroupColumnCompareStringGroupScalarBase(tdesc);
      } else if (tdesc[0].equals("FilterStringGroupColumnCompareStringScalar")) {
        generateFilterStringGroupColumnCompareStringScalar(tdesc);
      } else if (tdesc[0].equals("FilterStringGroupColumnCompareTruncStringScalar")) {
        generateFilterStringGroupColumnCompareTruncStringScalar(tdesc);
      } else if (tdesc[0].equals("FilterStringColumnBetween")) {
        generateFilterStringColumnBetween(tdesc);
      } else if (tdesc[0].equals("FilterTruncStringColumnBetween")) {
        generateFilterTruncStringColumnBetween(tdesc);
      } else if (tdesc[0].equals("FilterDecimalColumnBetween")) {
        generateFilterDecimalColumnBetween(tdesc);
      } else if (tdesc[0].equals("FilterTimestampColumnBetween")) {
        generateFilterTimestampColumnBetween(tdesc);
       } else if (tdesc[0].equals("StringGroupColumnCompareStringGroupScalarBase")) {
        generateStringGroupColumnCompareStringGroupScalarBase(tdesc);
      } else if (tdesc[0].equals("StringGroupColumnCompareStringScalar")) {
        generateStringGroupColumnCompareStringScalar(tdesc);
      } else if (tdesc[0].equals("StringGroupColumnCompareTruncStringScalar")) {
        generateStringGroupColumnCompareTruncStringScalar(tdesc);
      } else if (tdesc[0].equals("FilterStringGroupScalarCompareStringGroupColumnBase")) {
        generateFilterStringGroupScalarCompareStringGroupColumnBase(tdesc);
      } else if (tdesc[0].equals("FilterStringScalarCompareStringGroupColumn")) {
        generateFilterStringScalarCompareStringGroupColumn(tdesc);
      } else if (tdesc[0].equals("FilterTruncStringScalarCompareStringGroupColumn")) {
        generateFilterTruncStringScalarCompareStringGroupColumn(tdesc);
      } else if (tdesc[0].equals("StringGroupScalarCompareStringGroupColumnBase")) {
        generateStringGroupScalarCompareStringGroupColumnBase(tdesc);
      } else if (tdesc[0].equals("StringScalarCompareStringGroupColumn")) {
        generateStringScalarCompareStringGroupColumn(tdesc);
      } else if (tdesc[0].equals("TruncStringScalarCompareStringGroupColumn")) {
        generateTruncStringScalarCompareStringGroupColumn(tdesc);
      } else if (tdesc[0].equals("FilterStringGroupColumnCompareStringGroupColumn")) {
        generateFilterStringGroupColumnCompareStringGroupColumn(tdesc);
      } else if (tdesc[0].equals("StringGroupColumnCompareStringGroupColumn")) {
        generateStringGroupColumnCompareStringGroupColumn(tdesc);
      } else if (tdesc[0].equals("IfExprColumnScalar")) {
        generateIfExprColumnScalar(tdesc);
      } else if (tdesc[0].equals("IfExprScalarColumn")) {
        generateIfExprScalarColumn(tdesc);
      } else if (tdesc[0].equals("IfExprScalarScalar")) {
        generateIfExprScalarScalar(tdesc);
      } else if (tdesc[0].equals("FilterDecimalColumnCompareDecimalScalar")) {
        generateFilterDecimalColumnCompareDecimalScalar(tdesc);
      } else if (tdesc[0].equals("FilterDecimalScalarCompareDecimalColumn")) {
        generateFilterDecimalScalarCompareDecimalColumn(tdesc);
      } else if (tdesc[0].equals("FilterDecimalColumnCompareDecimalColumn")) {
        generateFilterDecimalColumnCompareDecimalColumn(tdesc);
      } else if (tdesc[0].equals("FilterDTIScalarCompareColumn")) {
        generateFilterDTIScalarCompareColumn(tdesc);
      } else if (tdesc[0].equals("FilterDTIColumnCompareScalar")) {
        generateFilterDTIColumnCompareScalar(tdesc);
      } else if (tdesc[0].equals("DTIScalarCompareColumn")) {
        generateDTIScalarCompareColumn(tdesc);
      } else if (tdesc[0].equals("DTIColumnCompareScalar")) {
        generateDTIColumnCompareScalar(tdesc);
      } else if (tdesc[0].equals("DTIColumnArithmeticDTIScalarNoConvert")) {
        generateColumnArithmeticScalar(tdesc);
      } else if (tdesc[0].equals("DTIScalarArithmeticDTIColumnNoConvert")) {
        generateScalarArithmeticColumn(tdesc);
      } else if (tdesc[0].equals("DTIColumnArithmeticDTIColumnNoConvert")) {
        generateColumnArithmeticColumn(tdesc);

      } else if (tdesc[0].equals("DateArithmeticIntervalYearMonth")) {
        generateDateTimeArithmeticIntervalYearMonth(tdesc);

      } else if (tdesc[0].equals("IntervalYearMonthArithmeticDate")) {
        generateDateTimeArithmeticIntervalYearMonth(tdesc);

      } else if (tdesc[0].equals("TimestampArithmeticIntervalYearMonth")) {
        generateDateTimeArithmeticIntervalYearMonth(tdesc);

      } else if (tdesc[0].equals("IntervalYearMonthArithmeticTimestamp")) {
        generateDateTimeArithmeticIntervalYearMonth(tdesc);

      } else if (tdesc[0].equals("TimestampArithmeticTimestamp")) {
        generateTimestampArithmeticTimestamp(tdesc);

      } else if (tdesc[0].equals("DateArithmeticTimestamp")) {
        generateDateArithmeticTimestamp(tdesc);

      } else if (tdesc[0].equals("TimestampArithmeticDate")) {
        generateTimestampArithmeticDate(tdesc);

      } else {
        continue;
      }
    }
    System.out.println("Generating vector expression test code");
    testCodeGen.generateTestSuites();
  }

  private void generateFilterStringColumnBetween(String[] tdesc) throws IOException {
    String optionalNot = tdesc[1];
    String className = "FilterStringColumn" + (optionalNot.equals("!") ? "Not" : "")
        + "Between";
        // Read the template into a string, expand it, and write it.
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<OptionalNot>", optionalNot);

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterTruncStringColumnBetween(String[] tdesc) throws IOException {
    String truncStringTypeName = tdesc[1];
    String truncStringHiveType;
    String truncStringHiveGetBytes;
    if (truncStringTypeName == "Char") {
      truncStringHiveType = "HiveChar";
      truncStringHiveGetBytes = "getStrippedValue().getBytes()";
    } else if (truncStringTypeName == "VarChar") {
      truncStringHiveType = "HiveVarchar";
      truncStringHiveGetBytes = "getValue().getBytes()";
    } else {
      throw new Error("Unsupported string type: " + truncStringTypeName);
    }
    String optionalNot = tdesc[2];
    String className = "Filter" + truncStringTypeName + "Column" + (optionalNot.equals("!") ? "Not" : "")
        + "Between";
        // Read the template into a string, expand it, and write it.
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<TruncStringTypeName>", truncStringTypeName);
    templateString = templateString.replaceAll("<TruncStringHiveType>", truncStringHiveType);
    templateString = templateString.replaceAll("<TruncStringHiveGetBytes>", truncStringHiveGetBytes);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<OptionalNot>", optionalNot);

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterDecimalColumnBetween(String[] tdesc) throws IOException {
    String optionalNot = tdesc[1];
    String className = "FilterDecimalColumn" + (optionalNot.equals("!") ? "Not" : "")
        + "Between";
    // Read the template into a string, expand it, and write it.
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<OptionalNot>", optionalNot);

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterTimestampColumnBetween(String[] tdesc) throws IOException {
    String optionalNot = tdesc[1];
    String className = "FilterTimestampColumn" + (optionalNot.equals("!") ? "Not" : "")
        + "Between";
    // Read the template into a string, expand it, and write it.
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<OptionalNot>", optionalNot);

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterColumnBetween(String[] tdesc) throws Exception {
    String operandType = tdesc[1];
    String optionalNot = tdesc[2];

    String className = "Filter" + getCamelCaseType(operandType) + "Column" +
      (optionalNot.equals("!") ? "Not" : "") + "Between";
    String inputColumnVectorType = getColumnVectorType(operandType);

    // Read the template into a string, expand it, and write it.
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<InputColumnVectorType>", inputColumnVectorType);
    templateString = templateString.replaceAll("<OperandType>", operandType);
    templateString = templateString.replaceAll("<OptionalNot>", optionalNot);

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterColumnBetweenDynamicValue(String[] tdesc) throws Exception {
    String operandType = tdesc[1];
    String optionalNot = tdesc[2];

    String className = "Filter" + getCamelCaseType(operandType) + "Column" +
      (optionalNot.equals("!") ? "Not" : "") + "BetweenDynamicValue";

    String typeName = getCamelCaseType(operandType);
    String defaultValue;
    String vectorType;
    String getPrimitiveMethod;
    String getValueMethod;
    String conversionMethod;

    if (operandType.equals("long")) {
      defaultValue = "0";
      vectorType = "long";
      getPrimitiveMethod = "getLong";
      getValueMethod = "";
      conversionMethod = "";
    } else if (operandType.equals("double")) {
      defaultValue = "0";
      vectorType = "double";
      getPrimitiveMethod = "getDouble";
      getValueMethod = "";
      conversionMethod = "";
    } else if (operandType.equals("decimal")) {
      defaultValue = "HiveDecimal.ZERO";
      vectorType = "HiveDecimal";
      getPrimitiveMethod = "getHiveDecimal";
      getValueMethod = "";
      conversionMethod = "";
    } else if (operandType.equals("string")) {
      defaultValue = "null";
      vectorType = "byte[]";
      getPrimitiveMethod = "getString";
      getValueMethod = ".getBytes()";
      conversionMethod = "";
    } else if (operandType.equals("char")) {
      defaultValue = "new HiveChar(\"\", 1)";
      vectorType = "byte[]";
      getPrimitiveMethod = "getHiveChar";
      getValueMethod = ".getStrippedValue().getBytes()";  // Does vectorization use stripped char values?
      conversionMethod = "";
    } else if (operandType.equals("varchar")) {
      defaultValue = "new HiveVarchar(\"\", 1)";
      vectorType = "byte[]";
      getPrimitiveMethod = "getHiveVarchar";
      getValueMethod = ".getValue().getBytes()";
      conversionMethod = "";
    } else if (operandType.equals("date")) {
      defaultValue = "0";
      vectorType = "long";
      getPrimitiveMethod = "getDate";
      getValueMethod = "";
      conversionMethod = "DateWritable.dateToDays";
      // Special case - Date requires its own specific BetweenDynamicValue class, but derives from FilterLongColumnBetween
      typeName = "Long";
    } else if (operandType.equals("timestamp")) {
      defaultValue = "new Timestamp(0)";
      vectorType = "Timestamp";
      getPrimitiveMethod = "getTimestamp";
      getValueMethod = "";
      conversionMethod = "";
    } else {
      throw new IllegalArgumentException("Type " + operandType + " not supported");
    }

    // Read the template into a string, expand it, and write it.
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<TypeName>", typeName);
    templateString = templateString.replaceAll("<DefaultValue>", defaultValue);
    templateString = templateString.replaceAll("<VectorType>", vectorType);
    templateString = templateString.replaceAll("<GetPrimitiveMethod>", getPrimitiveMethod);
    templateString = templateString.replaceAll("<GetValueMethod>", getValueMethod);
    templateString = templateString.replaceAll("<ConversionMethod>", conversionMethod);

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateColumnCompareColumn(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String className = getCamelCaseType(operandType1)
        + "Col" + operatorName + getCamelCaseType(operandType2) + "Column";
    generateColumnCompareOperatorColumn(tdesc, false, className);
  }

  private void generateVectorUDAFMinMax(String[] tdesc) throws Exception {
    String className = tdesc[1];
    String valueType = tdesc[2];
    String operatorSymbol = tdesc[3];
    String descName = tdesc[4];
    String descValue = tdesc[5];
    String columnType = getColumnVectorType(valueType);
    String writableType = getOutputWritableType(valueType);
    String inspectorType = getOutputObjectInspector(valueType);

    File templateFile = new File(joinPath(this.udafTemplateDirectory, tdesc[0] + ".txt"));

    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<ValueType>", valueType);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<InputColumnVectorType>", columnType);
    templateString = templateString.replaceAll("<DescriptionName>", descName);
    templateString = templateString.replaceAll("<DescriptionValue>", descValue);
    templateString = templateString.replaceAll("<OutputType>", writableType);
    templateString = templateString.replaceAll("<OutputTypeInspector>", inspectorType);
    writeFile(templateFile.lastModified(), udafOutputDirectory, udafClassesDirectory,
        className, templateString);
  }

  private void generateVectorUDAFMinMaxString(String[] tdesc) throws Exception {
    String className = tdesc[1];
    String operatorSymbol = tdesc[2];
    String descName = tdesc[3];
    String descValue = tdesc[4];

    File templateFile = new File(joinPath(this.udafTemplateDirectory, tdesc[0] + ".txt"));

    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<DescriptionName>", descName);
    templateString = templateString.replaceAll("<DescriptionValue>", descValue);
    writeFile(templateFile.lastModified(), udafOutputDirectory, udafClassesDirectory,
        className, templateString);
  }

  private void generateVectorUDAFMinMaxObject(String[] tdesc) throws Exception {
      String className = tdesc[1];
      String operatorSymbol = tdesc[2];
      String descName = tdesc[3];
      String descValue = tdesc[4];

      File templateFile = new File(joinPath(this.udafTemplateDirectory, tdesc[0] + ".txt"));

      String templateString = readFile(templateFile);
      templateString = templateString.replaceAll("<ClassName>", className);
      templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
      templateString = templateString.replaceAll("<DescriptionName>", descName);
      templateString = templateString.replaceAll("<DescriptionValue>", descValue);
      writeFile(templateFile.lastModified(), udafOutputDirectory, udafClassesDirectory,
          className, templateString);
    }

  private void generateVectorUDAFSum(String[] tdesc) throws Exception {
  //template, <ClassName>, <ValueType>, <OutputType>, <OutputTypeInspector>
    String className = tdesc[1];
    String valueType = tdesc[2];
    String columnType = getColumnVectorType(valueType);
    String writableType = getOutputWritableType(valueType);
    String inspectorType = getOutputObjectInspector(valueType);

    File templateFile = new File(joinPath(this.udafTemplateDirectory, tdesc[0] + ".txt"));

    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<ValueType>", valueType);
    templateString = templateString.replaceAll("<InputColumnVectorType>", columnType);
    templateString = templateString.replaceAll("<OutputType>", writableType);
    templateString = templateString.replaceAll("<OutputTypeInspector>", inspectorType);
    writeFile(templateFile.lastModified(), udafOutputDirectory, udafClassesDirectory,
        className, templateString);
  }

  private void generateVectorUDAFAvg(String[] tdesc) throws Exception {
    String className = tdesc[1];
    String valueType = tdesc[2];
    String columnType = getColumnVectorType(valueType);

    File templateFile = new File(joinPath(this.udafTemplateDirectory, tdesc[0] + ".txt"));

    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<ValueType>", valueType);
    templateString = templateString.replaceAll("<InputColumnVectorType>", columnType);
    writeFile(templateFile.lastModified(), udafOutputDirectory, udafClassesDirectory,
        className, templateString);
  }

  private void generateVectorUDAFVar(String[] tdesc) throws Exception {
    String className = tdesc[1];
    String valueType = tdesc[2];
    String varianceFormula = tdesc[3];
    String descriptionName = tdesc[4];
    String descriptionValue = tdesc[5];
    String columnType = getColumnVectorType(valueType);

    File templateFile = new File(joinPath(this.udafTemplateDirectory, tdesc[0] + ".txt"));

    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<ValueType>", valueType);
    templateString = templateString.replaceAll("<InputColumnVectorType>", columnType);
    templateString = templateString.replaceAll("<VarianceFormula>", varianceFormula);
    templateString = templateString.replaceAll("<DescriptionName>", descriptionName);
    templateString = templateString.replaceAll("<DescriptionValue>", descriptionValue);
    writeFile(templateFile.lastModified(), udafOutputDirectory, udafClassesDirectory,
        className, templateString);
  }

  private void generateVectorUDAFVarDecimal(String[] tdesc) throws Exception {
      String className = tdesc[1];
      String varianceFormula = tdesc[2];
      String descriptionName = tdesc[3];
      String descriptionValue = tdesc[4];

      File templateFile = new File(joinPath(this.udafTemplateDirectory, tdesc[0] + ".txt"));

      String templateString = readFile(templateFile);
      templateString = templateString.replaceAll("<ClassName>", className);
      templateString = templateString.replaceAll("<VarianceFormula>", varianceFormula);
      templateString = templateString.replaceAll("<DescriptionName>", descriptionName);
      templateString = templateString.replaceAll("<DescriptionValue>", descriptionValue);
      writeFile(templateFile.lastModified(), udafOutputDirectory, udafClassesDirectory,
          className, templateString);
    }

  private void generateFilterStringGroupScalarCompareStringGroupColumnBase(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "FilterStringGroupScalar" + operatorName + "StringGroupColumnBase";

    // Template expansion logic is the same for both column-scalar and scalar-column cases.
    generateStringColumnCompareScalar(tdesc, className);
  }

  private void generateFilterStringScalarCompareStringGroupColumn(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "FilterStringScalar" + operatorName + "StringGroupColumn";
    String baseClassName = "FilterStringGroupScalar" + operatorName + "StringGroupColumnBase";
    String operatorSymbol = tdesc[2];
    // Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    // Expand, and write result

    String compareOrEqual;
    String compareOrEqualReturnType = "boolean";
    String optionalCompare = "";
    if (operatorName.equals("Equal")) {
      compareOrEqual = "StringExpr.equal";
    } else if (operatorName.equals("NotEqual")) {
      compareOrEqual = "!StringExpr.equal";
    } else {
      compareOrEqual = "StringExpr.compare";
      compareOrEqualReturnType = "int";
      optionalCompare = operatorSymbol + " 0";
    }
    templateString = templateString.replaceAll("<CompareOrEqual>", compareOrEqual);
    templateString = templateString.replaceAll("<CompareOrEqualReturnType>", compareOrEqualReturnType);
    templateString = templateString.replaceAll("<OptionalCompare>", optionalCompare);

    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterTruncStringScalarCompareStringGroupColumn(String[] tdesc) throws IOException {
    String truncStringTypeName = tdesc[1];
    String operatorName = tdesc[2];
    String className = "Filter" + truncStringTypeName + "Scalar" + operatorName + "StringGroupColumn";
    String baseClassName = "FilterStringGroupScalar" + operatorName + "StringGroupColumnBase";
    generateStringCompareTruncStringScalar(tdesc, className, baseClassName);
  }

  private void generateStringGroupScalarCompareStringGroupColumnBase(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "StringGroupScalar" + operatorName + "StringGroupColumnBase";

    // Template expansion logic is the same for both column-scalar and scalar-column cases.
    generateStringColumnCompareScalar(tdesc, className);
  }

  private void generateStringScalarCompareStringGroupColumn(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "StringScalar" + operatorName + "StringGroupColumn";
    String baseClassName = "StringGroupScalar" + operatorName + "StringGroupColumnBase";
    String operatorSymbol = tdesc[2];
    // Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    // Expand, and write result

    String compareOrEqual;
    String compareOrEqualReturnType = "boolean";
    String optionalCompare = "";
    if (operatorName.equals("Equal")) {
      compareOrEqual = "StringExpr.equal";
    } else if (operatorName.equals("NotEqual")) {
      compareOrEqual = "!StringExpr.equal";
    } else {
      compareOrEqual = "StringExpr.compare";
      compareOrEqualReturnType = "int";
      optionalCompare = operatorSymbol + " 0";
    }
    templateString = templateString.replaceAll("<CompareOrEqual>", compareOrEqual);
    templateString = templateString.replaceAll("<CompareOrEqualReturnType>", compareOrEqualReturnType);
    templateString = templateString.replaceAll("<OptionalCompare>", optionalCompare);

    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);  }

  private void generateTruncStringScalarCompareStringGroupColumn(String[] tdesc) throws IOException {
    String truncStringTypeName = tdesc[1];
    String operatorName = tdesc[2];
    String className = truncStringTypeName + "Scalar" + operatorName + "StringGroupColumn";
    String baseClassName = "StringGroupScalar" + operatorName + "StringGroupColumnBase";
    generateStringCompareTruncStringScalar(tdesc, className, baseClassName);
  }

  private void generateFilterStringGroupColumnCompareStringGroupScalarBase(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "FilterStringGroupCol" + operatorName + "StringGroupScalarBase";
    generateStringColumnCompareScalar(tdesc, className);
  }

  private void generateFilterStringGroupColumnCompareStringScalar(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "FilterStringGroupCol" + operatorName + "StringScalar";
    String baseClassName = "FilterStringGroupCol" + operatorName + "StringGroupScalarBase";
    String operatorSymbol = tdesc[2];
    // Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    // Expand, and write result

    String compareOrEqual;
    String compareOrEqualReturnType = "boolean";
    String optionalCompare = "";
    if (operatorName.equals("Equal")) {
      compareOrEqual = "StringExpr.equal";
    } else if (operatorName.equals("NotEqual")) {
      compareOrEqual = "!StringExpr.equal";
    } else {
      compareOrEqual = "StringExpr.compare";
      compareOrEqualReturnType = "int";
      optionalCompare = operatorSymbol + " 0";
    }
    templateString = templateString.replaceAll("<CompareOrEqual>", compareOrEqual);
    templateString = templateString.replaceAll("<CompareOrEqualReturnType>", compareOrEqualReturnType);
    templateString = templateString.replaceAll("<OptionalCompare>", optionalCompare);

    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterStringGroupColumnCompareTruncStringScalar(String[] tdesc) throws IOException {
    String truncStringTypeName = tdesc[1];
    String operatorName = tdesc[2];
    String className = "FilterStringGroupCol" + operatorName + truncStringTypeName + "Scalar";
    String baseClassName = "FilterStringGroupCol" + operatorName + "StringGroupScalarBase";
    generateStringCompareTruncStringScalar(tdesc, className, baseClassName);
  }

  private void generateStringGroupColumnCompareStringGroupScalarBase(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "StringGroupCol" + operatorName + "StringGroupScalarBase";
    String operatorSymbol = tdesc[2];
    // Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    // Expand, and write result

    String compareOrEqual;
    String compareOrEqualReturnType = "boolean";
    String optionalCompare = "";
    if (operatorName.equals("Equal")) {
      compareOrEqual = "StringExpr.equal";
    } else if (operatorName.equals("NotEqual")) {
      compareOrEqual = "!StringExpr.equal";
    } else {
      compareOrEqual = "StringExpr.compare";
      compareOrEqualReturnType = "int";
      optionalCompare = operatorSymbol + " 0";
    }
    templateString = templateString.replaceAll("<CompareOrEqual>", compareOrEqual);
    templateString = templateString.replaceAll("<CompareOrEqualReturnType>", compareOrEqualReturnType);
    templateString = templateString.replaceAll("<OptionalCompare>", optionalCompare);

    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateStringGroupColumnCompareStringScalar(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "StringGroupCol" + operatorName + "StringScalar";
    String baseClassName = "StringGroupCol" + operatorName + "StringGroupScalarBase";
    String operatorSymbol = tdesc[2];
    // Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    // Expand, and write result

    String compareOrEqual;
    String compareOrEqualReturnType = "boolean";
    String optionalCompare = "";
    if (operatorName.equals("Equal")) {
      compareOrEqual = "StringExpr.equal";
    } else if (operatorName.equals("NotEqual")) {
      compareOrEqual = "!StringExpr.equal";
    } else {
      compareOrEqual = "StringExpr.compare";
      compareOrEqualReturnType = "int";
      optionalCompare = operatorSymbol + " 0";
    }
    templateString = templateString.replaceAll("<CompareOrEqual>", compareOrEqual);
    templateString = templateString.replaceAll("<CompareOrEqualReturnType>", compareOrEqualReturnType);
    templateString = templateString.replaceAll("<OptionalCompare>", optionalCompare);

    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateStringGroupColumnCompareTruncStringScalar(String[] tdesc) throws IOException {
    String truncStringTypeName = tdesc[1];
    String operatorName = tdesc[2];
    String className = "StringGroupCol" + operatorName + truncStringTypeName + "Scalar";
    String baseClassName = "StringGroupCol" + operatorName + "StringGroupScalarBase";
    generateStringCompareTruncStringScalar(tdesc, className, baseClassName);
  }

  private void generateFilterStringGroupColumnCompareStringGroupColumn(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "FilterStringGroupCol" + operatorName + "StringGroupColumn";
    generateStringColumnCompareScalar(tdesc, className);
  }

  private void generateStringGroupColumnCompareStringGroupColumn(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "StringGroupCol" + operatorName + "StringGroupColumn";
    generateStringColumnCompareScalar(tdesc, className);
  }

  private void generateStringColumnCompareScalar(String[] tdesc, String className)
      throws IOException {
    String operatorName = tdesc[1];
    String operatorSymbol = tdesc[2];
    // Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    // Expand, and write result

    String compareOrEqual;
    String compareOrEqualReturnType = "boolean";
    String optionalCompare = "";
    if (operatorName.equals("Equal")) {
      compareOrEqual = "StringExpr.equal";
    } else if (operatorName.equals("NotEqual")) {
      compareOrEqual = "!StringExpr.equal";
    } else {
      compareOrEqual = "StringExpr.compare";
      compareOrEqualReturnType = "int";
      optionalCompare = operatorSymbol + " 0";
    }
    templateString = templateString.replaceAll("<CompareOrEqual>", compareOrEqual);
    templateString = templateString.replaceAll("<CompareOrEqualReturnType>", compareOrEqualReturnType);
    templateString = templateString.replaceAll("<OptionalCompare>", optionalCompare);

    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateStringCompareTruncStringScalar(String[] tdesc, String className, String baseClassName)
      throws IOException {
    String truncStringTypeName = tdesc[1];
    String truncStringHiveType;
    String truncStringHiveGetBytes;
    if (truncStringTypeName == "Char") {
      truncStringHiveType = "HiveChar";
      truncStringHiveGetBytes = "getStrippedValue().getBytes()";
    } else if (truncStringTypeName == "VarChar") {
      truncStringHiveType = "HiveVarchar";
      truncStringHiveGetBytes = "getValue().getBytes()";
    } else {
      throw new Error("Unsupported string type: " + truncStringTypeName);
    }
    String operatorSymbol = tdesc[3];
    // Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    // Expand, and write result
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<TruncStringTypeName>", truncStringTypeName);
    templateString = templateString.replaceAll("<TruncStringHiveType>", truncStringHiveType);
    templateString = templateString.replaceAll("<TruncStringHiveGetBytes>", truncStringHiveGetBytes);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterColumnCompareColumn(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String className = "Filter" + getCamelCaseType(operandType1)
        + "Col" + operatorName + getCamelCaseType(operandType2) + "Column";
    generateColumnCompareOperatorColumn(tdesc, true, className);
  }

  private void generateColumnUnaryMinus(String[] tdesc) throws Exception {
    String operandType = tdesc[1];
    String inputColumnVectorType = this.getColumnVectorType(operandType);
    String outputColumnVectorType = inputColumnVectorType;
    String returnType = operandType;
    String className = getCamelCaseType(operandType) + "ColUnaryMinus";
        File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    String vectorExprArgType = operandType;
    if (operandType.equals("long")) {
      // interval types can use long version
      vectorExprArgType = "int_interval_year_month";
    }
    // Expand, and write result
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<InputColumnVectorType>", inputColumnVectorType);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
    templateString = templateString.replaceAll("<OperandType>", operandType);
    templateString = templateString.replaceAll("<ReturnType>", returnType);
    templateString = templateString.replaceAll("<VectorExprArgType>", vectorExprArgType);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateIfExprColumnScalar(String[] tdesc) throws Exception {
    String operandType2 = tdesc[1];
    String operandType3 = tdesc[2];
    String arg2ColumnVectorType = this.getColumnVectorType(operandType2);
    String returnType = getArithmeticReturnType(operandType2, operandType3);
    String outputColumnVectorType = getColumnVectorType(returnType);
    String className = "IfExpr" + getCamelCaseType(operandType2) + "Column"
        + getCamelCaseType(operandType3) + "Scalar";
    String outputFile = joinPath(this.expressionOutputDirectory, className + ".java");
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    // Expand, and write result
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<Arg2ColumnVectorType>", arg2ColumnVectorType);
    templateString = templateString.replaceAll("<ReturnType>", returnType);
    templateString = templateString.replaceAll("<OperandType2>", operandType2);
    templateString = templateString.replaceAll("<OperandType3>", operandType3);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);

    String vectorExprArgType2 = operandType2;
    String vectorExprArgType3 = operandType3;

    // Toss in timestamp and date.
    if (operandType2.equals("long") && operandType3.equals("long")) {
      vectorExprArgType2 = "int_date_interval_year_month";
      vectorExprArgType3 = "int_date_interval_year_month";
    }
    templateString = templateString.replaceAll("<VectorExprArgType2>", vectorExprArgType2);
    templateString = templateString.replaceAll("<VectorExprArgType3>", vectorExprArgType3);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateIfExprScalarColumn(String[] tdesc) throws Exception {
    String operandType2 = tdesc[1];
    String operandType3 = tdesc[2];
    String arg3ColumnVectorType = this.getColumnVectorType(operandType3);
    String returnType = getArithmeticReturnType(operandType2, operandType3);
    String outputColumnVectorType = getColumnVectorType(returnType);
    String className = "IfExpr" + getCamelCaseType(operandType2) + "Scalar"
        + getCamelCaseType(operandType3) + "Column";
    String outputFile = joinPath(this.expressionOutputDirectory, className + ".java");
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    // Expand, and write result
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<Arg3ColumnVectorType>", arg3ColumnVectorType);
    templateString = templateString.replaceAll("<ReturnType>", returnType);
    templateString = templateString.replaceAll("<OperandType2>", operandType2);
    templateString = templateString.replaceAll("<OperandType3>", operandType3);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);

    String vectorExprArgType2 = operandType2;
    String vectorExprArgType3 = operandType3;

    // Toss in timestamp and date.
    if (operandType2.equals("long") && operandType3.equals("long")) {
      vectorExprArgType2 = "int_date_interval_year_month";
      vectorExprArgType3 = "int_date_interval_year_month";
    }
    templateString = templateString.replaceAll("<VectorExprArgType2>", vectorExprArgType2);
    templateString = templateString.replaceAll("<VectorExprArgType3>", vectorExprArgType3);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateIfExprScalarScalar(String[] tdesc) throws Exception {
    String operandType2 = tdesc[1];
    String operandType3 = tdesc[2];
    String arg3ColumnVectorType = this.getColumnVectorType(operandType3);
    String returnType = getArithmeticReturnType(operandType2, operandType3);
    String outputColumnVectorType = getColumnVectorType(returnType);
    String className = "IfExpr" + getCamelCaseType(operandType2) + "Scalar"
        + getCamelCaseType(operandType3) + "Scalar";
    String outputFile = joinPath(this.expressionOutputDirectory, className + ".java");
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    // Expand, and write result
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<ReturnType>", returnType);
    templateString = templateString.replaceAll("<OperandType2>", operandType2);
    templateString = templateString.replaceAll("<OperandType3>", operandType3);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);

    String vectorExprArgType2 = operandType2;
    String vectorExprArgType3 = operandType3;

    // Toss in timestamp and date.
    if (operandType2.equals("long") && operandType3.equals("long")) {
      vectorExprArgType2 = "int_date_interval_year_month";
      vectorExprArgType3 = "int_date_interval_year_month";
    }
    templateString = templateString.replaceAll("<VectorExprArgType2>", vectorExprArgType2);
    templateString = templateString.replaceAll("<VectorExprArgType3>", vectorExprArgType3);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  // template, <ClassNamePrefix>, <ReturnType>, <FuncName>
  private void generateDecimalColumnUnaryFunc(String [] tdesc) throws Exception {
    String classNamePrefix = tdesc[1];
    String returnType = tdesc[2];
    String operandType = "decimal";
    String outputColumnVectorType = this.getColumnVectorType(returnType);
    String className = classNamePrefix + getCamelCaseType(operandType) + "To"
        + getCamelCaseType(returnType);
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    String funcName = tdesc[3];
    // Expand, and write result
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
    templateString = templateString.replaceAll("<FuncName>", funcName);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  // template, <ClassNamePrefix>, <ReturnType>, <OperandType>, <FuncName>, <OperandCast>, <ResultCast>
  //   <Cleanup> <VectorExprArgType>
  private void generateColumnUnaryFunc(String[] tdesc) throws Exception {
    String classNamePrefix = tdesc[1];
    String operandType = tdesc[3];
    String inputColumnVectorType = this.getColumnVectorType(operandType);
    String returnType = tdesc[2];
    String outputColumnVectorType = this.getColumnVectorType(returnType);
    String className = classNamePrefix + getCamelCaseType(operandType) + "To"
      + getCamelCaseType(returnType);
        File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    String funcName = tdesc[4];
    String operandCast = tdesc[5];
    String resultCast = tdesc[6];
    String cleanup = tdesc[7];
    String vectorExprArgType = tdesc[8].isEmpty() ? operandType : tdesc[8];

    // Expand, and write result
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<InputColumnVectorType>", inputColumnVectorType);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
    templateString = templateString.replaceAll("<OperandType>", operandType);
    templateString = templateString.replaceAll("<ReturnType>", returnType);
    templateString = templateString.replaceAll("<FuncName>", funcName);
    templateString = templateString.replaceAll("<OperandCast>", operandCast);
    templateString = templateString.replaceAll("<ResultCast>", resultCast);
    templateString = templateString.replaceAll("<Cleanup>", cleanup);
    templateString = templateString.replaceAll("<VectorExprArgType>", vectorExprArgType);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateColumnArithmeticColumn(String [] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String className = getCamelCaseType(operandType1)
        + "Col" + operatorName + getCamelCaseType(operandType2) + "Column";
    String returnType = getArithmeticReturnType(operandType1, operandType2);
    generateColumnArithmeticOperatorColumn(tdesc, returnType, className);
  }

  private void generateFilterColumnCompareScalar(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String className = "Filter" + getCamelCaseType(operandType1)
        + "Col" + operatorName + getCamelCaseType(operandType2) + "Scalar";
    generateColumnCompareOperatorScalar(tdesc, true, className);
  }

  private void generateFilterScalarCompareColumn(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String className = "Filter" + getCamelCaseType(operandType1)
        + "Scalar" + operatorName + getCamelCaseType(operandType2) + "Column";
    generateScalarCompareOperatorColumn(tdesc, true, className);
  }

  private void generateColumnCompareScalar(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String className = getCamelCaseType(operandType1)
        + "Col" + operatorName + getCamelCaseType(operandType2) + "Scalar";
    generateColumnCompareOperatorScalar(tdesc, false, className);
  }

  private void generateScalarCompareColumn(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String className = getCamelCaseType(operandType1)
        + "Scalar" + operatorName + getCamelCaseType(operandType2) + "Column";
    generateScalarCompareOperatorColumn(tdesc, false, className);
  }

  private void generateColumnCompareOperatorColumn(String[] tdesc, boolean filter,
         String className) throws Exception {
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String inputColumnVectorType1 = this.getColumnVectorType(operandType1);
    String inputColumnVectorType2 = this.getColumnVectorType(operandType2);
    String returnType = "long";
    String outputColumnVectorType = this.getColumnVectorType(returnType);
    String operatorSymbol = tdesc[4];

    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<InputColumnVectorType1>", inputColumnVectorType1);
    templateString = templateString.replaceAll("<InputColumnVectorType2>", inputColumnVectorType2);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<OperandType1>", operandType1);
    templateString = templateString.replaceAll("<OperandType2>", operandType2);
    templateString = templateString.replaceAll("<ReturnType>", returnType);
    templateString = templateString.replaceAll("<CamelReturnType>", getCamelCaseType(returnType));
    String vectorExprArgType1 = operandType1;
    String vectorExprArgType2 = operandType2;

    // For column to column only, we toss in date and interval_year_month.
    if (operandType1.equals("long") && operandType2.equals("long")) {
      vectorExprArgType1 = "int_date_interval_year_month";
      vectorExprArgType2 = "int_date_interval_year_month";
    }
    templateString = templateString.replaceAll("<VectorExprArgType1>", vectorExprArgType1);
    templateString = templateString.replaceAll("<VectorExprArgType2>", vectorExprArgType2);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);

    if (filter) {
      testCodeGen.addColumnColumnFilterTestCases(
          className,
          inputColumnVectorType1,
          inputColumnVectorType2,
          operatorSymbol);
    } else {
      testCodeGen.addColumnColumnOperationTestCases(
          className,
          inputColumnVectorType1,
          inputColumnVectorType2,
          outputColumnVectorType);
    }
  }

  // -----------------------------------------------------------------------------------------------
  //
  // Filter timestamp against timestamp, long (seconds), and double (seconds with fractional
  // nanoseconds).
  //
  //  Filter  TimestampCol         {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   TimestampColumn
  //  Filter  TimestampCol         {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   {Long|Double}Column
  //* Filter  {Long|Double}Col     {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   TimestampColumn
  //
  //  Filter  TimestampCol         {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   TimestampScalar
  //  Filter  TimestampCol         {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   {Long|Double}Scalar
  //* Filter  {Long|Double}Col     {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   TimestampScalar
  //
  //  Filter  TimestampScalar      {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   TimestampColumn
  //  Filter  TimestampScalar      {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   {Long|Double}Column
  //* Filter  {Long|Double}Scalar  {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   TimestampColumn
  //
  // -----------------------------------------------------------------------------------------------

  private void generateFilterTimestampCompareTimestamp(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operatorSymbol = tdesc[2];
    String operandType = tdesc[3];
    String camelOperandType = getCamelCaseType(operandType);

    String className = "Filter" + camelOperandType + tdesc[4] + operatorName + camelOperandType + tdesc[5];
    String baseClassName = "FilterTimestamp" + tdesc[4] + operatorName + "Timestamp" + tdesc[5] + "Base";
    //Read the template into a string;
    String fileName = "FilterTimestamp" + (tdesc[4].equals("Col") ? "Column" : tdesc[4]) + "CompareTimestamp" +
        tdesc[5];
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, fileName + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<OperandType>", operandType);
    templateString = templateString.replaceAll("<CamelOperandType>", camelOperandType);
    templateString = templateString.replaceAll("<HiveOperandType>", getTimestampHiveType(operandType));

    String inputColumnVectorType = this.getColumnVectorType(operandType);
    templateString = templateString.replaceAll("<InputColumnVectorType>", inputColumnVectorType);

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterTimestampCompareLongDouble(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType = tdesc[2];
    String camelCaseOperandType = getCamelCaseType(operandType);
    String operatorSymbol = tdesc[3];
    String inputColumnVectorType2 = this.getColumnVectorType(operandType);

    String className = "FilterTimestamp" + tdesc[4] + operatorName + camelCaseOperandType + tdesc[5];

    // Timestamp Scalar case becomes use long/double scalar class.
    String baseClassName;
    if (tdesc[4].equals("Scalar")) {
      baseClassName = "org.apache.hadoop.hive.ql.exec.vector.expressions.gen." +
          "Filter" + camelCaseOperandType + "Scalar" + operatorName + camelCaseOperandType + "Column";
    } else {
      baseClassName = "";
    }

    //Read the template into a string;
    String fileName = "FilterTimestamp" + (tdesc[4].equals("Col") ? "Column" : tdesc[4]) + "CompareLongDouble" +
        tdesc[5];
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, fileName + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    if (baseClassName.length() > 0) {
      templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    }
    templateString = templateString.replaceAll("<OperandType>", operandType);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<InputColumnVectorType2>", inputColumnVectorType2);
    templateString = templateString.replaceAll("<GetTimestampLongDoubleMethod>", timestampLongDoubleMethod(operandType));
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterLongDoubleCompareTimestamp(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType = tdesc[2];
    String camelCaseOperandType = getCamelCaseType(operandType);
    String operatorSymbol = tdesc[3];
    String inputColumnVectorType1 = this.getColumnVectorType(operandType);

    String className = "Filter" + getCamelCaseType(operandType) + tdesc[4] + operatorName + "Timestamp" + tdesc[5];

    // Timestamp Scalar case becomes use long/double scalar class.
    String baseClassName;
    if (tdesc[5].equals("Scalar")) {
      baseClassName = "org.apache.hadoop.hive.ql.exec.vector.expressions.gen." +
          "Filter" + camelCaseOperandType + "Col" + operatorName + camelCaseOperandType + "Scalar";
    } else {
      baseClassName = "";
    }

    //Read the template into a string;
    String fileName = "FilterLongDouble" + (tdesc[4].equals("Col") ? "Column" : tdesc[4]) + "CompareTimestamp" +
        tdesc[5];
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, fileName + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    if (baseClassName.length() > 0) {
      templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    }
    templateString = templateString.replaceAll("<OperandType>", operandType);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<InputColumnVectorType1>", inputColumnVectorType1);
    templateString = templateString.replaceAll("<GetTimestampLongDoubleMethod>", timestampLongDoubleMethod(operandType));
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private String timestampLongDoubleMethod(String operandType) {
    if (operandType.equals("long")) {
      return "getTimestampAsLong";
    } else if (operandType.equals("double")) {
      return "getDouble";
    } else {
      return "unknown";
    }
  }

  // -----------------------------------------------------------------------------------------------
  //
  // Compare timestamp against timestamp, long (seconds), and double (seconds with fractional
  // nanoseconds).
  //
  //  TimestampCol         {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   TimestampColumn
  //  TimestampCol         {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   {Long|Double}Column
  //* {Long|Double}Col     {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   TimestampColumn
  //
  //  TimestampCol         {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   TimestampScalar
  //  TimestampCol         {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   {Long|Double}Scalar
  //* {Long|Double}Col     {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   TimestampScalar
  //
  //  TimestampScalar      {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   TimestampColumn
  //  TimestampScalar      {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   {Long|Double}Column
  //* {Long|Double}Scalar  {Equal|Greater|GreaterEqual|Less|LessEqual|NotEqual}   TimestampColumn
  //
  // -----------------------------------------------------------------------------------------------


  private void generateTimestampCompareTimestamp(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operatorSymbol = tdesc[2];
    String operandType = tdesc[3];
    String camelOperandType = getCamelCaseType(operandType);
    String className = camelOperandType + tdesc[4] + operatorName + camelOperandType + tdesc[5];

    //Read the template into a string;
    String fileName = "Timestamp" + (tdesc[4].equals("Col") ? "Column" : tdesc[4]) + "CompareTimestamp" +
        (tdesc[5].equals("Col") ? "Column" : tdesc[5]);
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, fileName + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<OperandType>", operandType);
    templateString = templateString.replaceAll("<CamelOperandType>", camelOperandType);
    templateString = templateString.replaceAll("<HiveOperandType>", getTimestampHiveType(operandType));
    templateString = templateString.replaceAll("<InputColumnVectorType>", getColumnVectorType(operandType));

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateTimestampCompareLongDouble(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType = tdesc[2];
    String camelCaseOperandType = getCamelCaseType(operandType);
    String operatorSymbol = tdesc[3];
    String inputColumnVectorType2 = this.getColumnVectorType(operandType);

    String className = "Timestamp" + tdesc[4] + operatorName + getCamelCaseType(operandType) + tdesc[5];


    // Timestamp Scalar case becomes use long/double scalar class.
    String baseClassName;
    if (tdesc[4].equals("Scalar")) {
      baseClassName = camelCaseOperandType + "Scalar" + operatorName + camelCaseOperandType + "Column";
    } else {
      baseClassName = "";
    }

    //Read the template into a string;
    String fileName = "Timestamp" + (tdesc[4].equals("Col") ? "Column" : tdesc[4]) + "CompareLongDouble" +
        tdesc[5];
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, fileName + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    if (baseClassName.length() > 0) {
      templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    }
    templateString = templateString.replaceAll("<OperandType>", operandType);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<InputColumnVectorType2>", inputColumnVectorType2);
    templateString = templateString.replaceAll("<GetTimestampLongDoubleMethod>", timestampLongDoubleMethod(operandType));
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateLongDoubleCompareTimestamp(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType = tdesc[2];
    String camelCaseOperandType = getCamelCaseType(operandType);
    String operatorSymbol = tdesc[3];
    String inputColumnVectorType1 = this.getColumnVectorType(operandType);

    String className = getCamelCaseType(operandType) + tdesc[4] + operatorName + "Timestamp" + tdesc[5];

    // Timestamp Scalar case becomes use long/double scalar class.
    String baseClassName;
    if (tdesc[5].equals("Scalar")) {
      baseClassName = camelCaseOperandType + "Col" + operatorName + camelCaseOperandType + "Scalar";
    } else {
      baseClassName = "";
    }

    //Read the template into a string;
    String fileName = "LongDouble" + (tdesc[4].equals("Col") ? "Column" : tdesc[4]) + "CompareTimestamp" +
        tdesc[5];
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, fileName + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    if (baseClassName.length() > 0) {
      templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    }
    templateString = templateString.replaceAll("<OperandType>", operandType);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<InputColumnVectorType1>", inputColumnVectorType1);
    templateString = templateString.replaceAll("<GetTimestampLongDoubleMethod>", timestampLongDoubleMethod(operandType));
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  // -----------------------------------------------------------------------------------------------
  //
  // -----------------------------------------------------------------------------------------------

  private void generateColumnArithmeticOperatorColumn(String[] tdesc, String returnType,
         String className) throws Exception {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String outputColumnVectorType = this.getColumnVectorType(returnType);
    String inputColumnVectorType1 = this.getColumnVectorType(operandType1);
    String inputColumnVectorType2 = this.getColumnVectorType(operandType2);
    String operatorSymbol = tdesc[4];

    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<InputColumnVectorType1>", inputColumnVectorType1);
    templateString = templateString.replaceAll("<InputColumnVectorType2>", inputColumnVectorType2);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
    templateString = templateString.replaceAll("<OperatorName>", operatorName);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<OperandType1>", operandType1);
    templateString = templateString.replaceAll("<OperandType2>", operandType2);
    templateString = templateString.replaceAll("<ReturnType>", returnType);
    templateString = templateString.replaceAll("<CamelReturnType>", getCamelCaseType(returnType));
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);

    testCodeGen.addColumnColumnOperationTestCases(
          className,
          inputColumnVectorType1,
          inputColumnVectorType2,
          outputColumnVectorType);
  }

  private void generateColumnCompareOperatorScalar(String[] tdesc, boolean filter,
     String className) throws Exception {
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String inputColumnVectorType = this.getColumnVectorType(operandType1);
    String returnType = "long";
    String outputColumnVectorType = this.getColumnVectorType(returnType);
    String operatorSymbol = tdesc[4];

    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<InputColumnVectorType>", inputColumnVectorType);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<OperandType1>", operandType1);
    templateString = templateString.replaceAll("<OperandType2>", operandType2);
    templateString = templateString.replaceAll("<ReturnType>", returnType);
    templateString = templateString.replaceAll("<CamelReturnType>", getCamelCaseType(returnType));
    String vectorExprArgType1 = operandType1;
    String vectorExprArgType2 = operandType2;
    templateString = templateString.replaceAll("<VectorExprArgType1>", vectorExprArgType1);
    templateString = templateString.replaceAll("<VectorExprArgType2>", vectorExprArgType2);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);

    if (filter) {
      testCodeGen.addColumnScalarFilterTestCases(
          true,
          className,
          inputColumnVectorType,
          operandType2,
          operatorSymbol);
    } else {
      testCodeGen.addColumnScalarOperationTestCases(
          true,
          className,
          inputColumnVectorType,
          outputColumnVectorType,
          operandType2);
    }
  }

  private void generateColumnArithmeticOperatorScalar(String[] tdesc, String returnType,
     String className) throws Exception {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String outputColumnVectorType = this.getColumnVectorType(returnType);
    String inputColumnVectorType = this.getColumnVectorType(operandType1);
    String operatorSymbol = tdesc[4];

    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<InputColumnVectorType>", inputColumnVectorType);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
    templateString = templateString.replaceAll("<OperatorName>", operatorName);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<OperandType1>", operandType1);
    templateString = templateString.replaceAll("<OperandType2>", operandType2);
    templateString = templateString.replaceAll("<ReturnType>", returnType);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);

    String testScalarType = operandType2;
    if (isDateIntervalType(testScalarType)) {
      testScalarType = "long";
    }

    testCodeGen.addColumnScalarOperationTestCases(
          true,
          className,
          inputColumnVectorType,
          outputColumnVectorType,
          testScalarType);
  }

  private void generateScalarCompareOperatorColumn(String[] tdesc, boolean filter,
     String className) throws Exception {
     String operandType1 = tdesc[2];
     String operandType2 = tdesc[3];
     String returnType = "long";
     String inputColumnVectorType = this.getColumnVectorType(operandType2);
     String outputColumnVectorType = this.getColumnVectorType(returnType);
     String operatorSymbol = tdesc[4];

     //Read the template into a string;
     File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
     String templateString = readFile(templateFile);
     templateString = templateString.replaceAll("<ClassName>", className);
     templateString = templateString.replaceAll("<InputColumnVectorType>", inputColumnVectorType);
     templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
     templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
     templateString = templateString.replaceAll("<OperandType1>", operandType1);
     templateString = templateString.replaceAll("<OperandType2>", operandType2);
     templateString = templateString.replaceAll("<ReturnType>", returnType);
     templateString = templateString.replaceAll("<CamelReturnType>", getCamelCaseType(returnType));
     String vectorExprArgType1 = operandType1;
     String vectorExprArgType2 = operandType2;
     templateString = templateString.replaceAll("<VectorExprArgType1>", vectorExprArgType1);
     templateString = templateString.replaceAll("<VectorExprArgType2>", vectorExprArgType2);
     writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);

     if (filter) {
       testCodeGen.addColumnScalarFilterTestCases(
           false,
           className,
           inputColumnVectorType,
           operandType1,
           operatorSymbol);
     } else {
       testCodeGen.addColumnScalarOperationTestCases(
           false,
           className,
           inputColumnVectorType,
           outputColumnVectorType,
           operandType1);
     }
  }

  private void generateScalarArithmeticOperatorColumn(String[] tdesc, String returnType,
     String className) throws Exception {
     String operatorName = tdesc[1];
     String operandType1 = tdesc[2];
     String operandType2 = tdesc[3];
     String outputColumnVectorType = this.getColumnVectorType(
             returnType == null ? "long" : returnType);
     String inputColumnVectorType = this.getColumnVectorType(operandType2);
     String operatorSymbol = tdesc[4];

     //Read the template into a string;
     File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
     String templateString = readFile(templateFile);
     templateString = templateString.replaceAll("<ClassName>", className);
     templateString = templateString.replaceAll("<InputColumnVectorType>", inputColumnVectorType);
     templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
     templateString = templateString.replaceAll("<OperatorName>", operatorName);
     templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
     templateString = templateString.replaceAll("<OperandType1>", operandType1);
     templateString = templateString.replaceAll("<OperandType2>", operandType2);
     templateString = templateString.replaceAll("<ReturnType>", returnType);
     templateString = templateString.replaceAll("<CamelReturnType>", getCamelCaseType(returnType));
     writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);

     String testScalarType = operandType1;
     if (isDateIntervalType(testScalarType)) {
       testScalarType = "long";
     }

     testCodeGen.addColumnScalarOperationTestCases(
           false,
           className,
           inputColumnVectorType,
           outputColumnVectorType,
           testScalarType);
  }

  //Binary arithmetic operator
  private void generateColumnArithmeticScalar(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String className = getCamelCaseType(operandType1)
        + "Col" + operatorName + getCamelCaseType(operandType2) + "Scalar";
    String returnType = getArithmeticReturnType(operandType1, operandType2);
    generateColumnArithmeticOperatorScalar(tdesc, returnType, className);
  }

  private void generateColumnArithmeticScalarDecimal(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "DecimalCol" + operatorName + "DecimalScalar";

    // Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<Operator>", operatorName.toLowerCase());

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
       className, templateString);
  }

  private void generateScalarArithmeticColumnDecimal(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "DecimalScalar" + operatorName + "DecimalColumn";

    // Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<Operator>", operatorName.toLowerCase());

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
       className, templateString);
  }

  private void generateColumnArithmeticColumnDecimal(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "DecimalCol" + operatorName + "DecimalColumn";

    // Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<Operator>", operatorName.toLowerCase());

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
       className, templateString);
  }

  private void generateColumnDivideScalarDecimal(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "DecimalCol" + getInitialCapWord(operatorName) + "DecimalScalar";

    // Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<Operator>", operatorName.toLowerCase());

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
       className, templateString);
  }

  private void generateScalarDivideColumnDecimal(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "DecimalScalar" + getInitialCapWord(operatorName) + "DecimalColumn";

    // Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<Operator>", operatorName.toLowerCase());

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
       className, templateString);
  }

  private void generateColumnDivideColumnDecimal(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "DecimalCol" + getInitialCapWord(operatorName) + "DecimalColumn";

    // Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<Operator>", operatorName.toLowerCase());

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
       className, templateString);
  }

  private void generateScalarArithmeticColumn(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String operandType2 = tdesc[3];
    String className = getCamelCaseType(operandType1)
        + "Scalar" + operatorName + getCamelCaseType(operandType2) + "Column";
    String returnType = getArithmeticReturnType(operandType1, operandType2);
    generateScalarArithmeticOperatorColumn(tdesc, returnType, className);
  }

  private void generateFilterDecimalColumnCompareDecimalScalar(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "FilterDecimalCol" + operatorName + "DecimalScalar";
    generateDecimalColumnCompare(tdesc, className);
  }

  private void generateFilterDecimalScalarCompareDecimalColumn(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "FilterDecimalScalar" + operatorName + "DecimalColumn";
    generateDecimalColumnCompare(tdesc, className);
  }

  private void generateFilterDecimalColumnCompareDecimalColumn(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "FilterDecimalCol" + operatorName + "DecimalColumn";
    generateDecimalColumnCompare(tdesc, className);
  }

  private void generateDecimalColumnCompare(String[] tdesc, String className)
      throws IOException {
    String operatorSymbol = tdesc[2];

    // Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);

    // Expand, and write result
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  // TODO: These can eventually be used to replace generateTimestampScalarCompareTimestampColumn()
  private void generateDTIScalarCompareColumn(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType = tdesc[2];
    String className = getCamelCaseType(operandType) + "Scalar" + operatorName
        + getCamelCaseType(operandType) + "Column";
    String baseClassName = "org.apache.hadoop.hive.ql.exec.vector.expressions.LongScalar" + operatorName + "LongColumn";
    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    templateString = templateString.replaceAll("<VectorExprArgType>", operandType);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterDTIScalarCompareColumn(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType = tdesc[2];
    String className = "Filter" + getCamelCaseType(operandType) + "Scalar" + operatorName
        + getCamelCaseType(operandType) + "Column";
    String baseClassName = "FilterLongScalar" + operatorName + "LongColumn";
    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    templateString = templateString.replaceAll("<VectorExprArgType>", operandType);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateDTIColumnCompareScalar(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType = tdesc[2];
    String className = getCamelCaseType(operandType) + "Col" + operatorName
        + getCamelCaseType(operandType) + "Scalar";
    String baseClassName = "org.apache.hadoop.hive.ql.exec.vector.expressions.LongCol" + operatorName + "LongScalar";
    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    templateString = templateString.replaceAll("<VectorExprArgType>", operandType);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterDTIColumnCompareScalar(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType = tdesc[2];
    String className = "Filter" + getCamelCaseType(operandType) + "Col" + operatorName
        + getCamelCaseType(operandType) + "Scalar";
    String baseClassName = "FilterLongCol" + operatorName + "LongScalar";
    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    templateString = templateString.replaceAll("<VectorExprArgType>", operandType);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  // DateColumnArithmeticIntervalYearMonthColumn.txt
  // DateScalarArithmeticIntervalYearMonthColumn.txt
  // DateColumnArithmeticIntervalYearMonthScalar.txt
  //
  // IntervalYearMonthColumnArithmeticDateColumn.txt
  // IntervalYearMonthScalarArithmeticDateColumn.txt
  // IntervalYearMonthColumnArithmeticDateScalar.txt
  //
  // TimestampColumnArithmeticIntervalYearMonthColumn.txt
  // TimestampScalarArithmeticIntervalYearMonthColumn.txt
  // TimestampColumnArithmeticIntervalYearMonthScalar.txt
  //
  // IntervalYearMonthColumnArithmeticTimestampColumn.txt
  // IntervalYearMonthScalarArithmeticTimestampColumn.txt
  // IntervalYearMonthColumnArithmeticTimestampScalar.txt
  //
  private void generateDateTimeArithmeticIntervalYearMonth(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operatorSymbol = tdesc[2];
    String operandType1 = tdesc[3];
    String colOrScalar1 = tdesc[4];
    String operandType2 = tdesc[5];
    String colOrScalar2 = tdesc[6];
    String className = getCamelCaseType(operandType1) + colOrScalar1 + operatorName +
        getCamelCaseType(operandType2) + colOrScalar2;

    //Read the template into a string;
    String fileName = getCamelCaseType(operandType1) + (colOrScalar1.equals("Col") ? "Column" : colOrScalar1) + "Arithmetic" +
        getCamelCaseType(operandType2) + colOrScalar2;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, fileName + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<OperatorMethod>", operatorName.toLowerCase());
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);

    String inputColumnVectorType1 = this.getColumnVectorType(operandType1);
    String inputColumnVectorType2 = this.getColumnVectorType(operandType2);

    if (colOrScalar1.equals("Col") && colOrScalar1.equals("Column")) {
      testCodeGen.addColumnColumnOperationTestCases(
            className,
            inputColumnVectorType1,
            inputColumnVectorType2,
            "long");
    } else if (colOrScalar1.equals("Col") && colOrScalar1.equals("Scalar")) {
      String testScalarType = operandType2;
      if (isDateIntervalType(testScalarType)) {
        testScalarType = "long";
      }
      testCodeGen.addColumnScalarOperationTestCases(
          true,
          className,
          inputColumnVectorType1,
          "long",
          testScalarType);
    } else if (colOrScalar1.equals("Scalar") && colOrScalar1.equals("Column")) {
      String testScalarType = operandType1;
      if (isDateIntervalType(testScalarType)) {
        testScalarType = "long";
      }

      testCodeGen.addColumnScalarOperationTestCases(
            false,
            className,
            inputColumnVectorType2,
            "long",
            testScalarType);
    }
  }

  private String getTimestampHiveType(String operandType) {
    if (operandType.equals("timestamp")) {
      return "Timestamp";
    } else if (operandType.equals("interval_day_time")) {
      return "HiveIntervalDayTime";
    } else {
      return "Unknown";
    }
  }

  private String getPisaTimestampConversion(String operandType) {
    if (operandType.equals("timestamp")) {
      return "new PisaTimestamp(value)";
    } else if (operandType.equals("interval_day_time")) {
      return "value.pisaTimestampUpdate(new PisaTimestamp())";
    } else {
      return "Unknown";
    }
  }

  private String replaceTimestampScalar(String templateString, int argNum, String operandType) {

    if (!operandType.equals("timestamp") && !operandType.equals("interval_day_time")) {
      return templateString;
    }

    String scalarHiveTimestampTypePattern = "<ScalarHiveTimestampType" + argNum + ">";
    String pisaTimestampConversionPattern = "<PisaTimestampConversion" + argNum + ">";

    templateString = templateString.replaceAll(scalarHiveTimestampTypePattern, getTimestampHiveType(operandType));
    templateString = templateString.replaceAll(pisaTimestampConversionPattern, getPisaTimestampConversion(operandType));

    return templateString;
  }

  // TimestampColumnArithmeticTimestampColumn.txt
  // TimestampScalarArithmeticTimestampColumn.txt
  // TimestampColumnArithmeticTimestampScalar.txt
  //
  private void generateTimestampArithmeticTimestamp(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String camelOperandType1 = getCamelCaseType(operandType1);
    String colOrScalar1 = tdesc[3];
    String operandType2 = tdesc[4];
    String camelOperandType2 = getCamelCaseType(operandType2);
    String colOrScalar2 = tdesc[5];

    String returnType;
    if (operandType1.equals(operandType2)) {
      // timestamp - timestamp
      // interval_day_time +/- interval_day_time
      returnType = "interval_day_time";
    } else {
      // timestamp +/- interval_day_time
      // interval_day_time + timestamp
      returnType = "timestamp";
    }

    String className = getCamelCaseType(operandType1) + colOrScalar1 + operatorName +
        getCamelCaseType(operandType2) + colOrScalar2;
    String baseClassName = "Timestamp" + colOrScalar1 + operatorName +
        "Timestamp" + colOrScalar2 + "Base";

    //Read the template into a string;
    String fileName = "Timestamp" + (colOrScalar1.equals("Col") ? "Column" : colOrScalar1) + "Arithmetic" +
        "Timestamp" + colOrScalar2;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, fileName + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    templateString = templateString.replaceAll("<OperatorMethod>", operatorName.toLowerCase());
    templateString = templateString.replaceAll("<OperandType1>", operandType1);
    templateString = templateString.replaceAll("<OperandType2>", operandType2);
    templateString = templateString.replaceAll("<CamelOperandType1>", camelOperandType1);
    templateString = templateString.replaceAll("<CamelOperandType2>", camelOperandType2);
    templateString = templateString.replaceAll("<HiveOperandType1>", getTimestampHiveType(operandType1));
    templateString = templateString.replaceAll("<HiveOperandType2>", getTimestampHiveType(operandType2));

    String inputColumnVectorType1 = this.getColumnVectorType(operandType1);
    templateString = templateString.replaceAll("<InputColumnVectorType1>", inputColumnVectorType1);
    String inputColumnVectorType2 = this.getColumnVectorType(operandType2);
    templateString = templateString.replaceAll("<InputColumnVectorType2>", inputColumnVectorType2);

    String outputColumnVectorType = this.getColumnVectorType(returnType);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
    templateString = templateString.replaceAll("<CamelReturnType>", getCamelCaseType(returnType));
    templateString = templateString.replaceAll("<ReturnType>", returnType);

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);

    /* UNDONE: Col Col, vs Scalar Col vs Col Scalar
    testCodeGen.addColumnColumnOperationTestCases(
          className,
          inputColumnVectorType1,
          inputColumnVectorType2,
          "long");
    */
  }

  // DateColumnArithmeticTimestampColumn.txt
  // DateScalarArithmeticTimestampColumn.txt
  // DateColumnArithmeticTimestampScalar.txt
  //
  private void generateDateArithmeticTimestamp(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String camelOperandType1 = getCamelCaseType(operandType1);
    String colOrScalar1 = tdesc[3];
    String operandType2 = tdesc[4];
    String camelOperandType2 = getCamelCaseType(operandType2);
    String colOrScalar2 = tdesc[5];

    String returnType;
    if (operandType1.equals("interval_day_time") || operandType2.equals("interval_day_time")) {
      returnType = "timestamp";
    } else if (operandType1.equals("timestamp") || operandType2.equals("timestamp")) {
      returnType = "interval_day_time";
    } else {
      returnType = "unknown";
    }

    String className = camelOperandType1 + colOrScalar1 + operatorName +
        camelOperandType2 + colOrScalar2;

    //Read the template into a string;
    String fileName = "Date" + (colOrScalar1.equals("Col") ? "Column" : colOrScalar1) + "Arithmetic" +
        "Timestamp" + colOrScalar2;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, fileName + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<OperatorMethod>", operatorName.toLowerCase());
    templateString = templateString.replaceAll("<OperandType1>", operandType1);
    templateString = templateString.replaceAll("<OperandType2>", operandType2);
    templateString = templateString.replaceAll("<CamelOperandType1>", camelOperandType1);
    templateString = templateString.replaceAll("<CamelOperandType2>", camelOperandType2);
    templateString = templateString.replaceAll("<HiveOperandType1>", getTimestampHiveType(operandType1));
    templateString = templateString.replaceAll("<HiveOperandType2>", getTimestampHiveType(operandType2));

    String inputColumnVectorType1 = this.getColumnVectorType(operandType1);
    templateString = templateString.replaceAll("<InputColumnVectorType1>", inputColumnVectorType1);
    String inputColumnVectorType2 = this.getColumnVectorType(operandType2);
    templateString = templateString.replaceAll("<InputColumnVectorType2>", inputColumnVectorType2);

    String outputColumnVectorType = this.getColumnVectorType(returnType);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
    templateString = templateString.replaceAll("<CamelReturnType>", getCamelCaseType(returnType));
    templateString = templateString.replaceAll("<ReturnType>", returnType);

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);

    /* UNDONE: Col Col, vs Scalar Col vs Col Scalar
    testCodeGen.addColumnColumnOperationTestCases(
          className,
          inputColumnVectorType1,
          inputColumnVectorType2,
          "long");
    */
  }

  // TimestampColumnArithmeticDateColumn.txt
  // TimestampScalarArithmeticDateColumn.txt
  // TimestampColumnArithmeticDateScalar.txt
  //
  private void generateTimestampArithmeticDate(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType1 = tdesc[2];
    String camelOperandType1 = getCamelCaseType(operandType1);
    String colOrScalar1 = tdesc[3];
    String operandType2 = tdesc[4];
    String camelOperandType2 = getCamelCaseType(operandType2);
    String colOrScalar2 = tdesc[5];

    String returnType;
    if (operandType1.equals("interval_day_time") || operandType2.equals("interval_day_time")) {
      returnType = "timestamp";
    } else if (operandType1.equals("timestamp") || operandType2.equals("timestamp")) {
      returnType = "interval_day_time";
    } else {
      returnType = "unknown";
    }

    String className = camelOperandType1 + colOrScalar1 + operatorName +
        camelOperandType2 + colOrScalar2;

    //Read the template into a string;
    String fileName = "Timestamp" + (colOrScalar1.equals("Col") ? "Column" : colOrScalar1) + "Arithmetic" +
        "Date" + colOrScalar2;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, fileName + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<OperatorMethod>", operatorName.toLowerCase());
    templateString = templateString.replaceAll("<OperandType1>", operandType1);
    templateString = templateString.replaceAll("<OperandType2>", operandType2);
    templateString = templateString.replaceAll("<CamelOperandType1>", camelOperandType1);
    templateString = templateString.replaceAll("<CamelOperandType2>", camelOperandType2);
    templateString = templateString.replaceAll("<HiveOperandType1>", getTimestampHiveType(operandType1));
    templateString = templateString.replaceAll("<HiveOperandType2>", getTimestampHiveType(operandType2));

    String inputColumnVectorType1 = this.getColumnVectorType(operandType1);
    templateString = templateString.replaceAll("<InputColumnVectorType1>", inputColumnVectorType1);
    String inputColumnVectorType2 = this.getColumnVectorType(operandType2);
    templateString = templateString.replaceAll("<InputColumnVectorType2>", inputColumnVectorType2);

    String outputColumnVectorType = this.getColumnVectorType(returnType);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
    templateString = templateString.replaceAll("<CamelReturnType>", getCamelCaseType(returnType));
    templateString = templateString.replaceAll("<ReturnType>", returnType);

    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);

    /* UNDONE: Col Col, vs Scalar Col vs Col Scalar
    testCodeGen.addColumnColumnOperationTestCases(
          className,
          inputColumnVectorType1,
          inputColumnVectorType2,
          "long");
    */
  }

  private static boolean isDateIntervalType(String type) {
    return (type.equals("date")
        || type.equals("interval_year_month"));
  }

  private static boolean isTimestampIntervalType(String type) {
    return (type.equals("timestamp")
        || type.equals("interval_day_time"));
  }

  static void writeFile(long templateTime, String outputDir, String classesDir,
       String className, String str) throws IOException {
    File outputFile = new File(outputDir, className + ".java");
    File outputClass = new File(classesDir, className + ".class");
    if (outputFile.lastModified() > templateTime && outputFile.length() == str.length() &&
        outputClass.lastModified() > templateTime) {
      // best effort
      return;
    }
    writeFile(outputFile, str);
  }

  static void writeFile(File outputFile, String str) throws IOException {
    BufferedWriter w = new BufferedWriter(new FileWriter(outputFile));
    w.write(str);
    w.close();
  }

  static String readFile(String templateFile) throws IOException {
    return readFile(new File(templateFile));
  }

  static String readFile(File templateFile) throws IOException {
    BufferedReader r = new BufferedReader(new FileReader(templateFile));
    String line = r.readLine();
    StringBuilder b = new StringBuilder();
    while (line != null) {
      b.append(line);
      b.append("\n");
      line = r.readLine();
    }
    r.close();
    return b.toString();
  }

  static String getCamelCaseType(String type) {
    if (type == null) {
      return null;
    }
    if (type.equals("long")) {
      return "Long";
    } else if (type.equals("double")) {
      return "Double";
    } else if (type.equals("decimal")) {
      return "Decimal";
    } else if (type.equals("interval_year_month")) {
      return "IntervalYearMonth";
    } else if (type.equals("interval_day_time")) {
      return "IntervalDayTime";
    } else if (type.equals("timestamp")) {
      return "Timestamp";
    } else if (type.equals("date")) {
      return "Date";
    } else if (type.equals("string")) {
      return "String";
    } else if (type.equals("char")) {
      return "Char";
    } else if (type.equals("varchar")) {
      return "VarChar";
    } else {
      return type;
    }
  }

  /**
   * Return the argument with the first letter capitalized
   */
  private static String getInitialCapWord(String word) {
    String firstLetterAsCap = word.substring(0, 1).toUpperCase();
    return firstLetterAsCap + word.substring(1);
  }

  private static final String ARITHMETIC_RETURN_TYPES[][] = {
    { "interval_year_month", "interval_year_month", "interval_year_month"},
    { "interval_year_month", "date", "date"},
    { "date", "interval_year_month", "date"},
    { "interval_year_month", "timestamp", "timestamp"},
    { "timestamp", "interval_year_month", "timestamp"},
    { "interval_day_time", "interval_day_time", "interval_day_time"},
    { "interval_day_time", "date", "timestamp"},
    { "date", "interval_day_time", "timestamp"},
    { "interval_day_time", "timestamp", "timestamp"},
    { "timestamp", "interval_day_time", "timestamp"},
    { "date", "date", "interval_day_time"},
    { "timestamp", "timestamp", "interval_day_time"},
    { "timestamp", "date", "interval_day_time"},
    { "date", "timestamp", "interval_day_time"},
    { "*", "double", "double"},
    { "double", "*", "double"},
  };

  private String getArithmeticReturnType(String operandType1,
      String operandType2) {
/*
    if (operandType1.equals("double") ||
        operandType2.equals("double")) {
      return "double";
    } else if (operandType1.equals("interval_year_month") &&
        operandType2.equals("interval_year_month")) {
      return "interval_year_month";
    } else if (operandType1.equals("interval_year_month") &&
        operandType2.equals("date")) {
      return "date";
    } else if (operandType1.equals("date") &&
        operandType2.equals("interval_year_month")) {
      return "date";
    } else if (operandType1.equals("interval_day_time") &&
        operandType2.equals("interval_day_time")) {
      return "interval_day_time";
    } else {
      return "long";
    }
*/
    for (String[] combination : ARITHMETIC_RETURN_TYPES) {
      if ((combination[0].equals("*") || combination[0].equals(operandType1)) &&
          (combination[1].equals("*") || combination[1].equals(operandType2))) {
        return combination[2];
      }
    }
    return "long";
  }

  private String getColumnVectorType(String primitiveType) throws Exception {
    if(primitiveType.equals("double")) {
      return "DoubleColumnVector";
    } else if (primitiveType.equals("long") || isDateIntervalType(primitiveType)) {
        return "LongColumnVector";
    } else if (primitiveType.equals("decimal")) {
        return "DecimalColumnVector";
    } else if (primitiveType.equals("string")) {
      return "BytesColumnVector";
    } else if (primitiveType.equals("timestamp")) {
      return "TimestampColumnVector";
    } else if (primitiveType.equals("interval_day_time")) {
      return "IntervalDayTimeColumnVector";
    }
    throw new Exception("Unimplemented primitive column vector type: " + primitiveType);
  }

  private String getVectorPrimitiveType(String columnVectorType) throws Exception {
    if (columnVectorType.equals("LongColumnVector")) {
      return "long";
    } else if (columnVectorType.equals("double")) {
      return "double";
    } else if (columnVectorType.equals("DecimalColumnVector")) {
      return "decimal";
    } else if (columnVectorType.equals("BytesColumnVector")) {
      return "string";
    }
    throw new Exception("Could not determine primitive type for column vector type: " + columnVectorType);
  }

  private String getOutputWritableType(String primitiveType) throws Exception {
    if (primitiveType.equals("long")) {
      return "LongWritable";
    } else if (primitiveType.equals("double")) {
      return "DoubleWritable";
    } else if (primitiveType.equals("decimal")) {
      return "HiveDecimalWritable";
    } else if (primitiveType.equals("interval_year_month")) {
      return "HiveIntervalYearMonthWritable";
    } else if (primitiveType.equals("interval_day_time")) {
      return "HiveIntervalDayTimeWritable";
    } else if (primitiveType.equals("date")) {
      return "HiveDateWritable";
    } else if (primitiveType.equals("timestamp")) {
      return "HiveTimestampWritable";
    }
    throw new Exception("Unimplemented primitive output writable: " + primitiveType);
  }

  private String getOutputObjectInspector(String primitiveType) throws Exception {
    if (primitiveType.equals("long")) {
      return "PrimitiveObjectInspectorFactory.writableLongObjectInspector";
    } else if (primitiveType.equals("double")) {
      return "PrimitiveObjectInspectorFactory.writableDoubleObjectInspector";
    } else if (primitiveType.equals("decimal")) {
      return "PrimitiveObjectInspectorFactory.writableHiveDecimalObjectInspector";
    } else if (primitiveType.equals("interval_year_month")) {
      return "PrimitiveObjectInspectorFactory.writableHiveIntervalYearMonthObjectInspector";
    } else if (primitiveType.equals("interval_day_time")) {
      return "PrimitiveObjectInspectorFactory.writableHiveIntervalDayTimeObjectInspector";
    } else if (primitiveType.equals("date")) {
      return "PrimitiveObjectInspectorFactory.writableDateObjectInspector";
    } else if (primitiveType.equals("timestamp")) {
      return "PrimitiveObjectInspectorFactory.writableTimestampObjectInspector";
    }
    throw new Exception("Unimplemented primitive output inspector: " + primitiveType);
  }

  public void setTemplateBaseDir(String templateBaseDir) {
    this.templateBaseDir = templateBaseDir;
  }

  public void setBuildDir(String buildDir) {
    this.buildDir = buildDir;
  }
}

