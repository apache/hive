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

package org.apache.hadoop.hive.ant;

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

      {"ColumnCompareScalar", "Equal", "long", "long", "=="},
      {"ColumnCompareScalar", "Equal", "double", "long", "=="},
      {"ColumnCompareScalar", "NotEqual", "long", "long", "!="},
      {"ColumnCompareScalar", "NotEqual", "double", "long", "!="},
      {"ColumnCompareScalar", "Less", "long", "long", "<"},
      {"ColumnCompareScalar", "Less", "double", "long", "<"},
      {"ColumnCompareScalar", "LessEqual", "long", "long", "<="},
      {"ColumnCompareScalar", "LessEqual", "double", "long", "<="},
      {"ColumnCompareScalar", "Greater", "long", "long", ">"},
      {"ColumnCompareScalar", "Greater", "double", "long", ">"},
      {"ColumnCompareScalar", "GreaterEqual", "long", "long", ">="},
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

      {"ScalarCompareColumn", "Equal", "long", "long", "=="},
      {"ScalarCompareColumn", "Equal", "double", "long", "=="},
      {"ScalarCompareColumn", "NotEqual", "long", "long", "!="},
      {"ScalarCompareColumn", "NotEqual", "double", "long", "!="},
      {"ScalarCompareColumn", "Less", "long", "long", "<"},
      {"ScalarCompareColumn", "Less", "double", "long", "<"},
      {"ScalarCompareColumn", "LessEqual", "long", "long", "<="},
      {"ScalarCompareColumn", "LessEqual", "double", "long", "<="},
      {"ScalarCompareColumn", "Greater", "long", "long", ">"},
      {"ScalarCompareColumn", "Greater", "double", "long", ">"},
      {"ScalarCompareColumn", "GreaterEqual", "long", "long", ">="},
      {"ScalarCompareColumn", "GreaterEqual", "double", "long", ">="},

      {"TimestampColumnCompareTimestampScalar", "Equal"},
      {"TimestampColumnCompareTimestampScalar", "NotEqual"},
      {"TimestampColumnCompareTimestampScalar", "Less"},
      {"TimestampColumnCompareTimestampScalar", "LessEqual"},
      {"TimestampColumnCompareTimestampScalar", "Greater"},
      {"TimestampColumnCompareTimestampScalar", "GreaterEqual"},

      {"TimestampColumnCompareScalar", "Equal", "long"},
      {"TimestampColumnCompareScalar", "Equal", "double"},
      {"TimestampColumnCompareScalar", "NotEqual", "long"},
      {"TimestampColumnCompareScalar", "NotEqual", "double"},
      {"TimestampColumnCompareScalar", "Less", "long"},
      {"TimestampColumnCompareScalar", "Less", "double"},
      {"TimestampColumnCompareScalar", "LessEqual", "long"},
      {"TimestampColumnCompareScalar", "LessEqual", "double"},
      {"TimestampColumnCompareScalar", "Greater", "long"},
      {"TimestampColumnCompareScalar", "Greater", "double"},
      {"TimestampColumnCompareScalar", "GreaterEqual", "long"},
      {"TimestampColumnCompareScalar", "GreaterEqual", "double"},

      {"TimestampScalarCompareTimestampColumn", "Equal"},
      {"TimestampScalarCompareTimestampColumn", "NotEqual"},
      {"TimestampScalarCompareTimestampColumn", "Less"},
      {"TimestampScalarCompareTimestampColumn", "LessEqual"},
      {"TimestampScalarCompareTimestampColumn", "Greater"},
      {"TimestampScalarCompareTimestampColumn", "GreaterEqual"},

      {"ScalarCompareTimestampColumn", "Equal", "long"},
      {"ScalarCompareTimestampColumn", "Equal", "double"},
      {"ScalarCompareTimestampColumn", "NotEqual", "long"},
      {"ScalarCompareTimestampColumn", "NotEqual", "double"},
      {"ScalarCompareTimestampColumn", "Less", "long"},
      {"ScalarCompareTimestampColumn", "Less", "double"},
      {"ScalarCompareTimestampColumn", "LessEqual", "long"},
      {"ScalarCompareTimestampColumn", "LessEqual", "double"},
      {"ScalarCompareTimestampColumn", "Greater", "long"},
      {"ScalarCompareTimestampColumn", "Greater", "double"},
      {"ScalarCompareTimestampColumn", "GreaterEqual", "long"},
      {"ScalarCompareTimestampColumn", "GreaterEqual", "double"},

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

      {"FilterTimestampColumnCompareTimestampScalar", "Equal"},
      {"FilterTimestampColumnCompareTimestampScalar", "NotEqual"},
      {"FilterTimestampColumnCompareTimestampScalar", "Less"},
      {"FilterTimestampColumnCompareTimestampScalar", "LessEqual"},
      {"FilterTimestampColumnCompareTimestampScalar", "Greater"},
      {"FilterTimestampColumnCompareTimestampScalar", "GreaterEqual"},

      {"FilterTimestampColumnCompareScalar", "Equal", "long"},
      {"FilterTimestampColumnCompareScalar", "Equal", "double"},
      {"FilterTimestampColumnCompareScalar", "NotEqual", "long"},
      {"FilterTimestampColumnCompareScalar", "NotEqual", "double"},
      {"FilterTimestampColumnCompareScalar", "Less", "long"},
      {"FilterTimestampColumnCompareScalar", "Less", "double"},
      {"FilterTimestampColumnCompareScalar", "LessEqual", "long"},
      {"FilterTimestampColumnCompareScalar", "LessEqual", "double"},
      {"FilterTimestampColumnCompareScalar", "Greater", "long"},
      {"FilterTimestampColumnCompareScalar", "Greater", "double"},
      {"FilterTimestampColumnCompareScalar", "GreaterEqual", "long"},
      {"FilterTimestampColumnCompareScalar", "GreaterEqual", "double"},

      {"FilterTimestampScalarCompareTimestampColumn", "Equal"},
      {"FilterTimestampScalarCompareTimestampColumn", "NotEqual"},
      {"FilterTimestampScalarCompareTimestampColumn", "Less"},
      {"FilterTimestampScalarCompareTimestampColumn", "LessEqual"},
      {"FilterTimestampScalarCompareTimestampColumn", "Greater"},
      {"FilterTimestampScalarCompareTimestampColumn", "GreaterEqual"},

      {"FilterScalarCompareTimestampColumn", "Equal", "long"},
      {"FilterScalarCompareTimestampColumn", "Equal", "double"},
      {"FilterScalarCompareTimestampColumn", "NotEqual", "long"},
      {"FilterScalarCompareTimestampColumn", "NotEqual", "double"},
      {"FilterScalarCompareTimestampColumn", "Less", "long"},
      {"FilterScalarCompareTimestampColumn", "Less", "double"},
      {"FilterScalarCompareTimestampColumn", "LessEqual", "long"},
      {"FilterScalarCompareTimestampColumn", "LessEqual", "double"},
      {"FilterScalarCompareTimestampColumn", "Greater", "long"},
      {"FilterScalarCompareTimestampColumn", "Greater", "double"},
      {"FilterScalarCompareTimestampColumn", "GreaterEqual", "long"},
      {"FilterScalarCompareTimestampColumn", "GreaterEqual", "double"},

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

      {"FilterDecimalColumnCompareScalar", "Equal", "=="},
      {"FilterDecimalColumnCompareScalar", "NotEqual", "!="},
      {"FilterDecimalColumnCompareScalar", "Less", "<"},
      {"FilterDecimalColumnCompareScalar", "LessEqual", "<="},
      {"FilterDecimalColumnCompareScalar", "Greater", ">"},
      {"FilterDecimalColumnCompareScalar", "GreaterEqual", ">="},

      {"FilterDecimalScalarCompareColumn", "Equal", "=="},
      {"FilterDecimalScalarCompareColumn", "NotEqual", "!="},
      {"FilterDecimalScalarCompareColumn", "Less", "<"},
      {"FilterDecimalScalarCompareColumn", "LessEqual", "<="},
      {"FilterDecimalScalarCompareColumn", "Greater", ">"},
      {"FilterDecimalScalarCompareColumn", "GreaterEqual", ">="},

      {"FilterDecimalColumnCompareColumn", "Equal", "=="},
      {"FilterDecimalColumnCompareColumn", "NotEqual", "!="},
      {"FilterDecimalColumnCompareColumn", "Less", "<"},
      {"FilterDecimalColumnCompareColumn", "LessEqual", "<="},
      {"FilterDecimalColumnCompareColumn", "Greater", ">"},
      {"FilterDecimalColumnCompareColumn", "GreaterEqual", ">="},

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

      {"ColumnCompareColumn", "Equal", "long", "long", "=="},
      {"ColumnCompareColumn", "Equal", "double", "long", "=="},
      {"ColumnCompareColumn", "NotEqual", "long", "long", "!="},
      {"ColumnCompareColumn", "NotEqual", "double", "long", "!="},
      {"ColumnCompareColumn", "Less", "long", "long", "<"},
      {"ColumnCompareColumn", "Less", "double", "long", "<"},
      {"ColumnCompareColumn", "LessEqual", "long", "long", "<="},
      {"ColumnCompareColumn", "LessEqual", "double", "long", "<="},
      {"ColumnCompareColumn", "Greater", "long", "long", ">"},
      {"ColumnCompareColumn", "Greater", "double", "long", ">"},
      {"ColumnCompareColumn", "GreaterEqual", "long", "long", ">="},
      {"ColumnCompareColumn", "GreaterEqual", "double", "long", ">="},

      // template, <ClassNamePrefix>, <ReturnType>, <OperandType>, <FuncName>, <OperandCast>,
      //   <ResultCast>, <Cleanup> <VectorExprArgType>
      {"ColumnUnaryFunc", "FuncRound", "double", "double", "MathExpr.round", "", "", "", ""},
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
      {"DecimalColumnUnaryFunc", "FuncNegate", "decimal", "DecimalUtil.negate"},

      // Casts
      {"ColumnUnaryFunc", "Cast", "long", "double", "", "", "(long)", "", ""},
      {"ColumnUnaryFunc", "Cast", "double", "long", "", "", "(double)", "", ""},
      {"ColumnUnaryFunc", "CastTimestampToLongVia", "long", "long", "MathExpr.fromTimestamp", "",
        "", "", "timestamp"},
      {"ColumnUnaryFunc", "CastTimestampToDoubleVia", "double", "long",
          "MathExpr.fromTimestampToDouble", "", "", "", "timestamp"},
      {"ColumnUnaryFunc", "CastDoubleToBooleanVia", "long", "double", "MathExpr.toBool", "",
        "", "", ""},
      {"ColumnUnaryFunc", "CastLongToBooleanVia", "long", "long", "MathExpr.toBool", "",
        "", "", ""},
      {"ColumnUnaryFunc", "CastDateToBooleanVia", "long", "long", "MathExpr.toBool", "",
            "", "", "date"},
      {"ColumnUnaryFunc", "CastTimestampToBooleanVia", "long", "long", "MathExpr.toBool", "",
            "", "", "timestamp"},
      {"ColumnUnaryFunc", "CastLongToTimestampVia", "long", "long", "MathExpr.longToTimestamp", "",
          "", "", ""},
      {"ColumnUnaryFunc", "CastDoubleToTimestampVia", "long", "double",
         "MathExpr.doubleToTimestamp", "", "", "", ""},

      // Boolean to long is done with an IdentityExpression
      // Boolean to double is done with standard Long to Double cast
      // See org.apache.hadoop.hive.ql.exec.vector.expressions for remaining cast VectorExpression
      // classes

      {"ColumnUnaryMinus", "long"},
      {"ColumnUnaryMinus", "double"},

      // IF conditional expression
      // fileHeader, resultType, arg2Type, arg3Type
      {"IfExprColumnColumn", "long"},
      {"IfExprColumnColumn", "double"},
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
      } else if (tdesc[0].equals("TimestampScalarCompareTimestampColumn")) {
          generateTimestampScalarCompareTimestampColumn(tdesc);
      } else if (tdesc[0].equals("ScalarCompareTimestampColumn")) {
          generateScalarCompareTimestampColumn(tdesc);
      } else if (tdesc[0].equals("TimestampColumnCompareTimestampScalar")) {
          generateTimestampColumnCompareTimestampScalar(tdesc);
      } else if (tdesc[0].equals("TimestampColumnCompareScalar")) {
          generateTimestampColumnCompareScalar(tdesc);
      } else if (tdesc[0].equals("FilterColumnCompareScalar")) {
        generateFilterColumnCompareScalar(tdesc);
      } else if (tdesc[0].equals("FilterScalarCompareColumn")) {
        generateFilterScalarCompareColumn(tdesc);
      } else if (tdesc[0].equals("FilterTimestampColumnCompareTimestampScalar")) {
          generateFilterTimestampColumnCompareTimestampScalar(tdesc);
      } else if (tdesc[0].equals("FilterTimestampColumnCompareScalar")) {
          generateFilterTimestampColumnCompareScalar(tdesc);
      } else if (tdesc[0].equals("FilterTimestampScalarCompareTimestampColumn")) {
          generateFilterTimestampScalarCompareTimestampColumn(tdesc);
      } else if (tdesc[0].equals("FilterScalarCompareTimestampColumn")) {
          generateFilterScalarCompareTimestampColumn(tdesc);
      } else if (tdesc[0].equals("FilterColumnBetween")) {
        generateFilterColumnBetween(tdesc);
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
        generateVectorUDAFMinMaxDecimal(tdesc);
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
      } else if (tdesc[0].equals("IfExprColumnColumn")) {
        generateIfExprColumnColumn(tdesc);
      } else if (tdesc[0].equals("IfExprColumnScalar")) {
        generateIfExprColumnScalar(tdesc);
      } else if (tdesc[0].equals("IfExprScalarColumn")) {
        generateIfExprScalarColumn(tdesc);
      } else if (tdesc[0].equals("IfExprScalarScalar")) {
        generateIfExprScalarScalar(tdesc);
      } else if (tdesc[0].equals("FilterDecimalColumnCompareScalar")) {
        generateFilterDecimalColumnCompareScalar(tdesc);
      } else if (tdesc[0].equals("FilterDecimalScalarCompareColumn")) {
        generateFilterDecimalScalarCompareColumn(tdesc);
      } else if (tdesc[0].equals("FilterDecimalColumnCompareColumn")) {
        generateFilterDecimalColumnCompareColumn(tdesc);
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

  private void generateVectorUDAFMinMaxDecimal(String[] tdesc) throws Exception {
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
    // Expand, and write result
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<InputColumnVectorType>", inputColumnVectorType);
    templateString = templateString.replaceAll("<OutputColumnVectorType>", outputColumnVectorType);
    templateString = templateString.replaceAll("<OperandType>", operandType);
    templateString = templateString.replaceAll("<ReturnType>", returnType);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateIfExprColumnColumn(String[] tdesc) throws Exception {
    String operandType = tdesc[1];
    String inputColumnVectorType = this.getColumnVectorType(operandType);
    String outputColumnVectorType = inputColumnVectorType;
    String returnType = operandType;
    String className = "IfExpr" + getCamelCaseType(operandType) + "Column"
        + getCamelCaseType(operandType) + "Column";
    String outputFile = joinPath(this.expressionOutputDirectory, className + ".java");
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    // Expand, and write result
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<InputColumnVectorType>", inputColumnVectorType);
    templateString = templateString.replaceAll("<OperandType>", operandType);
    String vectorExprArgType = operandType;

    // Toss in timestamp and date.
    if (operandType.equals("long")) {
      // Let comparisons occur for DATE and TIMESTAMP, too.
      vectorExprArgType = "int_datetime_family";
    }
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
      vectorExprArgType2 = "int_datetime_family";
      vectorExprArgType3 = "int_datetime_family";
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
      vectorExprArgType2 = "int_datetime_family";
      vectorExprArgType3 = "int_datetime_family";
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
      vectorExprArgType2 = "int_datetime_family";
      vectorExprArgType3 = "int_datetime_family";
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

    // For column to column only, we toss in timestamp and date.
    // But {timestamp|date} and scalar must be handled separately.
    if (operandType1.equals("long") && operandType2.equals("long")) {
      // Let comparisons occur for DATE and TIMESTAMP, too.
      vectorExprArgType1 = "int_datetime_family";
      vectorExprArgType2 = "int_datetime_family";
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

  private void generateTimestampScalarCompareTimestampColumn(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String className = "TimestampScalar" + operatorName + "TimestampColumn";
    String baseClassName = "LongScalar" + operatorName + "LongColumn";
    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateTimestampColumnCompareTimestampScalar(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String className = "TimestampCol" + operatorName + "TimestampScalar";
    String baseClassName = "LongCol" + operatorName + "LongScalar";
    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterTimestampColumnCompareTimestampScalar(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String className = "FilterTimestampCol" + operatorName + "TimestampScalar";
    String baseClassName = "FilterLongCol" + operatorName + "LongScalar";
    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterTimestampScalarCompareTimestampColumn(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String className = "FilterTimestampScalar" + operatorName + "TimestampColumn";
    String baseClassName = "FilterLongScalar" + operatorName + "LongColumn";
    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private String timestampScalarConversion(String operandType) {
    if (operandType.equals("long")) {
      return "secondsToNanoseconds";
    } else if (operandType.equals("double")) {
      return "doubleToNanoseconds";
    } else {
      return "unknown";
    }
  }

  private void generateScalarCompareTimestampColumn(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType = tdesc[2];
    String className = getCamelCaseType(operandType) + "Scalar" + operatorName + "TimestampColumn";
    String baseClassName = "LongScalar" + operatorName + "LongColumn";
    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    templateString = templateString.replaceAll("<OperandType>", operandType);
    templateString = templateString.replaceAll("<TimestampScalarConversion>", timestampScalarConversion(operandType));
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateTimestampColumnCompareScalar(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType = tdesc[2];
    String className = "TimestampCol" + operatorName + getCamelCaseType(operandType) + "Scalar";
    String baseClassName = "LongCol" + operatorName + "LongScalar";
    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    templateString = templateString.replaceAll("<OperandType>", operandType);
    templateString = templateString.replaceAll("<TimestampScalarConversion>", timestampScalarConversion(operandType));
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterTimestampColumnCompareScalar(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType = tdesc[2];
    String className = "FilterTimestampCol" + operatorName + getCamelCaseType(operandType) + "Scalar";
    String baseClassName = "FilterLongCol" + operatorName + "LongScalar";
    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    templateString = templateString.replaceAll("<OperandType>", operandType);
    templateString = templateString.replaceAll("<TimestampScalarConversion>", timestampScalarConversion(operandType));
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }

  private void generateFilterScalarCompareTimestampColumn(String[] tdesc) throws Exception {
    String operatorName = tdesc[1];
    String operandType = tdesc[2];
    String className = "Filter" + getCamelCaseType(operandType) + "Scalar" + operatorName + "TimestampColumn";
    String baseClassName = "FilterLongScalar" + operatorName + "LongColumn";
    //Read the template into a string;
    File templateFile = new File(joinPath(this.expressionTemplateDirectory, tdesc[0] + ".txt"));
    String templateString = readFile(templateFile);
    templateString = templateString.replaceAll("<ClassName>", className);
    templateString = templateString.replaceAll("<BaseClassName>", baseClassName);
    templateString = templateString.replaceAll("<OperandType>", operandType);
    templateString = templateString.replaceAll("<TimestampScalarConversion>", timestampScalarConversion(operandType));
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);
  }


  private void generateColumnArithmeticOperatorColumn(String[] tdesc, String returnType,
         String className) throws Exception {
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
    templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
    templateString = templateString.replaceAll("<OperandType1>", operandType1);
    templateString = templateString.replaceAll("<OperandType2>", operandType2);
    templateString = templateString.replaceAll("<ReturnType>", returnType);
    writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);

    testCodeGen.addColumnScalarOperationTestCases(
          true,
          className,
          inputColumnVectorType,
          outputColumnVectorType,
          operandType2);
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
     templateString = templateString.replaceAll("<OperatorSymbol>", operatorSymbol);
     templateString = templateString.replaceAll("<OperandType1>", operandType1);
     templateString = templateString.replaceAll("<OperandType2>", operandType2);
     templateString = templateString.replaceAll("<ReturnType>", returnType);
     templateString = templateString.replaceAll("<CamelReturnType>", getCamelCaseType(returnType));
     writeFile(templateFile.lastModified(), expressionOutputDirectory, expressionClassesDirectory,
        className, templateString);

     testCodeGen.addColumnScalarOperationTestCases(
           false,
           className,
           inputColumnVectorType,
           outputColumnVectorType,
           operandType1);
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

  private void generateFilterDecimalColumnCompareScalar(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "FilterDecimalCol" + operatorName + "DecimalScalar";
    generateDecimalColumnCompare(tdesc, className);
  }

  private void generateFilterDecimalScalarCompareColumn(String[] tdesc) throws IOException {
    String operatorName = tdesc[1];
    String className = "FilterDecimalScalar" + operatorName + "DecimalColumn";
    generateDecimalColumnCompare(tdesc, className);
  }

  private void generateFilterDecimalColumnCompareColumn(String[] tdesc) throws IOException {
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

  private String getArithmeticReturnType(String operandType1,
      String operandType2) {
    if (operandType1.equals("double") ||
        operandType2.equals("double")) {
      return "double";
    } else {
      return "long";
    }
  }

  private String getColumnVectorType(String primitiveType) throws Exception {
    if(primitiveType.equals("double")) {
      return "DoubleColumnVector";
    } else if (primitiveType.equals("long")) {
        return "LongColumnVector";
    } else if (primitiveType.equals("decimal")) {
        return "DecimalColumnVector";
    } else if (primitiveType.equals("string")) {
      return "BytesColumnVector";
    }
    throw new Exception("Unimplemented primitive column vector type: " + primitiveType);
  }

  private String getOutputWritableType(String primitiveType) throws Exception {
    if (primitiveType.equals("long")) {
      return "LongWritable";
    } else if (primitiveType.equals("double")) {
      return "DoubleWritable";
    } else if (primitiveType.equals("decimal")) {
      return "HiveDecimalWritable";
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

