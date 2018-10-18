/*
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
package org.apache.hadoop.hive.ql.udf.generic;

import java.text.SimpleDateFormat;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDateSubColCol;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDateSubColScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorUDFDateSubScalarCol;

/**
 * UDFDateSub.
 *
 * Subtract a number of days to the date. The time part of the string will be
 * ignored.
 *
 * NOTE: This is a subset of what MySQL offers as:
 * http://dev.mysql.com/doc/refman
 * /5.1/en/date-and-time-functions.html#function_date-sub
 *
 */
@Description(name = "date_sub",
    value = "_FUNC_(start_date, num_days) - Returns the date that is num_days before start_date.",
    extended = "start_date is a string in the format 'yyyy-MM-dd HH:mm:ss' or"
        + " 'yyyy-MM-dd'. num_days is a number. The time part of start_date is "
        + "ignored.\n"
        + "Example:\n "
        + "  > SELECT _FUNC_('2009-07-30', 1) FROM src LIMIT 1;\n"
        + "  '2009-07-29'")
@VectorizedExpressions({VectorUDFDateSubColScalar.class, VectorUDFDateSubScalarCol.class, VectorUDFDateSubColCol.class})
public class GenericUDFDateSub extends GenericUDFDateAdd {
  private transient SimpleDateFormat formatter = new SimpleDateFormat("yyyy-MM-dd");

  public GenericUDFDateSub() {
    this.signModifier = -1;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("date_sub", children);
  }
}
