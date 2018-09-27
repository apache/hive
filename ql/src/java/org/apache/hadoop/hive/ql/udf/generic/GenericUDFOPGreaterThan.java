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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressionsSupportDecimal64;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.io.Text;

/**
 * GenericUDF Class for operation GreaterThan.
 */
@Description(name = ">", value = "a _FUNC_ b - Returns TRUE if a is greater than b")
@VectorizedExpressions({LongColGreaterLongColumn.class, LongColGreaterDoubleColumn.class,
  DoubleColGreaterLongColumn.class, DoubleColGreaterDoubleColumn.class,
  LongColGreaterLongScalar.class, LongColGreaterDoubleScalar.class,
  DoubleColGreaterLongScalar.class, DoubleColGreaterDoubleScalar.class,
  LongScalarGreaterLongColumn.class, LongScalarGreaterDoubleColumn.class,
  DoubleScalarGreaterLongColumn.class, DoubleScalarGreaterDoubleColumn.class,

  DecimalColGreaterDecimalColumn.class, DecimalColGreaterDecimalScalar.class,
  DecimalScalarGreaterDecimalColumn.class,
  Decimal64ColGreaterDecimal64Column.class, Decimal64ColGreaterDecimal64Scalar.class,
  Decimal64ScalarGreaterDecimal64Column.class,

  StringGroupColGreaterStringGroupColumn.class, FilterStringGroupColGreaterStringGroupColumn.class,
  StringGroupColGreaterStringScalar.class,
  StringGroupColGreaterVarCharScalar.class, StringGroupColGreaterCharScalar.class,
  StringScalarGreaterStringGroupColumn.class,
  VarCharScalarGreaterStringGroupColumn.class, CharScalarGreaterStringGroupColumn.class,

  FilterStringGroupColGreaterStringScalar.class, FilterStringScalarGreaterStringGroupColumn.class,
  FilterStringGroupColGreaterVarCharScalar.class, FilterVarCharScalarGreaterStringGroupColumn.class,
  FilterStringGroupColGreaterCharScalar.class, FilterCharScalarGreaterStringGroupColumn.class,

  FilterLongColGreaterLongColumn.class, FilterLongColGreaterDoubleColumn.class,
  FilterDoubleColGreaterLongColumn.class, FilterDoubleColGreaterDoubleColumn.class,
  FilterLongColGreaterLongScalar.class, FilterLongColGreaterDoubleScalar.class,
  FilterDoubleColGreaterLongScalar.class, FilterDoubleColGreaterDoubleScalar.class,
  FilterLongScalarGreaterLongColumn.class, FilterLongScalarGreaterDoubleColumn.class,
  FilterDoubleScalarGreaterLongColumn.class, FilterDoubleScalarGreaterDoubleColumn.class,

  FilterDecimalColGreaterDecimalColumn.class, FilterDecimalColGreaterDecimalScalar.class,
  FilterDecimalScalarGreaterDecimalColumn.class,

  FilterDecimal64ColGreaterDecimal64Column.class, FilterDecimal64ColGreaterDecimal64Scalar.class,
  FilterDecimal64ScalarGreaterDecimal64Column.class,

  TimestampColGreaterTimestampColumn.class,
  TimestampColGreaterTimestampScalar.class, TimestampScalarGreaterTimestampColumn.class,
  TimestampColGreaterLongColumn.class,
  TimestampColGreaterLongScalar.class, TimestampScalarGreaterLongColumn.class,
  TimestampColGreaterDoubleColumn.class,
  TimestampColGreaterDoubleScalar.class, TimestampScalarGreaterDoubleColumn.class,
  LongColGreaterTimestampColumn.class,
  LongColGreaterTimestampScalar.class, LongScalarGreaterTimestampColumn.class,
  DoubleColGreaterTimestampColumn.class,
  DoubleColGreaterTimestampScalar.class, DoubleScalarGreaterTimestampColumn.class,

  FilterTimestampColGreaterTimestampColumn.class,
  FilterTimestampColGreaterTimestampScalar.class, FilterTimestampScalarGreaterTimestampColumn.class,
  FilterTimestampColGreaterLongColumn.class,
  FilterTimestampColGreaterLongScalar.class, FilterTimestampScalarGreaterLongColumn.class,
  FilterTimestampColGreaterDoubleColumn.class,
  FilterTimestampColGreaterDoubleScalar.class, FilterTimestampScalarGreaterDoubleColumn.class,
  FilterLongColGreaterTimestampColumn.class,
  FilterLongColGreaterTimestampScalar.class, FilterLongScalarGreaterTimestampColumn.class,
  FilterDoubleColGreaterTimestampColumn.class,
  FilterDoubleColGreaterTimestampScalar.class, FilterDoubleScalarGreaterTimestampColumn.class,

  IntervalYearMonthScalarGreaterIntervalYearMonthColumn.class, FilterIntervalYearMonthScalarGreaterIntervalYearMonthColumn.class,
  IntervalYearMonthColGreaterIntervalYearMonthScalar.class, FilterIntervalYearMonthColGreaterIntervalYearMonthScalar.class,

  IntervalDayTimeColGreaterIntervalDayTimeColumn.class, FilterIntervalDayTimeColGreaterIntervalDayTimeColumn.class,
  IntervalDayTimeScalarGreaterIntervalDayTimeColumn.class, FilterIntervalDayTimeScalarGreaterIntervalDayTimeColumn.class,
  IntervalDayTimeColGreaterIntervalDayTimeScalar.class, FilterIntervalDayTimeColGreaterIntervalDayTimeScalar.class,

  DateColGreaterDateScalar.class,FilterDateColGreaterDateScalar.class,
  DateScalarGreaterDateColumn.class,FilterDateScalarGreaterDateColumn.class,
  })
@VectorizedExpressionsSupportDecimal64()
@NDV(maxNdv = 2)
public class GenericUDFOPGreaterThan extends GenericUDFBaseCompare {
  public GenericUDFOPGreaterThan(){
    this.opName = "GREATER THAN";
    this.opDisplayName = ">";
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o0,o1;
    o0 = arguments[0].get();
    if (o0 == null) {
      return null;
    }
    o1 = arguments[1].get();
    if (o1 == null) {
      return null;
    }

    switch(compareType) {
    case COMPARE_TEXT:
      Text t0, t1;
      t0 = soi0.getPrimitiveWritableObject(o0);
      t1 = soi1.getPrimitiveWritableObject(o1);
      result.set(t0.compareTo(t1) > 0);
      break;
    case COMPARE_INT:
      result.set(ioi0.get(o0) > ioi1.get(o1));
      break;
    case COMPARE_LONG:
      result.set(loi0.get(o0) > loi1.get(o1));
      break;
    case COMPARE_BYTE:
      result.set(byoi0.get(o0) > byoi1.get(o1));
      break;
    case COMPARE_BOOL:
      boolean b0 = boi0.get(o0);
      boolean b1 = boi1.get(o1);
      result.set(b0 && !b1);
      break;
    case COMPARE_STRING:
      String s0, s1;
      s0 = soi0.getPrimitiveJavaObject(o0);
      s1 = soi1.getPrimitiveJavaObject(o1);
      result.set(s0.compareTo(s1) > 0);
      break;
    case SAME_TYPE:
      result.set(ObjectInspectorUtils.compare(
          o0, argumentOIs[0], o1, argumentOIs[1]) > 0);
      break;
    default:
      Object converted_o0 = converter0.convert(o0);
      if (converted_o0 == null) {
        return null;
      }
      Object converted_o1 = converter1.convert(o1);
      if (converted_o1 == null) {
        return null;
      }
      result.set(ObjectInspectorUtils.compare(
          converted_o0, compareOI,
          converted_o1, compareOI) > 0);
    }
    return result;
  }

  @Override
  public GenericUDF flip() {
    return new GenericUDFOPLessThan();
  }

  @Override
  public GenericUDF negative() {
    return new GenericUDFOPEqualOrLessThan();
  }
}

