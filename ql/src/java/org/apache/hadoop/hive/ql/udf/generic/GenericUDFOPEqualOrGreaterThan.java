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
 * GenericUDF Class for operation EqualOrGreaterThan.
 */
@Description(name = ">=", value = "a _FUNC_ b - Returns TRUE if a is not smaller than b")
@VectorizedExpressions({LongColGreaterEqualLongColumn.class, LongColGreaterEqualDoubleColumn.class,
  DoubleColGreaterEqualLongColumn.class, DoubleColGreaterEqualDoubleColumn.class,
  LongColGreaterEqualLongScalar.class, LongColGreaterEqualDoubleScalar.class,
  DoubleColGreaterEqualLongScalar.class, DoubleColGreaterEqualDoubleScalar.class,
  LongScalarGreaterEqualLongColumn.class, LongScalarGreaterEqualDoubleColumn.class,
  DoubleScalarGreaterEqualLongColumn.class, DoubleScalarGreaterEqualDoubleColumn.class,

  DecimalColGreaterEqualDecimalColumn.class, DecimalColGreaterEqualDecimalScalar.class,
  DecimalScalarGreaterEqualDecimalColumn.class,
  Decimal64ColGreaterEqualDecimal64Column.class, Decimal64ColGreaterEqualDecimal64Scalar.class,
  Decimal64ScalarGreaterEqualDecimal64Column.class,

  StringGroupColGreaterEqualStringGroupColumn.class, FilterStringGroupColGreaterEqualStringGroupColumn.class,
  StringGroupColGreaterEqualStringScalar.class,
  StringGroupColGreaterEqualVarCharScalar.class, StringGroupColGreaterEqualCharScalar.class,
  StringScalarGreaterEqualStringGroupColumn.class,
  VarCharScalarGreaterEqualStringGroupColumn.class, CharScalarGreaterEqualStringGroupColumn.class,

  FilterStringGroupColGreaterEqualStringScalar.class, FilterStringScalarGreaterEqualStringGroupColumn.class,
  FilterStringGroupColGreaterEqualVarCharScalar.class, FilterVarCharScalarGreaterEqualStringGroupColumn.class,
  FilterStringGroupColGreaterEqualCharScalar.class, FilterCharScalarGreaterEqualStringGroupColumn.class,

  FilterLongColGreaterEqualLongColumn.class, FilterLongColGreaterEqualDoubleColumn.class,
  FilterDoubleColGreaterEqualLongColumn.class, FilterDoubleColGreaterEqualDoubleColumn.class,
  FilterLongColGreaterEqualLongScalar.class, FilterLongColGreaterEqualDoubleScalar.class,
  FilterDoubleColGreaterEqualLongScalar.class, FilterDoubleColGreaterEqualDoubleScalar.class,
  FilterLongScalarGreaterEqualLongColumn.class, FilterLongScalarGreaterEqualDoubleColumn.class,
  FilterDoubleScalarGreaterEqualLongColumn.class, FilterDoubleScalarGreaterEqualDoubleColumn.class,

  FilterDecimalColGreaterEqualDecimalColumn.class, FilterDecimalColGreaterEqualDecimalScalar.class,
  FilterDecimalScalarGreaterEqualDecimalColumn.class,

  FilterDecimal64ColGreaterEqualDecimal64Column.class, FilterDecimal64ColGreaterEqualDecimal64Scalar.class,
  FilterDecimal64ScalarGreaterEqualDecimal64Column.class,

  TimestampColGreaterEqualTimestampColumn.class,
  TimestampColGreaterEqualTimestampScalar.class, TimestampScalarGreaterEqualTimestampColumn.class,
  TimestampColGreaterEqualLongColumn.class,
  TimestampColGreaterEqualLongScalar.class, TimestampScalarGreaterEqualLongColumn.class,
  TimestampColGreaterEqualDoubleColumn.class,
  TimestampColGreaterEqualDoubleScalar.class, TimestampScalarGreaterEqualDoubleColumn.class,
  LongColGreaterEqualTimestampColumn.class,
  LongColGreaterEqualTimestampScalar.class, LongScalarGreaterEqualTimestampColumn.class,
  DoubleColGreaterEqualTimestampColumn.class,
  DoubleColGreaterEqualTimestampScalar.class, DoubleScalarGreaterEqualTimestampColumn.class,

  FilterTimestampColGreaterEqualTimestampColumn.class,
  FilterTimestampColGreaterEqualTimestampScalar.class, FilterTimestampScalarGreaterEqualTimestampColumn.class,
  FilterTimestampColGreaterEqualLongColumn.class,
  FilterTimestampColGreaterEqualLongScalar.class, FilterTimestampScalarGreaterEqualLongColumn.class,
  FilterTimestampColGreaterEqualDoubleColumn.class,
  FilterTimestampColGreaterEqualDoubleScalar.class, FilterTimestampScalarGreaterEqualDoubleColumn.class,
  FilterLongColGreaterEqualTimestampColumn.class,
  FilterLongColGreaterEqualTimestampScalar.class, FilterLongScalarGreaterEqualTimestampColumn.class,
  FilterDoubleColGreaterEqualTimestampColumn.class,
  FilterDoubleColGreaterEqualTimestampScalar.class, FilterDoubleScalarGreaterEqualTimestampColumn.class,

  IntervalYearMonthScalarGreaterEqualIntervalYearMonthColumn.class, FilterIntervalYearMonthScalarGreaterEqualIntervalYearMonthColumn.class,
  IntervalYearMonthColGreaterEqualIntervalYearMonthScalar.class, FilterIntervalYearMonthColGreaterEqualIntervalYearMonthScalar.class,

  IntervalDayTimeColGreaterEqualIntervalDayTimeColumn.class, FilterIntervalDayTimeColGreaterEqualIntervalDayTimeColumn.class,
  IntervalDayTimeScalarGreaterEqualIntervalDayTimeColumn.class, FilterIntervalDayTimeScalarGreaterEqualIntervalDayTimeColumn.class,
  IntervalDayTimeColGreaterEqualIntervalDayTimeScalar.class, FilterIntervalDayTimeColGreaterEqualIntervalDayTimeScalar.class,

  DateColGreaterEqualDateScalar.class,FilterDateColGreaterEqualDateScalar.class,
  DateScalarGreaterEqualDateColumn.class,FilterDateScalarGreaterEqualDateColumn.class,
  })
@VectorizedExpressionsSupportDecimal64()
@NDV(maxNdv = 2)
public class GenericUDFOPEqualOrGreaterThan extends GenericUDFBaseCompare {
  public GenericUDFOPEqualOrGreaterThan(){
    this.opName = "EQUAL OR GREATER THAN";
    this.opDisplayName = ">=";
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
      result.set(t0.compareTo(t1) >= 0);
      break;
    case COMPARE_INT:
      result.set(ioi0.get(o0) >= ioi1.get(o1));
      break;
    case COMPARE_LONG:
      result.set(loi0.get(o0) >= loi1.get(o1));
      break;
    case COMPARE_BYTE:
      result.set(byoi0.get(o0) >= byoi1.get(o1));
      break;
    case COMPARE_BOOL:
      boolean b0 = boi0.get(o0);
      boolean b1 = boi1.get(o1);
      result.set(b0 || !b1);
      break;
    case COMPARE_STRING:
      String s0, s1;
      s0 = soi0.getPrimitiveJavaObject(o0);
      s1 = soi1.getPrimitiveJavaObject(o1);
      result.set(s0.compareTo(s1) >= 0);
      break;
    case SAME_TYPE:
      result.set(ObjectInspectorUtils.compare(
          o0, argumentOIs[0], o1, argumentOIs[1]) >= 0);
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
          converted_o1, compareOI) >= 0);
    }
    return result;
  }

  @Override
  public GenericUDF flip() {
    return new GenericUDFOPEqualOrLessThan();
  }

  @Override
  public GenericUDF negative() {
    return new GenericUDFOPLessThan();
  }
}
