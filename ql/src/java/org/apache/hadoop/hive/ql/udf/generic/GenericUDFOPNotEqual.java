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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;

/**
 * GenericUDF Class for operation Not EQUAL.
 */
@Description(name = "<>", value = "a _FUNC_ b - Returns TRUE if a is not equal to b")
@VectorizedExpressions({LongColNotEqualLongColumn.class, LongColNotEqualDoubleColumn.class,
  DoubleColNotEqualLongColumn.class, DoubleColNotEqualDoubleColumn.class,
  LongColNotEqualLongScalar.class, LongColNotEqualDoubleScalar.class,
  DoubleColNotEqualLongScalar.class, DoubleColNotEqualDoubleScalar.class,
  LongScalarNotEqualLongColumn.class, LongScalarNotEqualDoubleColumn.class,
  DoubleScalarNotEqualLongColumn.class, DoubleScalarNotEqualDoubleColumn.class,

  DecimalColNotEqualDecimalColumn.class, DecimalColNotEqualDecimalScalar.class,
  DecimalScalarNotEqualDecimalColumn.class,
  Decimal64ColNotEqualDecimal64Column.class, Decimal64ColNotEqualDecimal64Scalar.class,
  Decimal64ScalarNotEqualDecimal64Column.class,

  StringGroupColNotEqualStringGroupColumn.class, FilterStringGroupColNotEqualStringGroupColumn.class,
  StringGroupColNotEqualStringScalar.class,
  StringGroupColNotEqualVarCharScalar.class, StringGroupColNotEqualCharScalar.class,
  StringScalarNotEqualStringGroupColumn.class,
  VarCharScalarNotEqualStringGroupColumn.class, CharScalarNotEqualStringGroupColumn.class,

  FilterStringGroupColNotEqualStringScalar.class, FilterStringScalarNotEqualStringGroupColumn.class,
  FilterStringGroupColNotEqualVarCharScalar.class, FilterVarCharScalarNotEqualStringGroupColumn.class,
  FilterStringGroupColNotEqualCharScalar.class, FilterCharScalarNotEqualStringGroupColumn.class,

  FilterLongColNotEqualLongColumn.class, FilterLongColNotEqualDoubleColumn.class,
  FilterDoubleColNotEqualLongColumn.class, FilterDoubleColNotEqualDoubleColumn.class,
  FilterLongColNotEqualLongScalar.class, FilterLongColNotEqualDoubleScalar.class,
  FilterDoubleColNotEqualLongScalar.class, FilterDoubleColNotEqualDoubleScalar.class,
  FilterLongScalarNotEqualLongColumn.class, FilterLongScalarNotEqualDoubleColumn.class,
  FilterDoubleScalarNotEqualLongColumn.class, FilterDoubleScalarNotEqualDoubleColumn.class,

  FilterDecimalColNotEqualDecimalColumn.class, FilterDecimalColNotEqualDecimalScalar.class,
  FilterDecimalScalarNotEqualDecimalColumn.class,

  FilterDecimal64ColNotEqualDecimal64Column.class, FilterDecimal64ColNotEqualDecimal64Scalar.class,
  FilterDecimal64ScalarNotEqualDecimal64Column.class,

  TimestampColNotEqualTimestampColumn.class,
  TimestampColNotEqualTimestampScalar.class, TimestampScalarNotEqualTimestampColumn.class,
  TimestampColNotEqualLongColumn.class,
  TimestampColNotEqualLongScalar.class, TimestampScalarNotEqualLongColumn.class,
  TimestampColNotEqualDoubleColumn.class,
  TimestampColNotEqualDoubleScalar.class, TimestampScalarNotEqualDoubleColumn.class,
  LongColNotEqualTimestampColumn.class,
  LongColNotEqualTimestampScalar.class, LongScalarNotEqualTimestampColumn.class,
  DoubleColNotEqualTimestampColumn.class,
  DoubleColNotEqualTimestampScalar.class, DoubleScalarNotEqualTimestampColumn.class,

  FilterTimestampColNotEqualTimestampColumn.class,
  FilterTimestampColNotEqualTimestampScalar.class, FilterTimestampScalarNotEqualTimestampColumn.class,
  FilterTimestampColNotEqualLongColumn.class,
  FilterTimestampColNotEqualLongScalar.class, FilterTimestampScalarNotEqualLongColumn.class,
  FilterTimestampColNotEqualDoubleColumn.class,
  FilterTimestampColNotEqualDoubleScalar.class, FilterTimestampScalarNotEqualDoubleColumn.class,
  FilterLongColNotEqualTimestampColumn.class,
  FilterLongColNotEqualTimestampScalar.class, FilterLongScalarNotEqualTimestampColumn.class,
  FilterDoubleColNotEqualTimestampColumn.class,
  FilterDoubleColNotEqualTimestampScalar.class, FilterDoubleScalarNotEqualTimestampColumn.class,

  IntervalYearMonthScalarNotEqualIntervalYearMonthColumn.class, FilterIntervalYearMonthScalarNotEqualIntervalYearMonthColumn.class,
  IntervalYearMonthColNotEqualIntervalYearMonthScalar.class, FilterIntervalYearMonthColNotEqualIntervalYearMonthScalar.class,

  IntervalDayTimeColNotEqualIntervalDayTimeColumn.class, FilterIntervalDayTimeColNotEqualIntervalDayTimeColumn.class,
  IntervalDayTimeScalarNotEqualIntervalDayTimeColumn.class, FilterIntervalDayTimeScalarNotEqualIntervalDayTimeColumn.class,
  IntervalDayTimeColNotEqualIntervalDayTimeScalar.class, FilterIntervalDayTimeColNotEqualIntervalDayTimeScalar.class,

  DateColNotEqualDateScalar.class,FilterDateColNotEqualDateScalar.class,
  DateScalarNotEqualDateColumn.class,FilterDateScalarNotEqualDateColumn.class,
  })
@VectorizedExpressionsSupportDecimal64()
@NDV(maxNdv = 2)
public class GenericUDFOPNotEqual extends GenericUDFBaseCompare {
  public GenericUDFOPNotEqual(){
    this.opName = "NOT EQUAL";
    this.opDisplayName = "<>";
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
      result.set(!soi0.getPrimitiveWritableObject(o0).equals(
          soi1.getPrimitiveWritableObject(o1)));
      break;
    case COMPARE_INT:
      result.set(ioi0.get(o0) != ioi1.get(o1));
      break;
    case COMPARE_LONG:
      result.set(loi0.get(o0) != loi1.get(o1));
      break;
    case COMPARE_BYTE:
      result.set(byoi0.get(o0) != byoi1.get(o1));
      break;
    case COMPARE_BOOL:
      result.set(boi0.get(o0) != boi1.get(o1));
      break;
    case COMPARE_STRING:
      result.set(!soi0.getPrimitiveJavaObject(o0).equals(
          soi1.getPrimitiveJavaObject(o1)));
      break;
    case SAME_TYPE:
      result.set(ObjectInspectorUtils.compare(
          o0, argumentOIs[0], o1, argumentOIs[1]) != 0);
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
          converted_o1, compareOI) != 0);
    }
    return result;
  }

  @Override
  protected boolean supportsCategory(ObjectInspector.Category c) {
    return super.supportsCategory(c) ||
        c == ObjectInspector.Category.MAP ||
        c == ObjectInspector.Category.STRUCT ||
        c == ObjectInspector.Category.LIST;
  }

  @Override
  public GenericUDF negative() {
    return new GenericUDFOPEqual();
  }
}
