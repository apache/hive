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
 * GenericUDF Class for operation EQUAL.
 */
@Description(name = "=", value = "a _FUNC_ b - Returns TRUE if a equals b and false otherwise")
@VectorizedExpressions({LongColEqualLongColumn.class, LongColEqualDoubleColumn.class,
  DoubleColEqualLongColumn.class, DoubleColEqualDoubleColumn.class,
  LongColEqualLongScalar.class, LongColEqualDoubleScalar.class,
  DoubleColEqualLongScalar.class, DoubleColEqualDoubleScalar.class,
  LongScalarEqualLongColumn.class, LongScalarEqualDoubleColumn.class,
  DoubleScalarEqualLongColumn.class, DoubleScalarEqualDoubleColumn.class,

  DecimalColEqualDecimalColumn.class, DecimalColEqualDecimalScalar.class,
  DecimalScalarEqualDecimalColumn.class,
  Decimal64ColEqualDecimal64Column.class, Decimal64ColEqualDecimal64Scalar.class,
  Decimal64ScalarEqualDecimal64Column.class,

  StringGroupColEqualStringGroupColumn.class, FilterStringGroupColEqualStringGroupColumn.class,
  StringGroupColEqualStringScalar.class,
  StringGroupColEqualVarCharScalar.class, StringGroupColEqualCharScalar.class,
  StringScalarEqualStringGroupColumn.class,
  VarCharScalarEqualStringGroupColumn.class, CharScalarEqualStringGroupColumn.class,

  FilterStringGroupColEqualStringScalar.class, FilterStringScalarEqualStringGroupColumn.class,
  FilterStringGroupColEqualVarCharScalar.class, FilterVarCharScalarEqualStringGroupColumn.class,
  FilterStringGroupColEqualCharScalar.class, FilterCharScalarEqualStringGroupColumn.class,

  FilterLongColEqualLongColumn.class, FilterLongColEqualDoubleColumn.class,
  FilterDoubleColEqualLongColumn.class, FilterDoubleColEqualDoubleColumn.class,
  FilterLongColEqualLongScalar.class, FilterLongColEqualDoubleScalar.class,
  FilterDoubleColEqualLongScalar.class, FilterDoubleColEqualDoubleScalar.class,
  FilterLongScalarEqualLongColumn.class, FilterLongScalarEqualDoubleColumn.class,
  FilterDoubleScalarEqualLongColumn.class, FilterDoubleScalarEqualDoubleColumn.class,

  FilterDecimalColEqualDecimalColumn.class, FilterDecimalColEqualDecimalScalar.class,
  FilterDecimalScalarEqualDecimalColumn.class,

  FilterDecimal64ColEqualDecimal64Column.class, FilterDecimal64ColEqualDecimal64Scalar.class,
  FilterDecimal64ScalarEqualDecimal64Column.class,

  TimestampColEqualTimestampColumn.class,
  TimestampColEqualTimestampScalar.class, TimestampScalarEqualTimestampColumn.class,
  TimestampColEqualLongColumn.class,
  TimestampColEqualLongScalar.class, TimestampScalarEqualLongColumn.class,
  TimestampColEqualDoubleColumn.class,
  TimestampColEqualDoubleScalar.class, TimestampScalarEqualDoubleColumn.class,
  LongColEqualTimestampColumn.class,
  LongColEqualTimestampScalar.class, LongScalarEqualTimestampColumn.class,
  DoubleColEqualTimestampColumn.class,
  DoubleColEqualTimestampScalar.class, DoubleScalarEqualTimestampColumn.class,

  FilterTimestampColEqualTimestampColumn.class,
  FilterTimestampColEqualTimestampScalar.class, FilterTimestampScalarEqualTimestampColumn.class,
  FilterTimestampColEqualLongColumn.class,
  FilterTimestampColEqualLongScalar.class, FilterTimestampScalarEqualLongColumn.class,
  FilterTimestampColEqualDoubleColumn.class,
  FilterTimestampColEqualDoubleScalar.class, FilterTimestampScalarEqualDoubleColumn.class,
  FilterLongColEqualTimestampColumn.class,
  FilterLongColEqualTimestampScalar.class, FilterLongScalarEqualTimestampColumn.class,
  FilterDoubleColEqualTimestampColumn.class,
  FilterDoubleColEqualTimestampScalar.class, FilterDoubleScalarEqualTimestampColumn.class,

  IntervalYearMonthScalarEqualIntervalYearMonthColumn.class, FilterIntervalYearMonthScalarEqualIntervalYearMonthColumn.class,
  IntervalYearMonthColEqualIntervalYearMonthScalar.class, FilterIntervalYearMonthColEqualIntervalYearMonthScalar.class,

  IntervalDayTimeColEqualIntervalDayTimeColumn.class, FilterIntervalDayTimeColEqualIntervalDayTimeColumn.class,
  IntervalDayTimeScalarEqualIntervalDayTimeColumn.class, FilterIntervalDayTimeScalarEqualIntervalDayTimeColumn.class,
  IntervalDayTimeColEqualIntervalDayTimeScalar.class, FilterIntervalDayTimeColEqualIntervalDayTimeScalar.class,

  DateColEqualDateScalar.class,FilterDateColEqualDateScalar.class,
  DateScalarEqualDateColumn.class,FilterDateScalarEqualDateColumn.class,
  })
@VectorizedExpressionsSupportDecimal64()
@NDV(maxNdv = 2)
public class GenericUDFOPEqual extends GenericUDFBaseCompare {
  public GenericUDFOPEqual(){
    this.opName = "EQUAL";
    this.opDisplayName = "=";
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
      result.set(soi0.getPrimitiveWritableObject(o0).equals(
          soi1.getPrimitiveWritableObject(o1)));
      break;
    case COMPARE_INT:
      result.set(ioi0.get(o0) == ioi1.get(o1));
      break;
    case COMPARE_LONG:
      result.set(loi0.get(o0) == loi1.get(o1));
      break;
    case COMPARE_BYTE:
      result.set(byoi0.get(o0) == byoi1.get(o1));
      break;
    case COMPARE_BOOL:
      result.set(boi0.get(o0) == boi1.get(o1));
      break;
    case COMPARE_STRING:
      result.set(soi0.getPrimitiveJavaObject(o0).equals(
          soi1.getPrimitiveJavaObject(o1)));
      break;
    case SAME_TYPE:
      result.set(ObjectInspectorUtils.compare(
          o0, argumentOIs[0], o1, argumentOIs[1]) == 0);
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
          converted_o1, compareOI) == 0);
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
      return new GenericUDFOPNotEqual();
  }
}
