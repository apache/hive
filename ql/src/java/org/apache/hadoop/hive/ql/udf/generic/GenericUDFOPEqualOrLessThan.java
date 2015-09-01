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

package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.*;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.io.Text;

/**
 * GenericUDF Class for operation EqualOrLessThan.
 */
@Description(name = "<=", value = "a _FUNC_ b - Returns TRUE if a is not greater than b")
@VectorizedExpressions({LongColLessEqualLongColumn.class, LongColLessEqualDoubleColumn.class,
  DoubleColLessEqualLongColumn.class, DoubleColLessEqualDoubleColumn.class,
  LongColLessEqualLongScalar.class, LongColLessEqualDoubleScalar.class,
  DoubleColLessEqualLongScalar.class, DoubleColLessEqualDoubleScalar.class,
  LongScalarLessEqualLongColumn.class, LongScalarLessEqualDoubleColumn.class,
  DoubleScalarLessEqualLongColumn.class, DoubleScalarLessEqualDoubleColumn.class,
  StringGroupColLessEqualStringGroupColumn.class, FilterStringGroupColLessEqualStringGroupColumn.class,
  StringGroupColLessEqualStringScalar.class,
  StringGroupColLessEqualVarCharScalar.class, StringGroupColLessEqualCharScalar.class,
  StringScalarLessEqualStringGroupColumn.class,
  VarCharScalarLessEqualStringGroupColumn.class, CharScalarLessEqualStringGroupColumn.class,
  FilterStringGroupColLessEqualStringScalar.class, FilterStringScalarLessEqualStringGroupColumn.class,
  FilterStringGroupColLessEqualVarCharScalar.class, FilterVarCharScalarLessEqualStringGroupColumn.class,
  FilterStringGroupColLessEqualCharScalar.class, FilterCharScalarLessEqualStringGroupColumn.class,
  FilterLongColLessEqualLongColumn.class, FilterLongColLessEqualDoubleColumn.class,
  FilterDoubleColLessEqualLongColumn.class, FilterDoubleColLessEqualDoubleColumn.class,
  FilterLongColLessEqualLongScalar.class, FilterLongColLessEqualDoubleScalar.class,
  FilterDoubleColLessEqualLongScalar.class, FilterDoubleColLessEqualDoubleScalar.class,
  FilterLongScalarLessEqualLongColumn.class, FilterLongScalarLessEqualDoubleColumn.class,
  FilterDoubleScalarLessEqualLongColumn.class, FilterDoubleScalarLessEqualDoubleColumn.class,
  FilterDecimalColLessEqualDecimalColumn.class, FilterDecimalColLessEqualDecimalScalar.class,
  FilterDecimalScalarLessEqualDecimalColumn.class,
  TimestampColLessEqualTimestampScalar.class, TimestampScalarLessEqualTimestampColumn.class,
  FilterTimestampColLessEqualTimestampScalar.class, FilterTimestampScalarLessEqualTimestampColumn.class,
  TimestampColLessEqualLongScalar.class, LongScalarLessEqualTimestampColumn.class,
  FilterTimestampColLessEqualLongScalar.class, FilterLongScalarLessEqualTimestampColumn.class,
  TimestampColLessEqualDoubleScalar.class, DoubleScalarLessEqualTimestampColumn.class,
  FilterTimestampColLessEqualDoubleScalar.class, FilterDoubleScalarLessEqualTimestampColumn.class,
  IntervalYearMonthScalarLessEqualIntervalYearMonthColumn.class, FilterIntervalYearMonthScalarLessEqualIntervalYearMonthColumn.class,
  IntervalYearMonthColLessEqualIntervalYearMonthScalar.class, FilterIntervalYearMonthColLessEqualIntervalYearMonthScalar.class,
  IntervalDayTimeScalarLessEqualIntervalDayTimeColumn.class, FilterIntervalDayTimeScalarLessEqualIntervalDayTimeColumn.class,
  IntervalDayTimeColLessEqualIntervalDayTimeScalar.class, FilterIntervalDayTimeColLessEqualIntervalDayTimeScalar.class,
  DateColLessEqualDateScalar.class,FilterDateColLessEqualDateScalar.class,
  DateScalarLessEqualDateColumn.class,FilterDateScalarLessEqualDateColumn.class,
  })
public class GenericUDFOPEqualOrLessThan extends GenericUDFBaseCompare {
  public GenericUDFOPEqualOrLessThan(){
    this.opName = "EQUAL OR LESS THAN";
    this.opDisplayName = "<=";
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
      result.set(t0.compareTo(t1) <= 0);
      break;
    case COMPARE_INT:
      result.set(ioi0.get(o0) <= ioi1.get(o1));
      break;
    case COMPARE_LONG:
      result.set(loi0.get(o0) <= loi1.get(o1));
      break;
    case COMPARE_BYTE:
      result.set(byoi0.get(o0) <= byoi1.get(o1));
      break;
    case COMPARE_BOOL:
      boolean b0 = boi0.get(o0);
      boolean b1 = boi1.get(o1);
      result.set(!b0 || b1);
      break;
    case COMPARE_STRING:
      String s0, s1;
      s0 = soi0.getPrimitiveJavaObject(o0);
      s1 = soi1.getPrimitiveJavaObject(o1);
      result.set(s0.compareTo(s1) <= 0);
      break;
    case SAME_TYPE:
      result.set(ObjectInspectorUtils.compare(
          o0, argumentOIs[0], o1, argumentOIs[1]) <= 0);
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
          converted_o1, compareOI) <= 0);
    }
    return result;
  }

  @Override
  public GenericUDF flip() {
    return new GenericUDFOPEqualOrGreaterThan();
  }

  @Override
  public GenericUDF negative() {
    return new GenericUDFOPGreaterThan();
  }
}

