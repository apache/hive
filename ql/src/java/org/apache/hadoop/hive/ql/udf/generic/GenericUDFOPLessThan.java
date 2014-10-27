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
 * GenericUDF Class for operation LessThan.
 */
@Description(name = "<", value = "a _FUNC_ b - Returns TRUE if a is less than b")
@VectorizedExpressions({LongColLessLongColumn.class, LongColLessDoubleColumn.class,
    DoubleColLessLongColumn.class, DoubleColLessDoubleColumn.class,
    LongColLessLongScalar.class, LongColLessDoubleScalar.class,
    DoubleColLessLongScalar.class, DoubleColLessDoubleScalar.class,
    LongScalarLessLongColumn.class, LongScalarLessDoubleColumn.class,
    DoubleScalarLessLongColumn.class, DoubleScalarLessDoubleColumn.class,
    StringGroupColLessStringGroupColumn.class, FilterStringGroupColLessStringGroupColumn.class,
    StringGroupColLessStringScalar.class,
    StringGroupColLessVarCharScalar.class, StringGroupColLessCharScalar.class,
    StringScalarLessStringGroupColumn.class,
    VarCharScalarLessStringGroupColumn.class, CharScalarLessStringGroupColumn.class,
    FilterStringGroupColLessStringScalar.class, FilterStringScalarLessStringGroupColumn.class,
    FilterStringGroupColLessVarCharScalar.class, FilterVarCharScalarLessStringGroupColumn.class,
    FilterStringGroupColLessCharScalar.class, FilterCharScalarLessStringGroupColumn.class,
    FilterLongColLessLongColumn.class, FilterLongColLessDoubleColumn.class,
    FilterDoubleColLessLongColumn.class, FilterDoubleColLessDoubleColumn.class,
    FilterLongColLessLongScalar.class, FilterLongColLessDoubleScalar.class,
    FilterDoubleColLessLongScalar.class, FilterDoubleColLessDoubleScalar.class,
    FilterLongScalarLessLongColumn.class, FilterLongScalarLessDoubleColumn.class,
    FilterDoubleScalarLessLongColumn.class, FilterDoubleScalarLessDoubleColumn.class,
    FilterDecimalColLessDecimalColumn.class, FilterDecimalColLessDecimalScalar.class,
    FilterDecimalScalarLessDecimalColumn.class,
    TimestampColLessTimestampScalar.class, TimestampScalarLessTimestampColumn.class,
    FilterTimestampColLessTimestampScalar.class, FilterTimestampScalarLessTimestampColumn.class,
    TimestampColLessLongScalar.class, LongScalarLessTimestampColumn.class,
    FilterTimestampColLessLongScalar.class, FilterLongScalarLessTimestampColumn.class,
    TimestampColLessDoubleScalar.class, DoubleScalarLessTimestampColumn.class,
    FilterTimestampColLessDoubleScalar.class, FilterDoubleScalarLessTimestampColumn.class
    })
public class GenericUDFOPLessThan extends GenericUDFBaseCompare {
  public GenericUDFOPLessThan(){
    this.opName = "LESS THAN";
    this.opDisplayName = "<";
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
      result.set(t0.compareTo(t1) < 0);
      break;
    case COMPARE_INT:
      result.set(ioi0.get(o0) < ioi1.get(o1));
      break;
    case COMPARE_LONG:
      result.set(loi0.get(o0) < loi1.get(o1));
      break;
    case COMPARE_BYTE:
      result.set(byoi0.get(o0) < byoi1.get(o1));
      break;
    case COMPARE_BOOL:
      boolean b0 = boi0.get(o0);
      boolean b1 = boi1.get(o1);
      result.set(!b0 && b1);
      break;
    case COMPARE_STRING:
      String s0, s1;
      s0 = soi0.getPrimitiveJavaObject(o0);
      s1 = soi1.getPrimitiveJavaObject(o1);
      result.set(s0.compareTo(s1) < 0);
      break;
    case SAME_TYPE:
      result.set(ObjectInspectorUtils.compare(
          o0, argumentOIs[0], o1, argumentOIs[1]) < 0);
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
          converted_o1, compareOI) < 0);
    }
    return result;
  }

  @Override
  public GenericUDF flip() {
    return new GenericUDFOPGreaterThan();
  }

}
