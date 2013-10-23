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
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleScalarEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleScalarEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongScalarEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongScalarEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColEqualStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColEqualStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringScalarEqualStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringColEqualStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringColEqualStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringScalarEqualStringColumn;
import org.apache.hadoop.hive.ql.metadata.HiveException;
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
  StringColEqualStringColumn.class, StringColEqualStringScalar.class,
  StringScalarEqualStringColumn.class, FilterStringColEqualStringColumn.class,
  FilterStringColEqualStringScalar.class, FilterStringScalarEqualStringColumn.class,
  FilterLongColEqualLongColumn.class, FilterLongColEqualDoubleColumn.class,
  FilterDoubleColEqualLongColumn.class, FilterDoubleColEqualDoubleColumn.class,
  FilterLongColEqualLongScalar.class, FilterLongColEqualDoubleScalar.class,
  FilterDoubleColEqualLongScalar.class, FilterDoubleColEqualDoubleScalar.class,
  FilterLongScalarEqualLongColumn.class, FilterLongScalarEqualDoubleColumn.class,
  FilterDoubleScalarEqualLongColumn.class, FilterDoubleScalarEqualDoubleColumn.class})
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

}
