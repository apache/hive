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
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColNotEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColNotEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColNotEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColNotEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarNotEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarNotEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColNotEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColNotEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColNotEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColNotEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleScalarNotEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleScalarNotEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColNotEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColNotEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColNotEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColNotEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongScalarNotEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongScalarNotEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColNotEqualStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColNotEqualStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringScalarNotEqualStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColNotEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColNotEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColNotEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColNotEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarNotEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarNotEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringColNotEqualStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringColNotEqualStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringScalarNotEqualStringColumn;
import org.apache.hadoop.hive.ql.metadata.HiveException;
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
  StringColNotEqualStringColumn.class, StringColNotEqualStringScalar.class,
  StringScalarNotEqualStringColumn.class, FilterStringColNotEqualStringColumn.class,
  FilterStringColNotEqualStringScalar.class, FilterStringScalarNotEqualStringColumn.class,
  FilterLongColNotEqualLongColumn.class, FilterLongColNotEqualDoubleColumn.class,
  FilterDoubleColNotEqualLongColumn.class, FilterDoubleColNotEqualDoubleColumn.class,
  FilterLongColNotEqualLongScalar.class, FilterLongColNotEqualDoubleScalar.class,
  FilterDoubleColNotEqualLongScalar.class, FilterDoubleColNotEqualDoubleScalar.class,
  FilterLongScalarNotEqualLongColumn.class, FilterLongScalarNotEqualDoubleColumn.class,
  FilterDoubleScalarNotEqualLongColumn.class, FilterDoubleScalarNotEqualDoubleColumn.class})
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
}
