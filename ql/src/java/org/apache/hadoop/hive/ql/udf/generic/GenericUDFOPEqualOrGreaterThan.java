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
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColGreaterEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColGreaterEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColGreaterEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColGreaterEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarGreaterEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarGreaterEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColGreaterEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColGreaterEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColGreaterEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColGreaterEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleScalarGreaterEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleScalarGreaterEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongScalarGreaterEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongScalarGreaterEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColGreaterEqualStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColGreaterEqualStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringScalarGreaterEqualStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColGreaterEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColGreaterEqualDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColGreaterEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColGreaterEqualLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarGreaterEqualDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarGreaterEqualLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringColGreaterEqualStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringColGreaterEqualStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringScalarGreaterEqualStringColumn;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
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
  StringColGreaterEqualStringColumn.class, StringColGreaterEqualStringScalar.class,
  StringScalarGreaterEqualStringColumn.class, FilterStringColGreaterEqualStringColumn.class,
  FilterStringColGreaterEqualStringScalar.class, FilterStringScalarGreaterEqualStringColumn.class,
  FilterLongColGreaterEqualLongColumn.class, FilterLongColGreaterEqualDoubleColumn.class,
  FilterDoubleColGreaterEqualLongColumn.class, FilterDoubleColGreaterEqualDoubleColumn.class,
  FilterLongColGreaterEqualLongScalar.class, FilterLongColGreaterEqualDoubleScalar.class,
  FilterDoubleColGreaterEqualLongScalar.class, FilterDoubleColGreaterEqualDoubleScalar.class,
  FilterLongScalarGreaterEqualLongColumn.class, FilterLongScalarGreaterEqualDoubleColumn.class,
  FilterDoubleScalarGreaterEqualLongColumn.class, FilterDoubleScalarGreaterEqualDoubleColumn.class})
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

}
