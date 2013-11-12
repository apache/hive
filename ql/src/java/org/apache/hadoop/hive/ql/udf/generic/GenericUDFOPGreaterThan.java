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
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColGreaterDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColGreaterDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColGreaterLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleColGreaterLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarGreaterDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.DoubleScalarGreaterLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColGreaterDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColGreaterDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColGreaterLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleColGreaterLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleScalarGreaterDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterDoubleScalarGreaterLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongColGreaterLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongScalarGreaterDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterLongScalarGreaterLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColGreaterStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringColGreaterStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.FilterStringScalarGreaterStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColGreaterDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColGreaterDoubleScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColGreaterLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongColGreaterLongScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarGreaterDoubleColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.LongScalarGreaterLongColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringColGreaterStringColumn;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringColGreaterStringScalar;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.StringScalarGreaterStringColumn;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.shims.ShimLoader;
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
  StringColGreaterStringColumn.class, StringColGreaterStringScalar.class,
  StringScalarGreaterStringColumn.class, FilterStringColGreaterStringColumn.class,
  FilterStringColGreaterStringScalar.class, FilterStringScalarGreaterStringColumn.class,
  FilterLongColGreaterLongColumn.class, FilterLongColGreaterDoubleColumn.class,
  FilterDoubleColGreaterLongColumn.class, FilterDoubleColGreaterDoubleColumn.class,
  FilterLongColGreaterLongScalar.class, FilterLongColGreaterDoubleScalar.class,
  FilterDoubleColGreaterLongScalar.class, FilterDoubleColGreaterDoubleScalar.class,
  FilterLongScalarGreaterLongColumn.class, FilterLongScalarGreaterDoubleColumn.class,
  FilterDoubleScalarGreaterLongColumn.class, FilterDoubleScalarGreaterDoubleColumn.class})
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

}

