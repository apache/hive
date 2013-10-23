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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastDoubleToTimestampViaDoubleToLong;
import org.apache.hadoop.hive.ql.exec.vector.expressions.gen.CastLongToTimestampViaLongToLong;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TimestampConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 *
 * GenericUDFTimestamp
 *
 * Example usage:
 * ... CAST(<Timestamp string> as TIMESTAMP) ...
 *
 * Creates a TimestampWritable object using PrimitiveObjectInspectorConverter
 *
 */
@VectorizedExpressions({CastLongToTimestampViaLongToLong.class,
  CastDoubleToTimestampViaDoubleToLong.class})
public class GenericUDFTimestamp extends GenericUDF {

  private transient PrimitiveObjectInspector argumentOI;
  private transient TimestampConverter tc;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1) {
      throw new UDFArgumentLengthException(
          "The function TIMESTAMP requires at least one argument, got "
          + arguments.length);
    }
    try {
      argumentOI = (PrimitiveObjectInspector) arguments[0];
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
          "The function TIMESTAMP takes only primitive types");
    }

    tc = new TimestampConverter(argumentOI,
        PrimitiveObjectInspectorFactory.writableTimestampObjectInspector);
    return PrimitiveObjectInspectorFactory.writableTimestampObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o0 = arguments[0].get();
    if (o0 == null) {
      return null;
    }

    return tc.convert(o0);
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    StringBuilder sb = new StringBuilder();
    sb.append("CAST( ");
    sb.append(children[0]);
    sb.append(" AS TIMESTAMP)");
    return sb.toString();
  }

}
