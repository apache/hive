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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampLocalTZObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.io.LongWritable;

/**
 * GenericUDFEpochMilli.
 */
public class GenericUDFEpochMilli extends GenericUDF {

  private transient final LongWritable result = new LongWritable();
  private transient TimestampLocalTZObjectInspector tsWithLocalTzOi = null;
  private transient TimestampObjectInspector tsOi = null;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException(
          "The operator GenericUDFEpochMilli only accepts 1 argument.");
    }
    if (arguments[0] instanceof TimestampObjectInspector) {
      tsOi = (TimestampObjectInspector) arguments[0];
    } else {
      tsWithLocalTzOi = (TimestampLocalTZObjectInspector) arguments[0];
    }
    return PrimitiveObjectInspectorFactory.writableLongObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object a0 = arguments[0].get();
    if (a0 == null) {
      return null;
    }

    result.set(tsOi == null ?
        tsWithLocalTzOi.getPrimitiveJavaObject(a0).getZonedDateTime().toInstant().toEpochMilli() :
        tsOi.getPrimitiveJavaObject(a0).toEpochMilli());
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return "GenericUDFEpochMilli(" + children[0] + ")";
  }

}
