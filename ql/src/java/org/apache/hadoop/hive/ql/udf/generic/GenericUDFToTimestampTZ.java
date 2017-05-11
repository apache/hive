/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.udf.generic;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * Convert from string to TIMESTAMP WITH TIME ZONE.
 */
@Description(name = "timestamp with time zone",
    value = "CAST(STRING as TIMESTAMP WITH TIME ZONE) - returns the" +
        "timestamp with time zone represented by string.",
    extended = "The string should be of format 'yyyy-MM-dd HH:mm:ss[.SSS...] ZoneId/ZoneOffset'. " +
        "Examples of ZoneId and ZoneOffset are Asia/Shanghai and GMT+08:00. " +
        "The time and zone parts are optional. If time is absent, '00:00:00.0' will be used. " +
        "If zone is absent, the system time zone will be used.")
public class GenericUDFToTimestampTZ extends GenericUDF {

  private transient PrimitiveObjectInspector argumentOI;
  private transient PrimitiveObjectInspectorConverter.TimestampTZConverter converter;


  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1) {
      throw new UDFArgumentLengthException(
          "The function CAST as TIMESTAMP WITH TIME ZONE requires at least one argument, got "
              + arguments.length);
    }
    try {
      argumentOI = (PrimitiveObjectInspector) arguments[0];
      switch (argumentOI.getPrimitiveCategory()) {
      case CHAR:
      case VARCHAR:
      case STRING:
      case DATE:
      case TIMESTAMP:
      case TIMESTAMPTZ:
        break;
      default:
        throw new UDFArgumentException("CAST as TIMESTAMP WITH TIME ZONE only allows" +
            "string/date/timestamp/timestamp with time zone types");
      }
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
          "The function CAST as TIMESTAMP WITH TIME ZONE takes only primitive types");
    }
    converter = new PrimitiveObjectInspectorConverter.TimestampTZConverter(argumentOI,
        PrimitiveObjectInspectorFactory.writableTimestampTZObjectInspector);
    return PrimitiveObjectInspectorFactory.writableTimestampTZObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o0 = arguments[0].get();
    if (o0 == null) {
      return null;
    }
    return converter.convert(o0);
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return "CAST(" + children[0] + " AS TIMESTAMP WITH TIME ZONE)";
  }
}
