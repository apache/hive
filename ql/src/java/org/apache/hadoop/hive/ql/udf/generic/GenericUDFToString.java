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
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TextConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@Description(name = "string",
value = "CAST(<value> as STRING) - Converts the argument to a string value.",
extended = "Example:\n "
+ "  > SELECT CAST(1234 AS string) FROM src LIMIT 1;\n"
+ "  '1234'")
public class GenericUDFToString extends GenericUDF {
  private static final Logger LOG = LoggerFactory.getLogger(GenericUDFToString.class.getName());

  private transient PrimitiveObjectInspector argumentOI;
  private transient TextConverter converter;

  public GenericUDFToString() {
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("STRING cast requires a value argument");
    }
    try {
      argumentOI = (PrimitiveObjectInspector) arguments[0];
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
          "The function STRING takes only primitive types");
    }

    converter = new TextConverter(argumentOI);
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
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
    StringBuilder sb = new StringBuilder();
    sb.append("CAST( ");
    sb.append(children[0]);
    sb.append(" AS STRING)");
    return sb.toString();
  }
}
