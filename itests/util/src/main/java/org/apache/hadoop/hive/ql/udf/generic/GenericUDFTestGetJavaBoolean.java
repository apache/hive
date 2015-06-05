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
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;

/**
 * A test GenericUDF to return native Java's boolean type
 */
@Description(name = "test_udf_get_java_boolean",
value = "_FUNC_(str) - GenericUDF to return native Java's boolean type")
public class GenericUDFTestGetJavaBoolean extends GenericUDF {
  ObjectInspector[] argumentOIs;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    argumentOIs = arguments;
    return PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object input = ((PrimitiveObjectInspector) argumentOIs[0]).getPrimitiveJavaObject(arguments[0].get());
    if ("true".equalsIgnoreCase(input.toString())) {
      return Boolean.TRUE;
    } else if ("false".equalsIgnoreCase(input.toString())) {
      return false;
    } else {
      return null;
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return getStandardDisplayString("TestGetJavaBoolean", children);
  }
}
