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

package org.apache.hadoop.hive.ql.udf;

import com.github.f4b6a3.uuid.alt.GUID;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * UDF UUIDv7.
 *
 */
@Description(name = "uuid_v7",
        value = "_FUNC_() - Returns a universally unique identifier (UUID_V7) string.",
        extended = """
                The value is returned as a canonical UUIDv7 36-character string.
                Example:
                  > SELECT _FUNC_();
                  '0197b082-4dc8-770e-9a75-bd601dff26a1'
                  > SELECT _FUNC_();
                  '0197b082-bc45-745f-adb7-43df40797b0b'""")
@UDFType(deterministic = false)
public class UDFUUIDv7 extends GenericUDF {
  private final Text result = new Text();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if(arguments != null) {
      checkArgsSize(arguments, 0, 0);
    }
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    result.set(GUID.v7().toString());
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    return "uuid_v7";
  }
}
