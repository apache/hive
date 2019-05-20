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

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

@Description(name = "istrue", value = "_FUNC_ a - Returns true if a is TRUE and false otherwise")
@NDV(maxNdv = 2)
public class GenericUDFOPTrue extends GenericUDF {
  private final BooleanWritable result = new BooleanWritable();
  private Converter conditionConverter;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException("Invalid number of arguments");
    }
    conditionConverter = ObjectInspectorConverters.getConverter(arguments[0],
        PrimitiveObjectInspectorFactory.writableBooleanObjectInspector);

    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    BooleanWritable condition = (BooleanWritable) conditionConverter.convert(arguments[0].get());
    result.set(condition != null && condition.get());
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return children[0] + " is true";
  }

  @Override
  public GenericUDF negative() {
    return new GenericUDFOPNotTrue();
  }
}
