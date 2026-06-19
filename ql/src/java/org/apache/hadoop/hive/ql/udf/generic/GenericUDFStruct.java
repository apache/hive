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

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;

@Description(name = "struct",
    value = "_FUNC_(col1, col2, col3, ...) - Creates a struct with the given field values")
public class GenericUDFStruct extends GenericUDF {
  private transient Object[] ret;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    
    int numFields = arguments.length;
    ret = new Object[numFields];
    
    ArrayList<String> fname = new ArrayList<String>(numFields);
    for (int f = 1; f <= numFields; f++) {
      fname.add("col" + f);
    }
    boolean constantStruct = true;
    for (int i = 0; i < arguments.length; i++) {
      ObjectInspector oi = arguments[i];
      constantStruct &= (oi.getCategory() == Category.PRIMITIVE)
          && (oi instanceof ConstantObjectInspector);
      if (constantStruct) {
        // nested complex types trigger Kryo issue #216 in plan deserialization
        ret[i] = ((ConstantObjectInspector) oi).getWritableConstantValue();
      }
    }
    if (constantStruct) {
      return ObjectInspectorFactory.getStandardConstantStructObjectInspector(fname,
          Arrays.asList(arguments), Arrays.asList(ret));
    } else {
      return ObjectInspectorFactory.getStandardStructObjectInspector(fname,
          Arrays.asList(arguments));
    }
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    for (int i = 0; i < arguments.length; i++) {
      ret[i] = arguments[i].get();
    }
    return ret;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("struct", children, ",");
  }
}
