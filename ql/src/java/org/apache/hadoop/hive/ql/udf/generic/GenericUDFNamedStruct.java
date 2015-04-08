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

import java.util.ArrayList;

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

@Description(name = "named_struct",
    value = "_FUNC_(name1, val1, name2, val2, ...) - Creates a struct with the given " +
            "field names and values")
public class GenericUDFNamedStruct extends GenericUDF {
  private transient Object[] ret;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {

    int numFields = arguments.length;
    if (numFields % 2 == 1) {
      throw new UDFArgumentLengthException(
          "NAMED_STRUCT expects an even number of arguments.");
    }
    ret = new Object[numFields / 2];

    ArrayList<String> fname = new ArrayList<String>(numFields / 2);
    ArrayList<ObjectInspector> retOIs = new ArrayList<ObjectInspector>(numFields / 2);
    for (int f = 0; f < numFields; f+=2) {
      if (!(arguments[f] instanceof ConstantObjectInspector)) {
        throw new UDFArgumentTypeException(f, "Even arguments" +
            " to NAMED_STRUCT must be a constant STRING." + arguments[f].toString());
      }
      ConstantObjectInspector constantOI =
        (ConstantObjectInspector)arguments[f];
      fname.add(constantOI.getWritableConstantValue().toString());
      retOIs.add(arguments[f + 1]);
    }
    StructObjectInspector soi =
      ObjectInspectorFactory.getStandardStructObjectInspector(fname, retOIs);
    return soi;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    for (int i = 0; i < arguments.length / 2; i++) {
      ret[i] = arguments[2 * i + 1].get();
    }
    return ret;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("named_struct", children, ",");
  }
}
