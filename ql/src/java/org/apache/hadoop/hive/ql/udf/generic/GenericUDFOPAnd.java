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
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.BooleanWritable;

/**
 * GenericUDF Class for computing and.
 */
@Description(name = "and", value = "a _FUNC_ b - Logical and")
public class GenericUDFOPAnd extends GenericUDF {
  private final BooleanWritable result = new BooleanWritable();
  BooleanObjectInspector boi0,boi1;
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
          "The operator 'AND' only accepts 2 argument.");
    }
    boi0 = (BooleanObjectInspector) arguments[0];
    boi1 = (BooleanObjectInspector) arguments[1];
    return PrimitiveObjectInspectorFactory.writableBooleanObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    boolean bool_a0 = false, bool_a1 = false;
    BooleanWritable a0 = (BooleanWritable) arguments[0].get();
    if (a0 != null) {
      bool_a0 = boi0.get(a0);
      if (bool_a0 == false) {
        result.set(false);
        return result;
      }
    }

    BooleanWritable a1 = (BooleanWritable) arguments[1].get();
    if (a1 != null) {
      bool_a1 = boi1.get(a1);
      if (bool_a1 == false) {
        result.set(false);
        return result;
      }
    }

    if ((a0 != null && bool_a0 == true) && (a1 != null && bool_a1 == true)) {
      result.set(true);
      return result;
    }

    return null;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return "(" + children[0] + " and " + children[1] + ")";
  }

}
