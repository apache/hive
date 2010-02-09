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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

/**
 * GenericUDFSize.
 *
 */
@Description(name = "size", value = "_FUNC_(a) - Returns the size of a")
public class GenericUDFSize extends GenericUDF {
  private ObjectInspector returnOI;
  private final IntWritable result = new IntWritable(-1);

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException(
          "The function SIZE only accepts 1 argument.");
    }
    Category category = arguments[0].getCategory();
    String typeName = arguments[0].getTypeName();
    if (category != Category.MAP && category != Category.LIST
        && !typeName.equals(Constants.VOID_TYPE_NAME)) {
      throw new UDFArgumentTypeException(0, "\""
          + Category.MAP.toString().toLowerCase() + "\" or \""
          + Category.LIST.toString().toLowerCase()
          + "\" is expected at function SIZE, " + "but \""
          + arguments[0].getTypeName() + "\" is found");
    }

    returnOI = arguments[0];
    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object data = arguments[0].get();
    if (returnOI.getCategory() == Category.MAP) {
      result.set(((MapObjectInspector) returnOI).getMapSize(data));
    } else if (returnOI.getCategory() == Category.LIST) {
      result.set(((ListObjectInspector) returnOI).getListLength(data));
    } else if (returnOI.getTypeName().equals(Constants.VOID_TYPE_NAME)) {
      // null
      result.set(-1);
    }
    return result;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    return "size(" + children[0] + ")";
  }
}
