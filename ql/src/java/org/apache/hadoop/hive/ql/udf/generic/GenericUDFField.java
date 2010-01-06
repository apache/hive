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

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.exec.description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

@description(
    name="field",
    value = "_FUNC_(str, str1, str2, ...) - returns the index of str in the str1,str2,... list or 0 if not found",
    extended = "All primitive types are supported, arguments are compared using str.equals(x)." +
    " If str is NULL, the return value is 0."
    )
public class GenericUDFField extends GenericUDF {
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentException("The function FIELD(str, str1, str2, ...) needs at least two arguments.");
    }
    
    for (int i = 0; i < arguments.length; i++) {
      Category category = arguments[i].getCategory();
      if (category != Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i,
            "The " + GenericUDFUtils.getOrdinal(i + 1) + " argument of function FIELD is expected to a " 
            + Category.PRIMITIVE.toString().toLowerCase()
            + " type, but " + category.toString().toLowerCase() + " is found");
      }
    }
    
    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }
  
  private IntWritable r = new IntWritable();

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      r.set(0);
      return r;
    }
    
    for (int i=1; i< arguments.length; i++) {
      if (arguments[0].get().equals(arguments[i].get())) {
        r.set(i);
        return r;
      }
    }
    
    r.set(0);
    return r;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert(children.length >= 2);
    
    final StringBuilder sb = new StringBuilder();
    sb.append("field(");
    sb.append(StringUtils.join(children, ", "));
    sb.append(")");
    
    return sb.toString();
  }
}
