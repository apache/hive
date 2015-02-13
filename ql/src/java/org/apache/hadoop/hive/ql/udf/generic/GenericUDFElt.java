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
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

/**
 * Generic UDF for string function <code>ELT(N,str1,str2,str3,...)</code>. This
 * mimics the function from MySQL
 * http://dev.mysql.com/doc/refman/5.1/en/string-functions.html#function_elt
 * 
 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF
 */
@Description(name = "elt",
    value = "_FUNC_(n, str1, str2, ...) - returns the n-th string",
    extended = "Example:\n"
    + "  > SELECT _FUNC_(1, 'face', 'book') FROM src LIMIT 1;\n" + "  'face'")
public class GenericUDFElt extends GenericUDF {
  private transient ObjectInspectorConverters.Converter[] converters;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(
          "The function ELT(N,str1,str2,str3,...) needs at least two arguments.");
    }

    for (int i = 0; i < arguments.length; i++) {
      Category category = arguments[i].getCategory();
      if (category != Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i, "The "
            + GenericUDFUtils.getOrdinal(i + 1)
            + " argument of function ELT is expected to a "
            + Category.PRIMITIVE.toString().toLowerCase() + " type, but "
            + category.toString().toLowerCase() + " is found");
      }
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      if (i == 0) {
        converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
            PrimitiveObjectInspectorFactory.writableIntObjectInspector);
      } else {
        converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
            PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      }
    }

    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    IntWritable intWritable = (IntWritable) converters[0].convert(arguments[0]
        .get());
    if (intWritable == null) {
      return null;
    }
    int index = intWritable.get();
    if (index <= 0 || index >= arguments.length) {
      return null;
    }
    return converters[index].convert(arguments[index].get());
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length >= 2);
    return getStandardDisplayString("elt", children);
  }
}
