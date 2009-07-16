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

import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Generic UDF for string function <code>LOCATE(substr, str)</code>, <code>LOCATE(substr, str, start)</code>.
 * This mimcs the function from MySQL
 * http://dev.mysql.com/doc/refman/5.1/en/string-functions.html#function_locate
 * <pre> 
 * usage:
 * LOCATE(substr, str)
 * LOCATE(substr, str, start)
 * </pre><p>
 */
public class GenericUDFLocate extends GenericUDF{

  ObjectInspectorConverters.Converter[] converters;
  
  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length != 2 && arguments.length != 3) {
      throw new UDFArgumentLengthException(
          "The function LOCATE accepts exactly 2 or 3 arguments.");
    }

    for(int i = 0; i < arguments.length; i++) {
      Category category = arguments[i].getCategory();
      if(category != Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i,
            "The " + GenericUDFUtils.getOrdinal(i + 1) + " argument of function LOCATE is expected to a " 
            + Category.PRIMITIVE.toString().toLowerCase()
            + " type, but " + category.toString().toLowerCase() + " is found");
      }
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for(int i = 0; i < arguments.length; i++) {
      if(i == 0 || i == 1) {
        converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
            PrimitiveObjectInspectorFactory.writableStringObjectInspector);
      } else if(i == 2) {
        converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
            PrimitiveObjectInspectorFactory.writableIntObjectInspector);
      }
    }

    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }
  
  IntWritable intWritable = new IntWritable(0);
  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if(arguments[0].get() == null || arguments[1].get() == null)
      return null;

    Text subtext = (Text) converters[0].convert(arguments[0].get());
    Text text = (Text) converters[1].convert(arguments[1].get());
    int start = 1;
    if(arguments.length == 3) {
        IntWritable startWritable = (IntWritable)converters[2].convert(arguments[2].get());
        if(startWritable == null) {
          intWritable.set(0);
          return intWritable;
        }
        start = startWritable.get();
    }
    intWritable.set(GenericUDFUtils.findText(text, subtext, start - 1) + 1);
    return  intWritable;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert(children.length == 2 || children.length == 3);
    return "locate(" + children[0] + children[1] 
           + (children.length == 3 ? children[2] : "") + ")";
  }
}
