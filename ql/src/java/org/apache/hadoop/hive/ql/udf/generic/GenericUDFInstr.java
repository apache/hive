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
import org.apache.hadoop.io.Text;

/**
 * Generic UDF for string function <code>INSTR(str,substr)</code>. This mimcs
 * the function from MySQL
 * http://dev.mysql.com/doc/refman/5.1/en/string-functions.html#function_instr
 * 
 * <pre>
 * usage:
 * INSTR(str, substr)
 * </pre>
 * <p>
 */
@Description(name = "instr",
    value = "_FUNC_(str, substr) - Returns the index of the first occurance of substr in str",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('Facebook', 'boo') FROM src LIMIT 1;\n" + "  5")
public class GenericUDFInstr extends GenericUDF {

  private transient ObjectInspectorConverters.Converter[] converters;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(
          "The function INSTR accepts exactly 2 arguments.");
    }

    for (int i = 0; i < arguments.length; i++) {
      Category category = arguments[i].getCategory();
      if (category != Category.PRIMITIVE) {
        throw new UDFArgumentTypeException(i, "The "
            + GenericUDFUtils.getOrdinal(i + 1)
            + " argument of function INSTR is expected to a "
            + Category.PRIMITIVE.toString().toLowerCase() + " type, but "
            + category.toString().toLowerCase() + " is found");
      }
    }

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      converters[i] = ObjectInspectorConverters.getConverter(arguments[i],
          PrimitiveObjectInspectorFactory.writableStringObjectInspector);
    }

    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  IntWritable intWritable = new IntWritable(0);

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null || arguments[1].get() == null) {
      return null;
    }

    Text text = (Text) converters[0].convert(arguments[0].get());
    Text subtext = (Text) converters[1].convert(arguments[1].get());
    intWritable.set(GenericUDFUtils.findText(text, subtext, 0) + 1);
    return intWritable;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 2);
    return "instr(" + children[0] + children[1] + ")";
  }
}
