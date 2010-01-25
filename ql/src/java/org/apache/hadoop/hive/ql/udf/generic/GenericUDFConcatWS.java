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
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * Generic UDF for string function
 * <code>CONCAT_WS(sep,str1,str2,str3,...)</code>. This mimics the function from
 * MySQL http://dev.mysql.com/doc/refman/5.0/en/string-functions.html#
 * function_concat-ws
 * 
 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF
 */
@Description(name = "concat_ws", value = "_FUNC_(separator, str1, str2, ...) - "
    + "returns the concatenation of the strings separated by the separator.", extended = "Example:\n"
    + "  > SELECT _FUNC_('ce', 'fa', 'book') FROM src LIMIT 1;\n"
    + "  'facebook'")
public class GenericUDFConcatWS extends GenericUDF {

  ObjectInspector[] argumentOIs;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments)
      throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(
          "The function CONCAT_WS(separator,str1,str2,str3,...) needs at least two arguments.");
    }

    for (int i = 0; i < arguments.length; i++) {
      if (arguments[i].getTypeName() != Constants.STRING_TYPE_NAME
          && arguments[i].getTypeName() != Constants.VOID_TYPE_NAME) {
        throw new UDFArgumentTypeException(i, "Argument " + (i + 1)
            + " of function CONCAT_WS must be \"" + Constants.STRING_TYPE_NAME
            + "\", but \"" + arguments[i].getTypeName() + "\" was found.");
      }
    }

    argumentOIs = arguments;
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  private final Text resultText = new Text();

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }
    String separator = ((StringObjectInspector) argumentOIs[0])
        .getPrimitiveJavaObject(arguments[0].get());

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (int i = 1; i < arguments.length; i++) {
      if (arguments[i].get() != null) {
        if (first) {
          first = false;
        } else {
          sb.append(separator);
        }
        sb.append(((StringObjectInspector) argumentOIs[i])
            .getPrimitiveJavaObject(arguments[i].get()));
      }
    }

    resultText.set(sb.toString());
    return resultText;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length >= 2);
    StringBuilder sb = new StringBuilder();
    sb.append("concat_ws(");
    for (int i = 0; i < children.length - 1; i++) {
      sb.append(children[i]).append(", ");
    }
    sb.append(children[children.length - 1]).append(")");
    return sb.toString();
  }
}
