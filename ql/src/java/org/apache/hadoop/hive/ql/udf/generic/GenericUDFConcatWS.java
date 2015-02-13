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
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * Generic UDF for string function
 * <code>CONCAT_WS(sep, [string | array(string)]+)<code>.
 * This mimics the function from
 * MySQL http://dev.mysql.com/doc/refman/5.0/en/string-functions.html#
 * function_concat-ws
 *
 * @see org.apache.hadoop.hive.ql.udf.generic.GenericUDF
 */
@Description(name = "concat_ws",
    value = "_FUNC_(separator, [string | array(string)]+) - "
    + "returns the concatenation of the strings separated by the separator.",
    extended = "Example:\n"
    + "  > SELECT _FUNC_('.', 'www', array('facebook', 'com')) FROM src LIMIT 1;\n"
    + "  'www.facebook.com'")
public class GenericUDFConcatWS extends GenericUDF {
  private transient ObjectInspector[] argumentOIs;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 2) {
      throw new UDFArgumentLengthException(
          "The function CONCAT_WS(separator,[string | array(string)]+) "
            + "needs at least two arguments.");
    }

    // check if argument is a string or an array of strings
    for (int i = 0; i < arguments.length; i++) {
      switch(arguments[i].getCategory()) {
        case LIST:
          if (isStringOrVoidType(
              ((ListObjectInspector) arguments[i]).getListElementObjectInspector())) {
            break;
          }
        case PRIMITIVE:
          if (isStringOrVoidType(arguments[i])) {
          break;
          }
        default:
          throw new UDFArgumentTypeException(i, "Argument " + (i + 1)
            + " of function CONCAT_WS must be \"" + serdeConstants.STRING_TYPE_NAME
            + " or " + serdeConstants.LIST_TYPE_NAME + "<" + serdeConstants.STRING_TYPE_NAME
            + ">\", but \"" + arguments[i].getTypeName() + "\" was found.");
      }
    }

    argumentOIs = arguments;
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  protected boolean isStringOrVoidType(ObjectInspector oi) {
    if (oi.getCategory() == Category.PRIMITIVE) {
      if (PrimitiveGrouping.STRING_GROUP
          == PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
              ((PrimitiveObjectInspector) oi).getPrimitiveCategory())
          || ((PrimitiveObjectInspector) oi).getPrimitiveCategory() == PrimitiveCategory.VOID) {
        return true;
      }
    }
    return false;
  }

  private final Text resultText = new Text();

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null) {
      return null;
    }
    String separator = PrimitiveObjectInspectorUtils.getString(
        arguments[0].get(), (PrimitiveObjectInspector)argumentOIs[0]);

    StringBuilder sb = new StringBuilder();
    boolean first = true;
    for (int i = 1; i < arguments.length; i++) {
      if (arguments[i].get() != null) {
        if (first) {
          first = false;
        } else {
          sb.append(separator);
        }
        if (argumentOIs[i].getCategory().equals(Category.LIST)) {
          Object strArray = arguments[i].get();
          ListObjectInspector strArrayOI = (ListObjectInspector) argumentOIs[i];
          boolean strArrayFirst = true;
          for (int j = 0; j < strArrayOI.getListLength(strArray); j++) {
            if (strArrayFirst) {
              strArrayFirst = false;
            } else {
              sb.append(separator);
            }
            sb.append(strArrayOI.getListElement(strArray, j));
          }
        } else {
          sb.append(PrimitiveObjectInspectorUtils.getString(
              arguments[i].get(), (PrimitiveObjectInspector)argumentOIs[i]));
        }
      }
    }

    resultText.set(sb.toString());
    return resultText;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length >= 2);
    return getStandardDisplayString("concat_ws", children);
  }
}
