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

import java.util.HashSet;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.Text;

/**
 * Mimics oracle's function translate(str1, str2, str3).
 */
@Description(name = "test_translate",
value = "_FUNC_(str1, str2, str3) - Mimics oracle's function translate(str1, str2, str3)")
public class GenericUDFTestTranslate extends GenericUDF {
  private transient ObjectInspector[] argumentOIs;

  /**
   * Return a corresponding ordinal from an integer.
   */
  static String getOrdinal(int i) {
    int unit = i % 10;
    return (i <= 0) ? "" : (i != 11 && unit == 1) ? i + "st"
        : (i != 12 && unit == 2) ? i + "nd" : (i != 13 && unit == 3) ? i + "rd"
        : i + "th";
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 3) {
      throw new UDFArgumentLengthException(
          "The function TRANSLATE(expr,from_string,to_string) accepts exactly 3 arguments, but "
          + arguments.length + " arguments is found.");
    }

    for (int i = 0; i < 3; i++) {
      if (arguments[i].getTypeName() != serdeConstants.STRING_TYPE_NAME
          && arguments[i].getTypeName() != serdeConstants.VOID_TYPE_NAME) {
        throw new UDFArgumentTypeException(i, "The " + getOrdinal(i + 1)
            + " argument of function TRANSLATE is expected to \""
            + serdeConstants.STRING_TYPE_NAME + "\", but \""
            + arguments[i].getTypeName() + "\" is found");
      }
    }

    argumentOIs = arguments;
    return PrimitiveObjectInspectorFactory.writableStringObjectInspector;
  }

  private final Text resultText = new Text();

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null || arguments[1].get() == null
        || arguments[2].get() == null) {
      return null;
    }
    String exprString = ((StringObjectInspector) argumentOIs[0])
        .getPrimitiveJavaObject(arguments[0].get());
    String fromString = ((StringObjectInspector) argumentOIs[1])
        .getPrimitiveJavaObject(arguments[1].get());
    String toString = ((StringObjectInspector) argumentOIs[2])
        .getPrimitiveJavaObject(arguments[2].get());

    char[] expr = exprString.toCharArray();
    char[] from = fromString.toCharArray();
    char[] to = toString.toCharArray();
    char[] result = new char[expr.length];
    System.arraycopy(expr, 0, result, 0, expr.length);
    Set<Character> seen = new HashSet<Character>();

    for (int i = 0; i < from.length; i++) {
      if (seen.contains(from[i])) {
        continue;
      }
      seen.add(from[i]);
      for (int j = 0; j < expr.length; j++) {
        if (expr[j] == from[i]) {
          result[j] = (i < to.length) ? to[i] : 0;
        }
      }
    }

    int pos = 0;
    for (int i = 0; i < result.length; i++) {
      if (result[i] != 0) {
        result[pos++] = result[i];
      }
    }
    resultText.set(new String(result, 0, pos));
    return resultText;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 3);
    return "translate(" + children[0] + "," + children[1] + "," + children[2]
        + ")";
  }
}
