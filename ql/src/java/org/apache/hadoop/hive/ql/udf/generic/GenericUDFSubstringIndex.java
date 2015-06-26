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

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.NUMERIC_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * GenericUDFSubstringIndex.
 *
 */
@Description(name = "substring_index",
    value = "_FUNC_(str, delim, count) - Returns the substring from string str before count occurrences "
        + "of the delimiter delim.",
    extended = "If count is positive, everything to the left of the final delimiter (counting from the left) "
        + "is returned. If count is negative, everything to the right of the final delimiter "
        + "(counting from the right) is returned. Substring_index performs a case-sensitive match when searching "
        + "for delim.\n"
        + "Example:\n > SELECT _FUNC_('www.apache.org', '.', 2);\n 'www.apache'")
public class GenericUDFSubstringIndex extends GenericUDF {
  private transient Converter[] converters = new Converter[3];
  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[3];
  private final Text output = new Text();
  private transient String delimConst;
  private transient boolean isDelimConst;
  private transient Integer countConst;
  private transient boolean isCountConst;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 3, 3);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);
    checkArgPrimitive(arguments, 2);

    checkArgGroups(arguments, 0, inputTypes, STRING_GROUP);
    checkArgGroups(arguments, 1, inputTypes, STRING_GROUP);
    checkArgGroups(arguments, 2, inputTypes, NUMERIC_GROUP);

    obtainStringConverter(arguments, 0, inputTypes, converters);
    obtainStringConverter(arguments, 1, inputTypes, converters);
    obtainIntConverter(arguments, 2, inputTypes, converters);

    if (arguments[1] instanceof ConstantObjectInspector) {
      delimConst = getConstantStringValue(arguments, 1);
      isDelimConst = true;
    }

    if (arguments[2] instanceof ConstantObjectInspector) {
      countConst = getConstantIntValue(arguments, 2);
      isCountConst = true;
    }

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    // str
    String str = getStringValue(arguments, 0, converters);
    if (str == null) {
      return null;
    }
    if (str.length() == 0) {
      output.set("");
      return output;
    }

    // delim
    String delim;
    if (isDelimConst) {
      delim = delimConst;
    } else {
      delim = getStringValue(arguments, 1, converters);
    }
    if (delim == null) {
      return null;
    }
    if (delim.length() == 0) {
      output.set("");
      return output;
    }

    // count
    Integer countV;
    if (isCountConst) {
      countV = countConst;
    } else {
      countV = getIntValue(arguments, 2, converters);
    }
    if (countV == null) {
      return null;
    }
    int count = countV.intValue();
    if (count == 0) {
      output.set("");
      return output;
    }

    // get substring
    String res;
    if (count > 0) {
      int idx = StringUtils.ordinalIndexOf(str, delim, count);
      if (idx != -1) {
        res = str.substring(0, idx);
      } else {
        res = str;
      }
    } else {
      int idx = StringUtils.lastOrdinalIndexOf(str, delim, -count);
      if (idx != -1) {
        res = str.substring(idx + 1);
      } else {
        res = str;
      }
    }

    output.set(res);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "substring_index";
  }
}
