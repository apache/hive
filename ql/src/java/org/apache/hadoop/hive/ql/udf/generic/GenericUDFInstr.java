/*
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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;

/**
 * Generic UDF for string function <code>INSTR(str,substr[,pos[,occurrence]])</code>.
 * This extends the function from MySQL
 * http://dev.mysql.com/doc/refman/5.1/en/string-functions.html#function_instr
 * and mimics the function from Oracle
 * https://docs.oracle.com/database/121/SQLRF/functions089.htm#SQLRF00651
 *
 * <pre>
 * usage:
 * INSTR(str, substr[, pos[, occurrence]])
 * </pre>
 * <p>
 */
@Description(name = "instr",
    value = "_FUNC_(str, substr[, pos[, occurrence]]) " +
        "- Returns the index of the given occurrence of substr in str after position pos",
    extended = "pos is a 1-based index. If pos < 0, the starting position is\n" +
        "determined by counting backwards from the end of str and then Hive\n" +
        "searches backward from the resulting position.\n" +
        "occurrence is also a 1-based index. The value must be positive.\n" +
        "If occurrence is greater than the number of matching occurrences,\n" +
        "the function returns 0.\n" +
        "If either of the optional arguments, pos or occurrence, is NULL,\n" +
        "the function also returns NULL.\n" +
        "Example:\n" +
        "  > SELECT _FUNC_('Facebook', 'boo') FROM src LIMIT 1;\n" +
        "  5\n" +
        "  > SELECT _FUNC_('CORPORATE FLOOR','OR', 3, 2) FROM src LIMIT 1;\n" +
        "  14\n" +
        "  > SELECT _FUNC_('CORPORATE FLOOR','OR', -3, 2) FROM src LIMIT 1;\n" +
        "  2")
public class GenericUDFInstr extends GenericUDF {

  private transient ObjectInspectorConverters.Converter[] converters;
  private transient PrimitiveObjectInspector.PrimitiveCategory[] inputTypes;
  private transient Integer posConst;
  private transient boolean isPosConst = false;
  private transient Integer occurrenceConst;
  private transient boolean isOccurrenceConst = false;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 4);

    converters = new ObjectInspectorConverters.Converter[arguments.length];
    inputTypes = new PrimitiveObjectInspector.PrimitiveCategory[arguments.length];
    for (int i = 0; i < arguments.length; i++) {
      checkArgPrimitive(arguments, i);
      if (i < 2) {
        obtainStringConverter(arguments, i, inputTypes, converters);
      } else {
        obtainIntConverter(arguments, i, inputTypes, converters);
      }
    }

    if (arguments.length > 2 && arguments[2] instanceof ConstantObjectInspector) {
      posConst = getConstantIntValue(arguments, 2);
      isPosConst = true;
    }
    if (arguments.length > 3 && arguments[3] instanceof ConstantObjectInspector) {
      occurrenceConst = getConstantIntValue(arguments, 3);
      isOccurrenceConst = true;
      if (occurrenceConst != null && occurrenceConst <= 0) {
        throw new UDFArgumentException("occurrence of function INSTR should be " +
            "positive, got " + occurrenceConst);
      }
    }
    return PrimitiveObjectInspectorFactory.writableIntObjectInspector;
  }

  IntWritable intWritable = new IntWritable(0);

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    if (arguments[0].get() == null || arguments[1].get() == null
        || (isPosConst && posConst == null)
        || (isOccurrenceConst && occurrenceConst == null)) {
      return null;
    }
    int pos = 1;
    if (isPosConst) {
      pos = posConst;
    } else if (arguments.length > 2) {
      IntWritable posWritable = (IntWritable) converters[2].convert(arguments[2].get());
      if (posWritable == null) {
        return null;
      }
      pos = posWritable.get();
      if (pos == 0) {
        intWritable.set(0);
        return intWritable;
      }
    }
    int occurrence = 1;
    if (isOccurrenceConst) {
      occurrence = occurrenceConst;
    } else if (arguments.length > 3) {
      IntWritable occurrenceWritable = (IntWritable) converters[3]
          .convert(arguments[3].get());
      if (occurrenceWritable == null) {
        return null;
      }
      occurrence = occurrenceWritable.get();
      if (occurrence <= 0) {
        // The illegal occurrence is not a const value so we can't fail the query at
        // semantic analysis. Just returns NULL for this row.
        return null;
      }
    }

    Text text = (Text) converters[0].convert(arguments[0].get());
    Text subtext = (Text) converters[1].convert(arguments[1].get());
    String textString = text.toString();
    String subtextString = subtext.toString();

    int matchPos = -1;
    int matchNum;
    if (pos > 0) {
      int beg = pos - 1;
      for (matchNum = 0; matchNum < occurrence && beg < textString.length(); matchNum++) {
        matchPos = textString.indexOf(subtextString, beg);
        if (matchPos == -1) break;
        beg = matchPos + 1;
      }
    } else {
      int beg = textString.length() + pos;
      for (matchNum = 0; matchNum < occurrence && beg >= 0; matchNum++) {
        matchPos = textString.lastIndexOf(subtextString, beg);
        if (matchPos == -1) break;
        beg = matchPos - 1;
      }
    }
    if (matchNum < occurrence) {
      matchPos = -1;
    }

    intWritable.set(matchPos + 1);
    return intWritable;
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length >= 2 && children.length <= 4);
    return getStandardDisplayString("instr", children);
  }
}
