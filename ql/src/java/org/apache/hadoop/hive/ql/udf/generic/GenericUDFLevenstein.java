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

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.io.IntWritable;

/**
 * GenericUDFLevenstein.
 *
 * This function calculates the Levenshtein distance between two strings.
 * Levenshtein distance is a string metric for measuring the difference between
 * two sequences. Informally, the Levenshtein distance between two words is the
 * minimum number of single-character edits (i.e. insertions, deletions or
 * substitutions) required to change one word into the other. It is named after
 * Vladimir Levenshtein, who considered this distance in 1965
 *
 */
@Description(name = "levenshtein", value = "_FUNC_(str1, str2) - This function calculates the Levenshtein distance between two strings.",
    extended = "Levenshtein distance is a string metric for measuring the difference between"
    + " two sequences. Informally, the Levenshtein distance between two words is the"
    + " minimum number of single-character edits (i.e. insertions, deletions or"
    + " substitutions) required to change one word into the other. It is named after"
    + " Vladimir Levenshtein, who considered this distance in 1965."
    + "Example:\n "
    + " > SELECT _FUNC_('kitten', 'sitting');\n 3")
public class GenericUDFLevenstein extends GenericUDF {
  private transient Converter[] textConverters = new Converter[2];
  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[2];
  private final IntWritable output = new IntWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 2) {
      throw new UDFArgumentLengthException(getFuncName() + " requires 2 arguments, got "
          + arguments.length);
    }
    checkIfPrimitive(arguments, 0, "1st");
    checkIfPrimitive(arguments, 1, "2nd");

    checkIfStringGroup(arguments, 0, "1st");
    checkIfStringGroup(arguments, 1, "2nd");

    getStringConverter(arguments, 0, "1st");
    getStringConverter(arguments, 1, "2nd");

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object obj0;
    Object obj1;
    if ((obj0 = arguments[0].get()) == null || (obj1 = arguments[1].get()) == null) {
      return null;
    }

    String str0 = textConverters[0].convert(obj0).toString();
    String str1 = textConverters[1].convert(obj1).toString();

    int dist = StringUtils.getLevenshteinDistance(str0, str1);
    output.set(dist);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  protected void checkIfPrimitive(ObjectInspector[] arguments, int i, String argOrder)
      throws UDFArgumentTypeException {
    ObjectInspector.Category oiCat = arguments[i].getCategory();
    if (oiCat != ObjectInspector.Category.PRIMITIVE) {
      throw new UDFArgumentTypeException(i, getFuncName() + " only takes primitive types as "
          + argOrder + " argument, got " + oiCat);
    }
  }

  protected void checkIfStringGroup(ObjectInspector[] arguments, int i, String argOrder)
      throws UDFArgumentTypeException {
    inputTypes[i] = ((PrimitiveObjectInspector) arguments[i]).getPrimitiveCategory();
    if (PrimitiveObjectInspectorUtils.getPrimitiveGrouping(inputTypes[i]) != PrimitiveGrouping.STRING_GROUP) {
      throw new UDFArgumentTypeException(i, getFuncName() + " only takes STRING_GROUP types as "
          + argOrder + " argument, got " + inputTypes[i]);
    }
  }

  protected void getStringConverter(ObjectInspector[] arguments, int i, String argOrder)
      throws UDFArgumentTypeException {
    textConverters[i] = ObjectInspectorConverters.getConverter(
        (PrimitiveObjectInspector) arguments[i],
        PrimitiveObjectInspectorFactory.writableStringObjectInspector);
  }

  protected String getFuncName() {
    return "levenshtein";
  }
}
