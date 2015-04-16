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

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.VOID_GROUP;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.IntWritable;

/**
 * GenericUDFLevenshtein.
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
public class GenericUDFLevenshtein extends GenericUDF {
  private transient Converter[] converters = new Converter[2];
  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[2];
  private final IntWritable output = new IntWritable();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 2, 2);

    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);

    checkArgGroups(arguments, 0, inputTypes, STRING_GROUP, VOID_GROUP);
    checkArgGroups(arguments, 1, inputTypes, STRING_GROUP, VOID_GROUP);

    obtainStringConverter(arguments, 0, inputTypes, converters);
    obtainStringConverter(arguments, 1, inputTypes, converters);

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableIntObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    String str0 = getStringValue(arguments, 0, converters);
    String str1 = getStringValue(arguments, 1, converters);

    if (str0 == null || str1 == null) {
      return null;
    }

    int dist = StringUtils.getLevenshteinDistance(str0, str1);
    output.set(dist);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "levenshtein";
  }
}
