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

import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping.STRING_GROUP;

import org.apache.commons.lang.WordUtils;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedExpressions;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringInitCap;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorConverters.Converter;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.io.Text;

/**
 * UDFInitCap.
 *
 */
@Description(
    name = "initcap",
    value = "_FUNC_(str) - Returns str, with the first letter of each word in uppercase,"
        + " all other letters in lowercase. Words are delimited by white space.",
    extended = "Example:\n" + " > SELECT _FUNC_('tHe soap') FROM src LIMIT 1;\n" + " 'The Soap'")
@VectorizedExpressions({ StringInitCap.class })
public class GenericUDFInitCap extends GenericUDF {
  private transient PrimitiveCategory[] inputTypes = new PrimitiveCategory[1];
  private transient Converter[] converters = new Converter[1];
  private final Text output = new Text();

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    checkArgsSize(arguments, 1, 1);

    checkArgPrimitive(arguments, 0);

    checkArgGroups(arguments, 0, inputTypes, STRING_GROUP);

    obtainStringConverter(arguments, 0, inputTypes, converters);

    ObjectInspector outputOI = PrimitiveObjectInspectorFactory.writableStringObjectInspector;
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    String val = getStringValue(arguments, 0, converters);
    if (val == null) {
      return null;
    }

    String valCap = WordUtils.capitalizeFully(val);
    output.set(valCap);
    return output;
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString(getFuncName(), children);
  }

  @Override
  protected String getFuncName() {
    return "initcap";
  }
}