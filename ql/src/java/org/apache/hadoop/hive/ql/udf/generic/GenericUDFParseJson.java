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
import org.apache.hadoop.hive.ql.exec.UDFArgumentLengthException;
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.variant.Variant;
import org.apache.hadoop.hive.serde2.variant.VariantBuilder;

import java.io.IOException;
import java.util.List;

@Description(name = "parse_json", value = "_FUNC_(json_string) - Parses a JSON string into a VARIANT type", extended = """
    Example:
      > SELECT _FUNC_('{"a":5}');
      {"a":5}""")
public class GenericUDFParseJson extends GenericUDF {
  private PrimitiveObjectInspector inputOI;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentLengthException("parse_json requires one argument");
    }
    if (arguments[0].getCategory() != ObjectInspector.Category.PRIMITIVE
        || ((PrimitiveObjectInspector) arguments[0]).getPrimitiveCategory()
        != PrimitiveObjectInspector.PrimitiveCategory.STRING) {
      throw new UDFArgumentTypeException(0, "Only string input is accepted");
    }
    inputOI = (PrimitiveObjectInspector) arguments[0];

    // Return a Variant OI
    return ObjectInspectorFactory.getVariantObjectInspector();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object input = arguments[0].get();
    if (input == null) {
      return null;
    }
    String json = inputOI.getPrimitiveJavaObject(input).toString();
    try {
      Variant variant = VariantBuilder.parseJson(json, true);
      return List.of(variant.getMetadata(), variant.getValue());

    } catch (IOException e) {
      throw new HiveException("Failed to parse JSON: " + json, e);
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return "parse_json(" + children[0] + ")";
  }
}