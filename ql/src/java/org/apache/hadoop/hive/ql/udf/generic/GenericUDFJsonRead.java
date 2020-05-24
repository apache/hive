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
import org.apache.hadoop.hive.ql.exec.UDFArgumentTypeException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.json.HiveJsonReader;
import org.apache.hadoop.hive.serde2.json.HiveJsonReader.Feature;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.TextConverter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Parses a json string representation into a Hive struct.
 */
@Description(name = "json_read", value = "_FUNC_(json,type) - "
    + "Parses the given json according to the given complex type specification", extended = ""
    + "Parsed as null: if the json is null, it is the empty string or if it contains only whitespaces\n"
    + "Example:\n" + "select _FUNC_('[]','array<struct<a:string>>' ")
public class GenericUDFJsonRead extends GenericUDF {

  private TextConverter inputConverter;
  private HiveJsonReader jsonReader;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {

    checkArgsSize(arguments, 2, 2);
    checkArgPrimitive(arguments, 0);
    checkArgPrimitive(arguments, 1);
    if (!ObjectInspectorUtils.isConstantObjectInspector(arguments[1])) {
      throw new UDFArgumentTypeException(1, getFuncName() + " argument 2 may only be a constant");
    }

    inputConverter = new TextConverter((PrimitiveObjectInspector) arguments[0]);
    String typeStr = getConstantStringValue(arguments, 1);

    try {
      final TypeInfo t = TypeInfoUtils.getTypeInfoFromTypeString(typeStr);
      final ObjectInspector oi =
          TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(t);
      jsonReader = new HiveJsonReader(oi);
      jsonReader.enable(Feature.PRIMITIVE_TO_WRITABLE);
    } catch (Exception e) {
      throw new UDFArgumentException(getFuncName() + ": Error parsing typestring: " + e.getMessage());
    }

    return jsonReader.getObjectInspector();
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object valObject = arguments[0].get();
    if (valObject == null) {
      return null;
    }

    try {
      String text = inputConverter.convert(valObject).toString();
      if (text.trim().length() == 0) {
        return null;
      }
      return jsonReader.parseStruct(text);
    } catch (Exception e) {
      throw new HiveException("Error parsing json: " + e.getMessage(), e);
    }
  }

  @Override
  public String getDisplayString(String[] children) {
    return getStandardDisplayString("json_read", children);
  }


}
