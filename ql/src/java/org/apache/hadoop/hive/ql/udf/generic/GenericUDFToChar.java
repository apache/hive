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

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.HiveCharConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;

@Description(name = "char",
value = "CAST(<value> as CHAR(length)) - Converts the argument to a char value.",
extended = "Values will be truncated if the input value is too long to fit"
+ " within the char length."
+ "Example:\n "
+ "  > SELECT CAST(1234 AS char(10)) FROM src LIMIT 1;\n"
+ "  '1234'")
public class GenericUDFToChar extends GenericUDF implements SettableUDF, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(GenericUDFToChar.class.getName());

  private transient PrimitiveObjectInspector argumentOI;
  private transient HiveCharConverter converter;

  // The char type info need to be set prior to initialization,
  // and must be preserved when the plan serialized to other processes.
  private CharTypeInfo typeInfo;

  public GenericUDFToChar() {
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("CHAR cast requires a value argument");
    }
    try {
      argumentOI = (PrimitiveObjectInspector) arguments[0];
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
          "The function CHAR takes only primitive types");
    }

    // Check if this UDF has been provided with type params for the output char type
    SettableHiveCharObjectInspector outputOI;
    outputOI = (SettableHiveCharObjectInspector)
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo);

    converter = new HiveCharConverter(argumentOI, outputOI);
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
      Object o0 = arguments[0].get();
      if (o0 == null) {
        return null;
      }

      return converter.convert(o0);
  }

  @Override
  public String getDisplayString(String[] children) {
    assert (children.length == 1);
    StringBuilder sb = new StringBuilder();
    sb.append("CAST( ");
    sb.append(children[0]);
    sb.append(" AS CHAR(");
    sb.append("" + typeInfo.getLength());
    sb.append(")");
    return sb.toString();
  }

/**
  * Provide char type parameters for the output object inspector.
  * This should be done before the UDF is initialized.
 */
  @Override
  public void setTypeInfo(TypeInfo typeInfo) throws UDFArgumentException {
    this.typeInfo = (CharTypeInfo) typeInfo;
  }

  @Override
  public TypeInfo getTypeInfo() {
    return typeInfo;
  }

}
