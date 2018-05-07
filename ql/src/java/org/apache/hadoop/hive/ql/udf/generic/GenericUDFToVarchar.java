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

import java.io.Serializable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.HiveVarcharConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveVarcharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

@Description(name = "varchar",
value = "CAST(<value> as VARCHAR(length)) - Converts the argument to a varchar value.",
extended = "Values will be truncated if the input value is too long to fit"
+ " within the varchar length."
+ "Example:\n "
+ "  > SELECT CAST(1234 AS varchar(10)) FROM src LIMIT 1;\n"
+ "  '1234'")
public class GenericUDFToVarchar extends GenericUDF implements SettableUDF, Serializable {
  private static final Logger LOG = LoggerFactory.getLogger(GenericUDFToVarchar.class.getName());

  private transient PrimitiveObjectInspector argumentOI;
  private transient HiveVarcharConverter converter;

  // The varchar type info need to be set prior to initialization,
  // and must be preserved when the plan serialized to other processes.
  private VarcharTypeInfo typeInfo;

  public GenericUDFToVarchar() {
  }

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length != 1) {
      throw new UDFArgumentException("VARCHAR cast requires a value argument");
    }
    try {
      argumentOI = (PrimitiveObjectInspector) arguments[0];
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
          "The function VARCHAR takes only primitive types");
    }

    // Check if this UDF has been provided with type params for the output varchar type
    SettableHiveVarcharObjectInspector outputOI;
    outputOI = (SettableHiveVarcharObjectInspector)
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo);

    converter = new HiveVarcharConverter(argumentOI, outputOI);
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
    sb.append(" AS ");
    sb.append(typeInfo.getQualifiedName());
    sb.append(")");
    return sb.toString();
  }

/**
  * Provide varchar type parameters for the output object inspector.
  * This should be done before the UDF is initialized.
 */
  @Override
  public void setTypeInfo(TypeInfo typeInfo) throws UDFArgumentException {
    this.typeInfo = (VarcharTypeInfo) typeInfo;
  }

  @Override
  public TypeInfo getTypeInfo() {
    return typeInfo;
  }

}
