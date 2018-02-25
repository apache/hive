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
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.udf.SettableUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorConverter.HiveDecimalConverter;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.SettableHiveDecimalObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

@Description(name = "decimal", value = "_FUNC_(a) - cast a to decimal")
public class GenericUDFToDecimal extends GenericUDF implements SettableUDF {

  private transient PrimitiveObjectInspector argumentOI;
  private transient HiveDecimalConverter bdConverter;

  private DecimalTypeInfo typeInfo;

  @Override
  public ObjectInspector initialize(ObjectInspector[] arguments) throws UDFArgumentException {
    if (arguments.length < 1) {
      throw new UDFArgumentLengthException(
          "The function DECIMAL requires at least one argument, got "
          + arguments.length);
    }
    try {
      argumentOI = (PrimitiveObjectInspector) arguments[0];
    } catch (ClassCastException e) {
      throw new UDFArgumentException(
          "The function DECIMAL takes only primitive types");
    }

    // Check if this UDF has been provided with type params for the output varchar type
    SettableHiveDecimalObjectInspector outputOI;
    outputOI = (SettableHiveDecimalObjectInspector)
          PrimitiveObjectInspectorFactory.getPrimitiveWritableObjectInspector(typeInfo);

    bdConverter = new HiveDecimalConverter(argumentOI, outputOI);
    return outputOI;
  }

  @Override
  public Object evaluate(DeferredObject[] arguments) throws HiveException {
    Object o0 = arguments[0].get();
    if (o0 == null) {
      return null;
    }

    return bdConverter.convert(o0);
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

  public DecimalTypeInfo getTypeInfo() {
    return typeInfo;
  }

  public void setTypeInfo(DecimalTypeInfo typeInfo) {
    this.typeInfo = typeInfo;
  }

  @Override
  public void setTypeInfo(TypeInfo typeInfo) throws UDFArgumentException {
    this.typeInfo = (DecimalTypeInfo) typeInfo;
  }

}
