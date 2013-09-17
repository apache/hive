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
package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.hive.serde2.typeinfo.BaseTypeParams;
import org.apache.hadoop.hive.serde2.typeinfo.ParameterizedPrimitiveTypeUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeParams;

public class JavaHiveVarcharObjectInspector
    extends AbstractPrimitiveJavaObjectInspector
    implements SettableHiveVarcharObjectInspector {

  public JavaHiveVarcharObjectInspector(PrimitiveTypeEntry typeEntry) {
    super(typeEntry);
    if (typeEntry.primitiveCategory != PrimitiveCategory.VARCHAR) {
      throw new RuntimeException(
          "TypeEntry of type varchar expected, got " + typeEntry.primitiveCategory);
    }
  }

  public HiveVarchar getPrimitiveJavaObject(Object o) {
    if (o == null) {
      return null;
    }
    HiveVarchar value = (HiveVarchar)o;
    if (ParameterizedPrimitiveTypeUtils.doesPrimitiveMatchTypeParams(
        value, (VarcharTypeParams) typeParams)) {
      return value;
    }
    // value needs to be converted to match the type params (length, etc).
    return getPrimitiveWithParams(value);
  }

  @Override
  public HiveVarcharWritable getPrimitiveWritableObject(Object o) {
    if (o == null) {
      return null;
    }
    return getWritableWithParams((HiveVarchar)o);
  }

  private HiveVarchar getPrimitiveWithParams(HiveVarchar val) {
    HiveVarchar hv = new HiveVarchar(val, getMaxLength());
    return hv;
  }

  private HiveVarcharWritable getWritableWithParams(HiveVarchar val) {
    HiveVarcharWritable newValue = new HiveVarcharWritable();
    newValue.set(val, getMaxLength());
    return newValue;
  }

  @Override
  public Object set(Object o, HiveVarchar value) {
    HiveVarchar setValue = (HiveVarchar)o;
    if (ParameterizedPrimitiveTypeUtils.doesPrimitiveMatchTypeParams(
        value, (VarcharTypeParams) typeParams)) {
      setValue.setValue(value);
    } else {
      // Otherwise value may be too long, convert to appropriate value based on params
      setValue.setValue(value, getMaxLength());
    }

    return setValue;
  }

  @Override
  public Object set(Object o, String value) {
    HiveVarchar convertedValue = (HiveVarchar)o;
    convertedValue.setValue(value, getMaxLength());
    return convertedValue;
  }

  @Override
  public Object create(HiveVarchar value) {
    HiveVarchar hc = new HiveVarchar(value, getMaxLength());
    return hc;
  }

  public int getMaxLength() {
    return typeParams != null ? ((VarcharTypeParams) typeParams).length : -1;
  }
}
