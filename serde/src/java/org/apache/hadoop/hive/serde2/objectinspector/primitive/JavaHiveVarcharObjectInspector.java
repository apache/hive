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
package org.apache.hadoop.hive.serde2.objectinspector.primitive;

import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;

public class JavaHiveVarcharObjectInspector extends AbstractPrimitiveJavaObjectInspector
  implements SettableHiveVarcharObjectInspector {

  // no-arg ctor required for Kyro serialization
  public JavaHiveVarcharObjectInspector() {
  }

  public JavaHiveVarcharObjectInspector(VarcharTypeInfo typeInfo) {
    super(typeInfo);
  }

  @Override
  public HiveVarchar getPrimitiveJavaObject(Object o) {
    if (o == null) {
      return null;
    }
    HiveVarchar value;
    if (o instanceof String) {
      value= new HiveVarchar((String)o, getMaxLength());
    } else {
      value = (HiveVarchar)o;
    }
    if (BaseCharUtils.doesPrimitiveMatchTypeParams(value, (VarcharTypeInfo) typeInfo)) {
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
    HiveVarchar var;
    if (o instanceof String) {
      var= new HiveVarchar((String)o, getMaxLength());
    } else {
      var = (HiveVarchar)o;
    }
    return getWritableWithParams(var);
  }

  @Override
  public Object set(Object o, HiveVarchar value) {
    if (BaseCharUtils.doesPrimitiveMatchTypeParams(value, (VarcharTypeInfo) typeInfo)) {
      return value;
    } else {
      // Otherwise value may be too long, convert to appropriate value based on params
      return new HiveVarchar(value, getMaxLength());
    }
  }

  @Override
  public Object set(Object o, String value) {
    return new HiveVarchar(value, getMaxLength());
  }

  @Override
  public Object create(HiveVarchar value) {
    return new HiveVarchar(value, getMaxLength());
  }

  public int getMaxLength() {
    VarcharTypeInfo ti = (VarcharTypeInfo) typeInfo;
    return ti.getLength();
  }

  private HiveVarchar getPrimitiveWithParams(HiveVarchar val) {
    return new HiveVarchar(val, getMaxLength());
  }

  private HiveVarcharWritable getWritableWithParams(HiveVarchar val) {
    HiveVarcharWritable newValue = new HiveVarcharWritable();
    newValue.set(val, getMaxLength());
    return newValue;
  }

}
