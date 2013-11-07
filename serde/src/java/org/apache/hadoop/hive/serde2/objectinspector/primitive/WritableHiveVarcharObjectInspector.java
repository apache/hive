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

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.serde2.io.HiveVarcharWritable;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharUtils;

public class WritableHiveVarcharObjectInspector extends AbstractPrimitiveWritableObjectInspector
implements SettableHiveVarcharObjectInspector {
  private static final Log LOG = LogFactory.getLog(WritableHiveVarcharObjectInspector.class);

  // no-arg ctor required for Kyro serialization
  public WritableHiveVarcharObjectInspector() {
  }

  public WritableHiveVarcharObjectInspector(VarcharTypeInfo typeInfo) {
    super(typeInfo);
  }

  @Override
  public HiveVarchar getPrimitiveJavaObject(Object o) {
    // check input object's length, if it doesn't match
    // then output a new primitive with the correct params.
    if (o == null) {
      return null;
    }
    HiveVarcharWritable writable = ((HiveVarcharWritable)o);
    if (doesWritableMatchTypeParams(writable)) {
      return writable.getHiveVarchar();
    }
    return getPrimitiveWithParams(writable);
  }

  @Override
  public HiveVarcharWritable getPrimitiveWritableObject(Object o) {
    // check input object's length, if it doesn't match
    // then output new writable with correct params.
    if (o == null) {
      return null;
    }
    HiveVarcharWritable writable = ((HiveVarcharWritable)o);
    if (doesWritableMatchTypeParams((HiveVarcharWritable)o)) {
      return writable;
    }

    return getWritableWithParams(writable);
  }

  private HiveVarchar getPrimitiveWithParams(HiveVarcharWritable val) {
    HiveVarchar hv = new HiveVarchar();
    hv.setValue(val.getHiveVarchar(), getMaxLength());
    return hv;
  }

  private HiveVarcharWritable getWritableWithParams(HiveVarcharWritable val) {
    HiveVarcharWritable newValue = new HiveVarcharWritable();
    newValue.set(val, getMaxLength());
    return newValue;
  }

  private boolean doesWritableMatchTypeParams(HiveVarcharWritable writable) {
    return BaseCharUtils.doesWritableMatchTypeParams(
        writable, (VarcharTypeInfo)typeInfo);
  }

  @Override
  public Object copyObject(Object o) {
    if (o == null) {
      return null;
    }
    HiveVarcharWritable writable = (HiveVarcharWritable)o;
    if (doesWritableMatchTypeParams((HiveVarcharWritable)o)) {
      return new HiveVarcharWritable(writable);
    }
    return getWritableWithParams(writable);
  }

  @Override
  public Object set(Object o, HiveVarchar value) {
    HiveVarcharWritable writable = (HiveVarcharWritable)o;
    writable.set(value, getMaxLength());
    return o;
  }

  @Override
  public Object set(Object o, String value) {
    HiveVarcharWritable writable = (HiveVarcharWritable)o;
    writable.set(value, getMaxLength());
    return o;
  }

  @Override
  public Object create(HiveVarchar value) {
    HiveVarcharWritable ret;
    ret = new HiveVarcharWritable();
    ret.set(value, getMaxLength());
    return ret;
  }

  public int getMaxLength() {
    return ((VarcharTypeInfo)typeInfo).getLength();
  }

}
