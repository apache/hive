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

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharUtils;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;

public class WritableHiveCharObjectInspector extends AbstractPrimitiveWritableObjectInspector
    implements SettableHiveCharObjectInspector {
  // no-arg ctor required for Kyro serialization
  public WritableHiveCharObjectInspector() {
  }

  public WritableHiveCharObjectInspector(CharTypeInfo typeInfo) {
    super(typeInfo);
  }

  @Override
  public HiveChar getPrimitiveJavaObject(Object o) {
    // check input object's length, if it doesn't match
    // then output a new primitive with the correct params.
    if (o == null) {
      return null;
    }
    HiveCharWritable writable = ((HiveCharWritable) o);
    if (doesWritableMatchTypeParams(writable)) {
      return writable.getHiveChar();
    }
    return getPrimitiveWithParams(writable);
  }

  @Override
  public HiveCharWritable getPrimitiveWritableObject(Object o) {
    // check input object's length, if it doesn't match
    // then output new writable with correct params.
    if (o == null) {
      return null;
    }
    HiveCharWritable writable = ((HiveCharWritable) o);
    if (doesWritableMatchTypeParams((HiveCharWritable) o)) {
      return writable;
    }

    return getWritableWithParams(writable);
  }

  private HiveChar getPrimitiveWithParams(HiveCharWritable val) {
    HiveChar hv = new HiveChar();
    hv.setValue(val.getHiveChar(), getMaxLength());
    return hv;
  }

  private HiveCharWritable getWritableWithParams(HiveCharWritable val) {
    HiveCharWritable newValue = new HiveCharWritable();
    newValue.set(val, getMaxLength());
    return newValue;
  }

  private boolean doesWritableMatchTypeParams(HiveCharWritable writable) {
    return BaseCharUtils.doesWritableMatchTypeParams(
        writable, (CharTypeInfo)typeInfo);
  }

  public Object copyObject(Object o) {
    if (o == null) {
      return null;
    }
    HiveCharWritable writable = (HiveCharWritable) o;
    if (doesWritableMatchTypeParams((HiveCharWritable) o)) {
      return new HiveCharWritable(writable);
    }
    return getWritableWithParams(writable);
  }

  @Override
  public Object set(Object o, HiveChar value) {
    HiveCharWritable writable = (HiveCharWritable) o;
    writable.set(value, getMaxLength());
    return o;
  }

  @Override
  public Object set(Object o, String value) {
    HiveCharWritable writable = (HiveCharWritable) o;
    writable.set(value, getMaxLength());
    return o;
  }

  @Override
  public Object create(HiveChar value) {
    HiveCharWritable ret;
    ret = new HiveCharWritable();
    ret.set(value, getMaxLength());
    return ret;
  }

  public int getMaxLength() {
    return ((CharTypeInfo)typeInfo).getLength();
  }
}
