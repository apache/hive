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

public class JavaHiveCharObjectInspector extends AbstractPrimitiveJavaObjectInspector
    implements SettableHiveCharObjectInspector {

  // no-arg ctor required for Kyro serialization
  public JavaHiveCharObjectInspector() {
  }

  public JavaHiveCharObjectInspector(CharTypeInfo typeInfo) {
    super(typeInfo);
  }

  public HiveChar getPrimitiveJavaObject(Object o) {
    if (o == null) {
      return null;
    }
    HiveChar value;
    if (o instanceof String) {
      value = new HiveChar((String) o, getMaxLength());
    } else {
      value = (HiveChar) o;
    }
    if (BaseCharUtils.doesPrimitiveMatchTypeParams(value, (CharTypeInfo) typeInfo)) {
      return value;
    }
    // value needs to be converted to match type params
    return getPrimitiveWithParams(value);
  }

  public HiveCharWritable getPrimitiveWritableObject(Object o) {
    if (o == null) {
      return null;
    }
    HiveChar var;
    if (o instanceof String) {
      var = new HiveChar((String) o, getMaxLength());
    } else {
      var = (HiveChar) o;
    }
    return getWritableWithParams(var);
  }

  private HiveChar getPrimitiveWithParams(HiveChar val) {
    HiveChar hc = new HiveChar(val, getMaxLength());
    return hc;
  }

  private HiveCharWritable getWritableWithParams(HiveChar val) {
    HiveCharWritable hcw = new HiveCharWritable();
    hcw.set(val, getMaxLength());
    return hcw;
  }

  public Object set(Object o, HiveChar value) {
    if (BaseCharUtils.doesPrimitiveMatchTypeParams(value,
        (CharTypeInfo) typeInfo)) {
      return value;
    } else {
      return new HiveChar(value, getMaxLength());
    }
  }

  public Object set(Object o, String value) {
    return new HiveChar(value, getMaxLength());
  }

  public Object create(HiveChar value) {
    HiveChar hc = new HiveChar(value, getMaxLength());
    return hc;
  }

  public int getMaxLength() {
    CharTypeInfo ti = (CharTypeInfo) typeInfo;
    return ti.getLength();
  }
}
