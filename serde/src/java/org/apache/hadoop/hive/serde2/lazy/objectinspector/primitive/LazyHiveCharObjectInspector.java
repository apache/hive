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
package org.apache.hadoop.hive.serde2.lazy.objectinspector.primitive;


import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.serde2.io.HiveCharWritable;
import org.apache.hadoop.hive.serde2.lazy.LazyHiveChar;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.HiveCharObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharUtils;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;

public class LazyHiveCharObjectInspector
    extends AbstractPrimitiveLazyObjectInspector<HiveCharWritable>
    implements HiveCharObjectInspector {

  private boolean escaped;
  private byte escapeChar;

  // no-arg ctor required for Kyro
  public LazyHiveCharObjectInspector() {
  }

  public LazyHiveCharObjectInspector(CharTypeInfo typeInfo) {
    this(typeInfo, false, (byte)0);
  }

  public LazyHiveCharObjectInspector(CharTypeInfo typeInfo, boolean escaped, byte escapeChar) {
    super(typeInfo);
    this.escaped = escaped;
    this.escapeChar = escapeChar;
  }

  @Override
  public Object copyObject(Object o) {
    if (o == null) {
      return null;
    }

    LazyHiveChar ret = new LazyHiveChar(this);
    ret.setValue((LazyHiveChar) o);
    return ret;
  }

  @Override
  public HiveChar getPrimitiveJavaObject(Object o) {
    if (o == null) {
      return null;
    }

    HiveChar ret = ((LazyHiveChar) o).getWritableObject().getHiveChar();
    if (!BaseCharUtils.doesPrimitiveMatchTypeParams(
        ret, (CharTypeInfo)typeInfo)) {
      HiveChar newValue = new HiveChar(ret, ((CharTypeInfo)typeInfo).getLength());
      return newValue;
    }
    return ret;
  }

  public boolean isEscaped() {
    return escaped;
  }

  public byte getEscapeChar() {
    return escapeChar;
  }

  @Override
  public String toString() {
    return getTypeName();
  }
}
