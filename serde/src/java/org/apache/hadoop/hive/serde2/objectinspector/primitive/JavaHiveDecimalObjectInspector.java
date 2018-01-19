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

import java.math.BigInteger;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;

public class JavaHiveDecimalObjectInspector
extends AbstractPrimitiveJavaObjectInspector
implements SettableHiveDecimalObjectInspector {

  public JavaHiveDecimalObjectInspector() {
  }

  public JavaHiveDecimalObjectInspector(DecimalTypeInfo typeInfo) {
    super(typeInfo);
  }

  @Override
  public HiveDecimalWritable getPrimitiveWritableObject(Object o) {
    if (o == null) {
      return null;
    }

    if (o instanceof String) {
      HiveDecimal dec = enforcePrecisionScale(HiveDecimal.create((String)o));
      return dec == null ? null : new HiveDecimalWritable(dec);
    }

    HiveDecimal dec = enforcePrecisionScale((HiveDecimal)o);
    return dec == null ? null : new HiveDecimalWritable(dec);
  }

  @Override
  public HiveDecimal getPrimitiveJavaObject(Object o) {
    return enforcePrecisionScale((HiveDecimal)o);
  }

  @Override
  public Object set(Object o, byte[] bytes, int scale) {
    return enforcePrecisionScale(HiveDecimal.create(new BigInteger(bytes), scale));
  }

  @Override
  public Object set(Object o, HiveDecimal t) {
    return enforcePrecisionScale(t);
  }

  @Override
  public Object set(Object o, HiveDecimalWritable t) {
    return t == null ? null : enforcePrecisionScale(t.getHiveDecimal());
  }

  @Override
  public Object create(byte[] bytes, int scale) {
    return HiveDecimal.create(new BigInteger(bytes), scale);
  }

  @Override
  public Object create(HiveDecimal t) {
    return t;
  }

  private HiveDecimal enforcePrecisionScale(HiveDecimal dec) {
    return HiveDecimalUtils.enforcePrecisionScale(dec,(DecimalTypeInfo)typeInfo);
  }

}
