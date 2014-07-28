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


import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.HiveDecimalUtils;

public class WritableHiveDecimalObjectInspector extends AbstractPrimitiveWritableObjectInspector
implements SettableHiveDecimalObjectInspector {

  public WritableHiveDecimalObjectInspector() {
  }

  protected WritableHiveDecimalObjectInspector(DecimalTypeInfo typeInfo) {
    super(typeInfo);
  }

  @Override
  public HiveDecimalWritable getPrimitiveWritableObject(Object o) {
    if (o == null) {
      return null;
    }
    return enforcePrecisionScale(((HiveDecimalWritable) o));
  }

  @Override
  public HiveDecimal getPrimitiveJavaObject(Object o) {
    if (o == null) {
      return null;
    }
    return enforcePrecisionScale(((HiveDecimalWritable)o).getHiveDecimal());
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new HiveDecimalWritable((HiveDecimalWritable) o);
  }

  @Override
  public Object set(Object o, byte[] bytes, int scale) {
    HiveDecimalWritable writable = (HiveDecimalWritable)create(bytes, scale);
    if (writable != null) {
      ((HiveDecimalWritable)o).set(writable);
      return o;
    } else {
      return null;
    }
  }

  @Override
  public Object set(Object o, HiveDecimal t) {
    HiveDecimal dec = enforcePrecisionScale(t);
    if (dec != null) {
      ((HiveDecimalWritable) o).set(dec);
      return o;
    } else {
      return null;
    }
  }

  @Override
  public Object set(Object o, HiveDecimalWritable t) {
    HiveDecimalWritable writable = enforcePrecisionScale(t);
    if (writable == null) {
      return null;
    }

    ((HiveDecimalWritable) o).set(writable);
    return o;
  }

  @Override
  public Object create(byte[] bytes, int scale) {
    return new HiveDecimalWritable(bytes, scale);
  }

  @Override
  public Object create(HiveDecimal t) {
    return t == null ? null : new HiveDecimalWritable(t);
  }

  private HiveDecimal enforcePrecisionScale(HiveDecimal dec) {
    return HiveDecimalUtils.enforcePrecisionScale(dec, (DecimalTypeInfo)typeInfo);
  }

  private HiveDecimalWritable enforcePrecisionScale(HiveDecimalWritable writable) {
    return HiveDecimalUtils.enforcePrecisionScale(writable, (DecimalTypeInfo)typeInfo);
  }

}
