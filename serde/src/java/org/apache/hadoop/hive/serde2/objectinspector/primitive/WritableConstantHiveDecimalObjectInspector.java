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
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;

/**
 * A WritableConstantHiveDecimalObjectInspector is a WritableHiveDecimalObjectInspector
 * that implements ConstantObjectInspector.
 */
public class WritableConstantHiveDecimalObjectInspector extends WritableHiveDecimalObjectInspector
implements ConstantObjectInspector {

  private HiveDecimalWritable value;

  protected WritableConstantHiveDecimalObjectInspector() {
    super();
  }

  WritableConstantHiveDecimalObjectInspector(DecimalTypeInfo typeInfo,
      HiveDecimalWritable value) {
    super(typeInfo);
    this.value = value;
  }

  @Override
  public HiveDecimalWritable getWritableConstantValue() {
    // We need to enforce precision/scale here.
    // A little inefficiency here as we need to create a HiveDecimal instance from the writable and
    // recreate a HiveDecimalWritable instance on the HiveDecimal instance. However, we don't know
    // the precision/scale of the original writable until we get a HiveDecimal instance from it.
    DecimalTypeInfo decTypeInfo = (DecimalTypeInfo)typeInfo;
    HiveDecimal dec = value == null ? null :
      value.getHiveDecimal(decTypeInfo.precision(), decTypeInfo.scale());
    if (dec == null) {
      return null;
    }
    return new HiveDecimalWritable(dec);
  }

  @Override
  public int precision() {
    return value.getHiveDecimal().precision();
  }

  @Override
  public int scale() {
    return value.getHiveDecimal().scale();
  }

}
