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
package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**

 */
public class Decimal64ColumnVector extends LongColumnVector {

  public short scale;
  public short precision;

  private HiveDecimalWritable tempHiveDecWritable;

  public Decimal64ColumnVector(int precision, int scale) {
    this(VectorizedRowBatch.DEFAULT_SIZE, precision, scale);
  }

  public Decimal64ColumnVector(int size, int precision, int scale) {
    super(size);
    this.precision = (short) precision;
    this.scale = (short) scale;
    tempHiveDecWritable = new HiveDecimalWritable();
  }

  public void set(int elementNum, HiveDecimalWritable writable) {
    tempHiveDecWritable.set(writable);
    tempHiveDecWritable.mutateEnforcePrecisionScale(precision, scale);
    if (!tempHiveDecWritable.isSet()) {
      noNulls = false;
      isNull[elementNum] = true;
    } else {
      isNull[elementNum] = false;
      vector[elementNum] = tempHiveDecWritable.serialize64(scale);
    }
  }

  public void set(int elementNum, HiveDecimal hiveDec) {
    tempHiveDecWritable.set(hiveDec);
    tempHiveDecWritable.mutateEnforcePrecisionScale(precision, scale);
    if (!tempHiveDecWritable.isSet()) {
      noNulls = false;
      isNull[elementNum] = true;
    } else {
      isNull[elementNum] = false;
      vector[elementNum] = tempHiveDecWritable.serialize64(scale);
    }
  }
}
