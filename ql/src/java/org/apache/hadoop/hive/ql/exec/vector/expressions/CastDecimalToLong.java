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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * Type cast decimal to long
 */
public class CastDecimalToLong extends FuncDecimalToLong {
  private static final long serialVersionUID = 1L;

  public CastDecimalToLong() {
    super();
  }

  public CastDecimalToLong(int inputColumn, int outputColumn) {
    super(inputColumn, outputColumn);
  }

  @Override
  protected void func(LongColumnVector outV, DecimalColumnVector inV,  int i) {
    HiveDecimalWritable decWritable = inV.vector[i];

    // Check based on the Hive integer type we need to test with isByte, isShort, isInt, isLong
    // so we do not use corrupted (truncated) values for the Hive integer type.
    boolean isInRange;
    switch (integerPrimitiveCategory) {
    case BYTE:
      isInRange = decWritable.isByte();
      break;
    case SHORT:
      isInRange = decWritable.isShort();
      break;
    case INT:
      isInRange = decWritable.isInt();
      break;
    case LONG:
      isInRange = decWritable.isLong();
      break;
    default:
      throw new RuntimeException("Unexpected integer primitive category " + integerPrimitiveCategory);
    }
    if (!isInRange) {
      outV.isNull[i] = true;
      outV.noNulls = false;
      return;
    }
    switch (integerPrimitiveCategory) {
    case BYTE:
      outV.vector[i] = decWritable.byteValue();
      break;
    case SHORT:
      outV.vector[i] = decWritable.shortValue();
      break;
    case INT:
      outV.vector[i] = decWritable.intValue();
      break;
    case LONG:
      outV.vector[i] = decWritable.longValue();
      break;
    default:
      throw new RuntimeException("Unexpected integer primitive category " + integerPrimitiveCategory);
    }
  }
}
