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

package org.apache.hadoop.hive.ql.exec.vector.expressions;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor.ArgumentType;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * Vectorized implementation of trunc(number, scale) function for decimal input
 */
public class TruncDecimal extends TruncFloat {
  /**
   * 
   */
  private static final long serialVersionUID = 1L;

  public TruncDecimal() {
    super();
  }

  public TruncDecimal(int colNum, int scale, int outputColumnNum) {
    super(colNum, scale, outputColumnNum);
  }

  @Override
  protected void trunc(ColumnVector inputColVector, ColumnVector outputColVector, int i) {
    HiveDecimal input = ((DecimalColumnVector) inputColVector).vector[i].getHiveDecimal();

    HiveDecimal output = trunc(input);
    ((DecimalColumnVector) outputColVector).vector[i] = new HiveDecimalWritable(output);
  }

  protected HiveDecimal trunc(HiveDecimal input) {
    HiveDecimal pow = HiveDecimal.create(Math.pow(10, Math.abs(scale)));

    if (scale >= 0) {
      if (scale != 0) {
        long longValue = input.multiply(pow).longValue();
        return HiveDecimal.create(longValue).divide(pow);
      } else {
        return HiveDecimal.create(input.longValue());
      }
    } else {
      long longValue2 = input.divide(pow).longValue();
      return HiveDecimal.create(longValue2).multiply(pow);
    }
  }

  protected ArgumentType getInputColumnType() {
    return VectorExpressionDescriptor.ArgumentType.DECIMAL;
  }
}
