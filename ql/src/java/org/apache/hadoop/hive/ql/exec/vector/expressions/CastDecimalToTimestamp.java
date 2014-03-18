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

import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.common.type.SqlMathUtil;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;

/**
 * Type cast decimal to timestamp. The decimal value is interpreted
 * as NNNN.DDDDDDDDD where NNNN is a number of seconds and DDDDDDDDD
 * is a number of nano-seconds.
 */
public class CastDecimalToTimestamp extends FuncDecimalToLong {
  private static final long serialVersionUID = 1L;

  /* The field tmp is a scratch variable for this operation. It is
   * purposely not made static because if this code is ever made multi-threaded,
   * each thread will then have its own VectorExpression tree and thus
   * its own copy of the variable.
   */
  private transient Decimal128 tmp = null;
  private static transient Decimal128 tenE9 = new Decimal128(1000000000);

  public CastDecimalToTimestamp(int inputColumn, int outputColumn) {
    super(inputColumn, outputColumn);
    tmp = new Decimal128(0);
  }

  public CastDecimalToTimestamp() {

    // initialize local field after deserialization
    tmp = new Decimal128(0);
  }

  @Override
  protected void func(LongColumnVector outV, DecimalColumnVector inV,  int i) {
    tmp.update(inV.vector[i]);

    // Reduce scale at most by 9, therefore multiplication will not require rounding.
    int newScale = inV.scale > 9 ? (inV.scale - 9) : 0;
    tmp.multiplyDestructive(tenE9, (short) newScale);

    // set output
    outV.vector[i] = tmp.longValue();
  }
}
