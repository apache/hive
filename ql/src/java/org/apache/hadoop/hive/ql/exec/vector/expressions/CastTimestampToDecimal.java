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

/**
 * To be used to cast timestamp to decimal.
 */
public class CastTimestampToDecimal extends FuncLongToDecimal {

  private static final long serialVersionUID = 1L;

  public CastTimestampToDecimal() {
    super();
  }

  public CastTimestampToDecimal(int inputColumn, int outputColumn) {
    super(inputColumn, outputColumn);
  }

  @Override
  protected void func(DecimalColumnVector outV, LongColumnVector inV, int i) {

    // the resulting decimal value is 10e-9 * the input long value.
    outV.vector[i].updateFixedPoint(inV.vector[i], (short) 9);
    outV.vector[i].changeScaleDestructive(outV.scale);
    outV.checkPrecisionOverflow(i);
  }
}
