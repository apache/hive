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
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.util.TimestampUtils;

/**
 * To be used to cast timestamp to decimal.
 */
public class CastTimestampToDecimal extends FuncTimestampToDecimal {

  private static final long serialVersionUID = 1L;

  public CastTimestampToDecimal() {
    super();
  }

  public CastTimestampToDecimal(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  @Override
  protected void func(DecimalColumnVector outV, TimestampColumnVector inV, int i) {
    Double timestampDouble = TimestampUtils.getDouble(inV.asScratchTimestamp(i));
    HiveDecimal result = HiveDecimal.create(timestampDouble.toString());
    outV.set(i, HiveDecimal.create(timestampDouble.toString()));
  }
}
