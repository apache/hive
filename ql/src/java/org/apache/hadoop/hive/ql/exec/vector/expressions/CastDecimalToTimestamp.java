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

import java.sql.Timestamp;

import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.TimestampColumnVector;
import org.apache.hadoop.hive.ql.util.TimestampUtils;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * Type cast decimal to timestamp. The decimal value is interpreted
 * as NNNN.DDDDDDDDD where NNNN is a number of seconds and DDDDDDDDD
 * is a number of nano-seconds.
 */
public class CastDecimalToTimestamp extends FuncDecimalToTimestamp {
  private static final long serialVersionUID = 1L;

  private transient final HiveDecimalWritable scratchHiveDecimalWritable1 = new HiveDecimalWritable();
  private transient final HiveDecimalWritable scratchHiveDecimalWritable2 = new HiveDecimalWritable();

  public CastDecimalToTimestamp(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  public CastDecimalToTimestamp() {
    super();
  }

  @Override
  protected void func(TimestampColumnVector outV, DecimalColumnVector inV,  int i) {
    Timestamp timestamp =
        TimestampUtils.decimalToTimestamp(
            inV.vector[i],
            scratchHiveDecimalWritable1, scratchHiveDecimalWritable2);
    outV.set(i, timestamp);
  }
}
