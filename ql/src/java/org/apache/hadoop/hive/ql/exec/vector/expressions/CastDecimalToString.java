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

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.DecimalColumnVector;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.serde2.io.HiveDecimalWritable;

/**
 * To support vectorized cast of decimal to string.
 */
public class CastDecimalToString extends DecimalToStringUnaryUDF {

  private static final long serialVersionUID = 1L;

  // Transient members initialized by transientInit method.

  // We use a scratch buffer with the HiveDecimalWritable toBytes method so
  // we don't incur poor performance creating a String result.
  private transient byte[] scratchBuffer;

  public CastDecimalToString() {
    super();
  }

  public CastDecimalToString(int inputColumn, int outputColumnNum) {
    super(inputColumn, outputColumnNum);
  }

  @Override
  public void transientInit(Configuration conf) throws HiveException {
    super.transientInit(conf);

    scratchBuffer = new byte[HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES];
  }

  // The assign method will be overridden for CHAR and VARCHAR.
  protected void assign(BytesColumnVector outV, int i, byte[] bytes, int offset, int length) {
    outV.setVal(i, bytes, offset, length);
  }

  @Override
  protected void func(BytesColumnVector outV, DecimalColumnVector inV, int i) {
    HiveDecimalWritable decWritable = inV.vector[i];
    final int byteIndex = decWritable.toFormatBytes(inV.scale, scratchBuffer);
    assign(outV, i, scratchBuffer, byteIndex, HiveDecimal.SCRATCH_BUFFER_LEN_TO_BYTES - byteIndex);
  }
}
