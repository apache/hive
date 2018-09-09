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

import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorExpressionDescriptor;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.metadata.HiveException;

import java.nio.ByteBuffer;

/**
 * An expression representing _bucket_number.
 */
public class BucketNumExpression extends VectorExpression {
  private static final long serialVersionUID = 1L;
  private int rowNum = -1;
  private int bucketNum = -1;

  public BucketNumExpression(int outputColNum) {
    super(outputColNum);
  }

  public void initBuffer(VectorizedRowBatch batch) {
    BytesColumnVector cv = (BytesColumnVector) batch.cols[outputColumnNum];
    cv.isRepeating = false;
    cv.initBuffer();
  }

  public void setRowNum(final int rowNum) {
    this.rowNum = rowNum;
  }

  public void setBucketNum(final int bucketNum) {
    this.bucketNum = bucketNum;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {
    BytesColumnVector cv = (BytesColumnVector) batch.cols[outputColumnNum];
    String bucketNumStr = String.valueOf(bucketNum);
    cv.setVal(rowNum, bucketNumStr.getBytes(), 0, bucketNumStr.length());
  }

  @Override
  public String vectorExpressionParameters() {
    return "col : _bucket_number";
  }

  @Override
  public VectorExpressionDescriptor.Descriptor getDescriptor() {
    return (new VectorExpressionDescriptor.Builder()).build();
  }
}
