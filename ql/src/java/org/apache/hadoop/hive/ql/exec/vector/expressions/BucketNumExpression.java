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

/**
 * An expression representing _bucket_number.
 */
public class BucketNumExpression extends VectorExpression {
  private static final long serialVersionUID = 1L;
  private int rowNum = -1;
  private int bucketNum = -1;
  private boolean rowSet = false;
  private boolean bucketNumSet = false;

  public BucketNumExpression(int outputColNum) {
    super(-1, outputColNum);
  }

  public void initBuffer(VectorizedRowBatch batch) {
    BytesColumnVector cv = (BytesColumnVector) batch.cols[outputColumnNum];
    cv.isRepeating = false;
    cv.initBuffer();
  }

  public void setRowNum(final int rowNum) throws HiveException{
    this.rowNum = rowNum;
    if (rowSet) {
      throw new HiveException("Row number is already set");
    }
    rowSet = true;
  }

  public void setBucketNum(final int bucketNum) throws HiveException{
    this.bucketNum = bucketNum;
    if (bucketNumSet) {
      throw new HiveException("Bucket number is already set");
    }
    bucketNumSet = true;
  }

  @Override
  public void evaluate(VectorizedRowBatch batch) throws HiveException {
    if (!rowSet || !bucketNumSet) {
      throw new HiveException("row number or bucket number is not set before evaluation");
    }
    BytesColumnVector cv = (BytesColumnVector) batch.cols[outputColumnNum];
    String bucketNumStr = String.valueOf(bucketNum);
    cv.setVal(rowNum, bucketNumStr.getBytes(), 0, bucketNumStr.length());
    rowSet = false;
    bucketNumSet = false;
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
