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

package org.apache.hadoop.hive.ql.exec.vector.util;

import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * VectorizedRowBatch test source from individual column values (as RLE)
 * Used in unit test only.
 */
public class FakeVectorRowBatchFromRepeats extends FakeVectorRowBatchBase {
  private Long[] values;
  private int count;
  private int batchSize;
  private VectorizedRowBatch vrg;
  private final int numCols;

  public FakeVectorRowBatchFromRepeats(
    Long[] values,
    int count,
    int batchSize) {
    this.values = values;
    this.count = count;
    this.batchSize = batchSize;
    this.numCols = values.length;
    vrg = new VectorizedRowBatch(numCols, batchSize);
    for (int i =0; i < numCols; i++) {
      vrg.cols[i] = new LongColumnVector(batchSize);
    }
  }

  @Override
  public VectorizedRowBatch produceNextBatch() {
    vrg.size = 0;
    vrg.selectedInUse = false;
    if (count > 0) {
      vrg.size = batchSize < count ? batchSize : count;
      count -= vrg.size;
      for (int i=0; i<numCols; ++i) {
        LongColumnVector col = (LongColumnVector)vrg.cols[i];
        col.isRepeating = true;
        Long value = values[i];
        if (value == null) {
          col.isNull[0] = true;
          col.noNulls = false;
        } else {
          col.noNulls = true;
          col.vector[0] = value;
        }
      }
    }
    return vrg;
  }
}

