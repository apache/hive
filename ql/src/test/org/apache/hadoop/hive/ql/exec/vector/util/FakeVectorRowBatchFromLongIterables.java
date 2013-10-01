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

package org.apache.hadoop.hive.ql.exec.vector.util;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * VectorizedRowBatch test source from individual column values (as iterables)
 * Used in unit test only.
 */
public class FakeVectorRowBatchFromLongIterables extends FakeVectorRowBatchBase {
  private VectorizedRowBatch batch;
  private final int numCols;
  private final int batchSize;
  private List<Iterator<Long>> iterators;
  private boolean eof;

  public FakeVectorRowBatchFromLongIterables(int batchSize, Iterable<Long>...iterables) {
    numCols = iterables.length;
    this.batchSize = batchSize;
    iterators = new ArrayList<Iterator<Long>>();
    batch = new VectorizedRowBatch(numCols, batchSize);
    for (int i =0; i < numCols; i++) {
      batch.cols[i] = new LongColumnVector(batchSize);
      iterators.add(iterables[i].iterator());
    }
  }

  @Override
  public VectorizedRowBatch produceNextBatch() {
    batch.size = 0;
    batch.selectedInUse = false;
    for (int i=0; i < numCols; ++i) {
      ColumnVector col = batch.cols[i];
      col.noNulls = true;
      col.isRepeating = false;
    }
    while (!eof && batch.size < this.batchSize){
      int r = batch.size;
      for (int i=0; i < numCols; ++i) {
        Iterator<Long> it = iterators.get(i);
        if (!it.hasNext()) {
          eof = true;
          break;
        }
        LongColumnVector col = (LongColumnVector)batch.cols[i];
        Long value = it.next();
        if (null == value) {
          col.noNulls = false;
          col.isNull[batch.size] = true;
        } else {
          long[] vector = col.vector;
          vector[r] = value;
          col.isNull[batch.size] = false;
        }
      }
      if (!eof) {
        batch.size += 1;
      }
    }
    return batch;
  }
}

