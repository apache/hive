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
import org.apache.hadoop.hive.ql.exec.vector.TestVectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;

/**
 * Fundamental logic and performance tests for vector filters belong here.
 * <p>
 * For tests about filters to cover specific operator and data type combinations,
 * see also the other filter tests under org.apache.hadoop.hive.ql.exec.vector.expressions
 */
public class FakeDataReader {
  private final int size;
  private final VectorizedRowBatch vrg;
  private int currentSize = 0;
  private final int len = 1024;

  public FakeDataReader(int size, int numCols) {
    this.size = size;
    vrg = new VectorizedRowBatch(numCols, len);
    for (int i = 0; i < numCols; i++) {
      try {
        Thread.sleep(2);
      } catch (InterruptedException ignore) {
      }
      vrg.cols[i] = getLongVector();
    }
  }

  public VectorizedRowBatch getNext() {
    if (currentSize >= size) {
      vrg.size = 0;
    } else {
      vrg.size = len;
      currentSize += vrg.size;
      vrg.selectedInUse = false;
    }
    return vrg;
  }

  private LongColumnVector getLongVector() {
    LongColumnVector lcv = new LongColumnVector(len);
    TestVectorizedRowBatch.setRandomLongCol(lcv);
    return lcv;
  }
}
