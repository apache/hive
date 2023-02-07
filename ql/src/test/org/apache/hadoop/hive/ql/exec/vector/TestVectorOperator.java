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

package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.hive.conf.HiveConf;

public abstract class TestVectorOperator {

  protected HiveConf hiveConf = new HiveConf();

  public enum FakeDataSampleType {
    OrderedSequence,
    Random,
    Repeated
  }

  public class FakeDataReader {
    private final int size;
    private final VectorizedRowBatch vrg;
    private int currentSize = 0;
    private final int len = 1024;

    public FakeDataReader(int size, int numCols, FakeDataSampleType fakeDataSampleType) {
      this.size = size;
      vrg = new VectorizedRowBatch(numCols, len);
      for (int i = 0; i < numCols; i++) {
        try {
          Thread.sleep(2);
        } catch (InterruptedException ignore) {
        }
        vrg.cols[i] = getLongVector(fakeDataSampleType);
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

    private LongColumnVector getLongVector(FakeDataSampleType fakeDataSampleType) {
      LongColumnVector lcv = new LongColumnVector(len);

      switch (fakeDataSampleType) {
        case OrderedSequence:
          TestVectorizedRowBatch.setOrderedSequenceLongCol(lcv);
          break;
        case Random:
          TestVectorizedRowBatch.setRandomLongCol(lcv);
          break;
        case Repeated:
          TestVectorizedRowBatch.setRepeatingLongCol(lcv);
          break;
      }

      return lcv;
    }
  }
}
