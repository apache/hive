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

package org.apache.hadoop.hive.llap.io.probe;

import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMap;
import org.apache.hadoop.hive.ql.io.filter.MutableFilterContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinInfo;

import java.io.IOException;

public class OrcProbeLongHashMap extends OrcProbeHashTable {

  private VectorMapJoinLongHashMap probeLongHashMap;
  private VectorMapJoinHashMapResult hashMapResult;
  // For Longs, we have optional min/max filtering on HT bounds
  private boolean useMinMax = false;
  private long min = 0L;
  private long max = 0L;

  public OrcProbeLongHashMap(VectorMapJoinHashTable vTable, VectorMapJoinInfo vInfo) {
    super(vTable, vInfo);
    this.probeLongHashMap = (VectorMapJoinLongHashMap) probeDecodeMapJoinTable;
    this.hashMapResult = probeLongHashMap.createHashMapResult();
    this.useMinMax = probeLongHashMap.useMinMax();
    if (useMinMax) {
      this.min = probeLongHashMap.min();
      this.max = probeLongHashMap.max();
    }
  }

  @Override
  public void init() throws HiveException {

  }

  @Override
  public void filterColumnVector(ColumnVector cv, MutableFilterContext cntx, int batchSize) {
    int[] selected = cntx.updateSelected(batchSize);
    int newSize = 0;
    boolean selectedInUse = false;
    LongColumnVector probeCol = (LongColumnVector) cv;
    try {
      if (probeCol.isRepeating) {
        // Repeating values case
        boolean isNull = !probeCol.noNulls && probeCol.isNull[0];
        if (isNull || ((useMinMax && (probeCol.vector[0] < min || probeCol.vector[0] > max)))) {
          // Repeating and NO match, the entire batch is filtered out.
          selectedInUse = true; // and newSize remains 0
        } else {
          if (probeLongHashMap.lookup(probeCol.vector[0], hashMapResult) == JoinUtil.JoinResult.MATCH) {
            // If repeating and match, next CVs of batch are read FULLY
            // DO NOT set selected here as next CVs are not necessarily repeating!
            newSize = batchSize;
          } else {
            // Repeating and NO match, the entire batch is filtered out.
            selectedInUse = true; // and newSize remains 0
          }
        }
      } else {
        // Non-repeating values case
        // We optimize performance by only looking up the first key in a series of equal keys.
        boolean haveSaveKey = false;
        boolean saveKeyMatch = false;
        long saveKey = 0;
        for (int row = 0; row < batchSize; ++row) {
          if (probeCol.noNulls || !probeCol.isNull[row]) {
            long currentKey = probeCol.vector[row];
            // Equal key series checking.
            if (!haveSaveKey || currentKey != saveKey) {
              // New key.
              haveSaveKey = true;
              saveKey = currentKey;
              if (useMinMax && (currentKey < min || currentKey > max)) {
                // Key out of range for whole hash table.
                saveKeyMatch = false;
              } else {
                saveKeyMatch = probeLongHashMap.lookup(currentKey, hashMapResult) == JoinUtil.JoinResult.MATCH;
              }
            }
            // Pass Valid keys
            if (saveKeyMatch) {
              selected[newSize++] = row;
            }
          }
        }
        selectedInUse = newSize != batchSize;
      }
      cntx.setFilterContext(selectedInUse, selected, newSize);
      LlapIoImpl.LOG.debug("ProbeDecode LongMap Matched: {} selectedInUse {} batchSize {}", newSize, selectedInUse, batchSize);
    } catch (IOException e) {
      LlapIoImpl.LOG.error("ProbeDecode LongMap Filter failed: {}", e);
      e.printStackTrace();
    }
  }
}
