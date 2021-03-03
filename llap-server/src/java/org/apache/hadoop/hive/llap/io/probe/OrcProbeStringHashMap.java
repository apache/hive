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
import org.apache.hadoop.hive.ql.exec.vector.BytesColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.expressions.StringExpr;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.io.filter.MutableFilterContext;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.VectorMapJoinInfo;

import java.io.IOException;

public class OrcProbeStringHashMap extends OrcProbeHashTable {

  private VectorMapJoinBytesHashMap probeStringHashMap;
  private VectorMapJoinHashMapResult hashMapResult;

  public OrcProbeStringHashMap(VectorMapJoinHashTable vTable, VectorMapJoinInfo vInfo) {
    super(vTable, vInfo);
    this.probeStringHashMap = (VectorMapJoinBytesHashMap) probeDecodeMapJoinTable;
    this.hashMapResult = probeStringHashMap.createHashMapResult();
  }

  @Override
  public void init() throws HiveException {

  }

  @Override
  public void filterColumnVector(ColumnVector cv, MutableFilterContext cntx, int batchSize) {
    int[] selected = null;
    int newSize = 0;
    boolean selectedInUse = false;
    BytesColumnVector probeCol = (BytesColumnVector) cv;
    byte[][] vector = probeCol.vector;
    int[] start = probeCol.start;
    int[] length = probeCol.length;
    try {
      if (probeCol.isRepeating) {
        // Repeating values case
        boolean isNull = !probeCol.noNulls && probeCol.isNull[0];
        if (isNull) {
          // If repeating and NO match, the entire batch is filtered out.
          selectedInUse = true; // and newSize remains 0
        } else {
          if (probeStringHashMap.lookup(vector[0], start[0], length[0], hashMapResult) == JoinUtil.JoinResult.MATCH) {
            // If repeating and match, next CVs of batch are read FULLY
            // DO NOT set selected here as next CVs are not necessarily repeating!
            newSize = batchSize;
          } else {
            // If repeating and NO match, the entire batch is filtered out.
            selectedInUse = true; // and newSize remains 0
          }
        }
      } else {
        // Non-repeating values case
        selected = cntx.updateSelected(batchSize);
        // We optimize performance by only looking up the first key in a series of equal keys.
        boolean haveSaveKey = false;
        boolean saveKeyMatch = false;
        int saveKeyIndex = -1;
        for (int row = 0; row < batchSize; ++row) {
          if (probeCol.noNulls || !probeCol.isNull[row]) {
            // Equal key series checking.
            if (!haveSaveKey || StringExpr.equal(vector[saveKeyIndex], start[saveKeyIndex], length[saveKeyIndex],
                vector[row], start[row], length[row]) == false) {
              // New key.
              haveSaveKey = true;
              saveKeyIndex = row;
              saveKeyMatch = probeStringHashMap.lookup(vector[row], start[row], length[row], hashMapResult)
                  == JoinUtil.JoinResult.MATCH;
            }
            // Pass Valid keys
            if (saveKeyMatch) {
              selected[newSize++] = row;
            }
          }
        }
        selectedInUse = true;
      }
      cntx.setFilterContext(selectedInUse, selected, newSize);
      LlapIoImpl.LOG.debug("ProbeDecode Str Matched: {} selectedInUse {} batchSize {}", newSize, selectedInUse, batchSize);
    } catch (IOException e) {
      LlapIoImpl.LOG.error("ProbeDecode MultiKey Filter failed: {}", e);
      e.printStackTrace();
    }
  }
}
