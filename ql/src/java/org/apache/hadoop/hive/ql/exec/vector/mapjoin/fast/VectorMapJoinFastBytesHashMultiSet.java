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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMultiSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMultiSetResult;
import org.apache.hadoop.io.BytesWritable;

/*
 * An single byte array value hash multi-set optimized for vector map join.
 */
public abstract class VectorMapJoinFastBytesHashMultiSet
        extends VectorMapJoinFastBytesHashTable
        implements VectorMapJoinBytesHashMultiSet {

  private static final Log LOG = LogFactory.getLog(VectorMapJoinFastBytesHashMultiSet.class);

  @Override
  public VectorMapJoinHashMultiSetResult createHashMultiSetResult() {
    return new VectorMapJoinFastHashMultiSet.HashMultiSetResult();
  }

  @Override
  public void assignSlot(int slot, byte[] keyBytes, int keyStart, int keyLength,
          long hashCode, boolean isNewKey, BytesWritable currentValue) {

    int tripleIndex = 3 * slot;
    if (isNewKey) {
      // First entry.
      slotTriples[tripleIndex] = keyStore.add(keyBytes, keyStart, keyLength);
      slotTriples[tripleIndex + 1] = hashCode;
      slotTriples[tripleIndex + 2] = 1;    // Count.
      // LOG.debug("VectorMapJoinFastBytesHashMap add first keyRefWord " + Long.toHexString(slotTriples[tripleIndex]) + " hashCode " + Long.toHexString(slotTriples[tripleIndex + 1]) + " valueRefWord " + Long.toHexString(slotTriples[tripleIndex + 2]));
      keysAssigned++;
    } else {
      // Add another value.
      // LOG.debug("VectorMapJoinFastBytesHashMap add more keyRefWord " + Long.toHexString(slotTriples[tripleIndex]) + " hashCode " + Long.toHexString(slotTriples[tripleIndex + 1]) + " valueRefWord " + Long.toHexString(slotTriples[tripleIndex + 2]));
      slotTriples[tripleIndex + 2]++;
    }
  }

  @Override
  public JoinUtil.JoinResult contains(byte[] keyBytes, int keyStart, int keyLength,
          VectorMapJoinHashMultiSetResult hashMultiSetResult) {

    VectorMapJoinFastHashMultiSet.HashMultiSetResult optimizedHashMultiSetResult =
        (VectorMapJoinFastHashMultiSet.HashMultiSetResult) hashMultiSetResult;

    optimizedHashMultiSetResult.forget();

    long hashCode = VectorMapJoinFastBytesHashUtil.hashKey(keyBytes, keyStart, keyLength);
    long count = findReadSlot(keyBytes, keyStart, keyLength, hashCode);
    JoinUtil.JoinResult joinResult;
    if (count == -1) {
      joinResult = JoinUtil.JoinResult.NOMATCH;
    } else {

      optimizedHashMultiSetResult.set(count);

      joinResult = JoinUtil.JoinResult.MATCH;
    }

    optimizedHashMultiSetResult.setJoinResult(joinResult);

    return joinResult;
  }

  public VectorMapJoinFastBytesHashMultiSet(
      int initialCapacity, float loadFactor, int writeBuffersSize) {
    super(initialCapacity, loadFactor, writeBuffersSize);

    keyStore = new VectorMapJoinFastKeyStore(writeBuffersSize);
  }
}