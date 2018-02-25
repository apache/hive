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

package org.apache.hadoop.hive.ql.exec.vector.mapjoin.fast;

import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMultiSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMultiSetResult;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;

import com.google.common.annotations.VisibleForTesting;

/*
 * An bytes key hash multi-set optimized for vector map join.
 *
 * This is the abstract base for the multi-key and string bytes key hash multi-set implementations.
 */
public abstract class VectorMapJoinFastBytesHashMultiSet
        extends VectorMapJoinFastBytesHashTable
        implements VectorMapJoinBytesHashMultiSet {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastBytesHashMultiSet.class);

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

    long hashCode = HashCodeUtil.murmurHash(keyBytes, keyStart, keyLength);
    long count = findReadSlot(keyBytes, keyStart, keyLength, hashCode, hashMultiSetResult.getReadPos());
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
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount) {
    super(initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount);

    keyStore = new VectorMapJoinFastKeyStore(writeBuffersSize);
  }

  @Override
  public long getEstimatedMemorySize() {
    return super.getEstimatedMemorySize() + keyStore.getEstimatedMemorySize();
  }
}
