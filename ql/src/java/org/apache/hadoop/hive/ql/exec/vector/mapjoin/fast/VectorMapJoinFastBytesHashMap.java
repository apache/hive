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

import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinBytesHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hive.common.util.HashCodeUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/*
 * An bytes key hash map optimized for vector map join.
 *
 * This is the abstract base for the multi-key and string bytes key hash map implementations.
 */
public abstract class VectorMapJoinFastBytesHashMap
        extends VectorMapJoinFastBytesHashTable
        implements VectorMapJoinBytesHashMap {

  private static final Logger LOG = LoggerFactory.getLogger(VectorMapJoinFastBytesHashMap.class);

  private VectorMapJoinFastValueStore valueStore;

  protected BytesWritable testValueBytesWritable;

  @Override
  public VectorMapJoinHashMapResult createHashMapResult() {
    return new VectorMapJoinFastValueStore.HashMapResult();
  }

  @Override
  public void assignSlot(int slot, byte[] keyBytes, int keyStart, int keyLength,
          long hashCode, boolean isNewKey, BytesWritable currentValue) {

    byte[] valueBytes = currentValue.getBytes();
    int valueLength = currentValue.getLength();

    int tripleIndex = 3 * slot;
    if (isNewKey) {
      // First entry.
      slotTriples[tripleIndex] = keyStore.add(keyBytes, keyStart, keyLength);
      slotTriples[tripleIndex + 1] = hashCode;
      slotTriples[tripleIndex + 2] = valueStore.addFirst(valueBytes, 0, valueLength);
      // LOG.debug("VectorMapJoinFastBytesHashMap add first keyRefWord " + Long.toHexString(slotTriples[tripleIndex]) + " hashCode " + Long.toHexString(slotTriples[tripleIndex + 1]) + " valueRefWord " + Long.toHexString(slotTriples[tripleIndex + 2]));
    } else {
      // Add another value.
      // LOG.debug("VectorMapJoinFastBytesHashMap add more keyRefWord " + Long.toHexString(slotTriples[tripleIndex]) + " hashCode " + Long.toHexString(slotTriples[tripleIndex + 1]) + " valueRefWord " + Long.toHexString(slotTriples[tripleIndex + 2]));
      slotTriples[tripleIndex + 2] = valueStore.addMore(slotTriples[tripleIndex + 2], valueBytes, 0, valueLength);
      // LOG.debug("VectorMapJoinFastBytesHashMap add more new valueRefWord " + Long.toHexString(slotTriples[tripleIndex + 2]));
    }
  }

  @Override
  public JoinUtil.JoinResult lookup(byte[] keyBytes, int keyStart, int keyLength, VectorMapJoinHashMapResult hashMapResult) {
    VectorMapJoinFastValueStore.HashMapResult optimizedHashMapResult =
        (VectorMapJoinFastValueStore.HashMapResult) hashMapResult;

    optimizedHashMapResult.forget();

    long hashCode = HashCodeUtil.murmurHash(keyBytes, keyStart, keyLength);
    long valueRefWord = findReadSlot(keyBytes, keyStart, keyLength, hashCode, hashMapResult.getReadPos());
    JoinUtil.JoinResult joinResult;
    if (valueRefWord == -1) {
      joinResult = JoinUtil.JoinResult.NOMATCH;
    } else {
      // LOG.debug("VectorMapJoinFastBytesHashMap lookup hashCode " + Long.toHexString(hashCode) + " valueRefWord " + Long.toHexString(valueRefWord) + " (valueStore != null) " + (valueStore != null));

      optimizedHashMapResult.set(valueStore, valueRefWord);

      joinResult = JoinUtil.JoinResult.MATCH;
    }

    optimizedHashMapResult.setJoinResult(joinResult);

    return joinResult;
  }

  public VectorMapJoinFastBytesHashMap(
      int initialCapacity, float loadFactor, int writeBuffersSize, long estimatedKeyCount) {
    super(initialCapacity, loadFactor, writeBuffersSize, estimatedKeyCount);

    valueStore = new VectorMapJoinFastValueStore(writeBuffersSize);

    // Share the same write buffers with our value store.
    keyStore = new VectorMapJoinFastKeyStore(valueStore.writeBuffers());
  }

  @Override
  public long getEstimatedMemorySize() {
    long size = super.getEstimatedMemorySize();
    size += valueStore.getEstimatedMemorySize();
    // keyStore / valueStore back buffers are shared; so don't need:
    // size += keyStore.getEstimatedMemorySize();
    return size;
  }
}
