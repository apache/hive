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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.SortedSet;
import java.util.TreeMap;
import java.util.TreeSet;

import com.google.common.collect.MinMaxPriorityQueue;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.BinaryComparable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;

/**
 * Stores binary key/value in sorted manner to get top-n key/value
 * TODO: rename to TopNHeap?
 */
public class TopNHash {
  public static Log LOG = LogFactory.getLog(TopNHash.class);

  /**
   * For interaction between operator and top-n hash.
   * Currently only used to forward key/values stored in hash.
   */
  public static interface BinaryCollector {
    public void collect(byte[] key, byte[] value, int hash) throws IOException;
  }

  public static final int FORWARD = -1; // Forward the row to reducer as is.
  public static final int EXCLUDE = -2; // Discard the row.
  private static final int MAY_FORWARD = -3; // Vectorized - may forward the row, not sure yet.

  private BinaryCollector collector;
  private int topN;

  private long threshold;   // max heap size
  private long usage;

  // binary keys, values and hashCodes of rows, lined up by index
  private byte[][] keys;
  private byte[][] values;
  private int[] hashes;
  private int[] distKeyLengths;
  private IndexStore indexes; // The heap over the keys, storing indexes in the array.

  private int evicted; // recently evicted index (used for next key/value)
  private int excluded; // count of excluded rows from previous flush

  // temporary single-batch context used for vectorization
  private int batchNumForwards = 0; // whether current batch has any forwarded keys
  private int[] indexToBatchIndex; // mapping of index (lined up w/keys) to index in the batch
  private int[] batchIndexToResult; // mapping of index in the batch (linear) to hash result
  private int batchSize; // Size of the current batch.

  private boolean isEnabled = false;

  private final Comparator<Integer> C = new Comparator<Integer>() {
    public int compare(Integer o1, Integer o2) {
      byte[] key1 = keys[o1];
      byte[] key2 = keys[o2];
      int length1 = distKeyLengths[o1];
      int length2 = distKeyLengths[o2];
      return WritableComparator.compareBytes(key1, 0, length1, key2, 0, length2);
    }
  };

  public void initialize(
      int topN, float memUsage, boolean isMapGroupBy, BinaryCollector collector) {
    assert topN >= 0 && memUsage > 0;
    assert !this.isEnabled;
    this.isEnabled = false;
    this.topN = topN;
    this.collector = collector;
    if (topN == 0) {
      isEnabled = true;
      return; // topN == 0 will cause a short-circuit, don't need any initialization
    }

    // limit * 64 : compensation of arrays for key/value/hashcodes
    this.threshold = (long) (memUsage * Runtime.getRuntime().maxMemory()) - topN * 64;
    if (threshold < 0) {
      return;
    }
    this.indexes = isMapGroupBy ? new HashForGroup() : new HashForRow();
    this.keys = new byte[topN + 1][];
    this.values = new byte[topN + 1][];
    this.hashes = new int[topN + 1];
    this.distKeyLengths = new int[topN + 1];
    this.evicted = topN;
    this.isEnabled = true;
  }

  /**
   * Try store the non-vectorized key.
   * @param key Serialized key.
   * @return TopNHash.FORWARD if the row should be forwarded;
   *         TopNHash.EXCLUDED if the row should be discarded;
   *         any other number if the row is to be stored; the index should be passed to storeValue.
   */
  public int tryStoreKey(HiveKey key) throws HiveException, IOException {
    if (!isEnabled) {
      return FORWARD; // short-circuit quickly - forward all rows
    }
    if (topN == 0) {
      return EXCLUDE; // short-circuit quickly - eat all rows
    }
    int index = insertKeyIntoHeap(key);
    if (index >= 0) {
      usage += key.getLength();
      return index;
    }
    // IndexStore is trying to tell us something.
    switch (index) {
      case FORWARD:  return FORWARD;
      case EXCLUDE: return EXCLUDE; // skip the row.
      default: {
        assert false;
        throw new HiveException("Invalid result trying to store the key: " + index);
      }
    }
  }


  /**
   * Perform basic checks and initialize TopNHash for the new vectorized row batch.
   * @param size batch size
   * @return TopNHash.FORWARD if all rows should be forwarded w/o trying to call TopN;
   *         TopNHash.EXCLUDED if all rows should be discarded w/o trying to call TopN;
   *         any other result means the batch has been started.
   */
  public int startVectorizedBatch(int size) throws IOException, HiveException {
    if (!isEnabled) {
      return FORWARD; // short-circuit quickly - forward all rows
    } else if (topN == 0) {
      return EXCLUDE; // short-circuit quickly - eat all rows
    }
    // Flush here if the memory usage is too high. After that, we have the entire
    // batch already in memory anyway so we will bypass the memory checks.
    if (usage > threshold) {
      int excluded = this.excluded;
      LOG.info("Top-N hash is flushing rows");
      flushInternal();
      if (excluded == 0) {
        LOG.info("Top-N hash has been disabled");
        isEnabled = false;
        return FORWARD; // Hash is ineffective, disable.
      }
    }
    // Started ok; initialize context for new batch.
    batchSize = size;
    if (batchIndexToResult == null || batchIndexToResult.length < batchSize) {
      batchIndexToResult = new int[Math.max(batchSize, VectorizedRowBatch.DEFAULT_SIZE)];
    }
    if (indexToBatchIndex == null) {
      indexToBatchIndex = new int[topN + 1];
    }
    Arrays.fill(indexToBatchIndex, -1);
    batchNumForwards = 0;
    return 0;
  }

  /**
   * Try to put the key from the current vectorized batch into the heap.
   * @param key the key.
   * @param batchIndex The index of the key in the vectorized batch (sequential, not .selected).
   */
  public void tryStoreVectorizedKey(HiveKey key, int batchIndex)
      throws HiveException, IOException {
    // Assumption - batchIndex is increasing; startVectorizedBatch was called
    int size = indexes.size();
    int index = size < topN ? size : evicted;
    keys[index] = Arrays.copyOf(key.getBytes(), key.getLength());
    distKeyLengths[index] = key.getDistKeyLength();
    Integer collisionIndex = indexes.store(index);
    if (null != collisionIndex) {
      // forward conditional on the survival of the corresponding key currently in indexes.
      ++batchNumForwards;
      batchIndexToResult[batchIndex] = MAY_FORWARD - collisionIndex;
      return;
    }
    indexToBatchIndex[index] = batchIndex;
    batchIndexToResult[batchIndex] = index;
    if (size != topN) return;
    evicted = indexes.removeBiggest();  // remove the biggest key
    if (index == evicted) {
      excluded++;
      batchIndexToResult[batchIndex] = EXCLUDE;
      indexToBatchIndex[index] = -1;
      return; // input key is bigger than any of keys in hash
    }
    removed(evicted);
    int evictedBatchIndex = indexToBatchIndex[evicted];
    if (evictedBatchIndex >= 0) {
      // reset the result for the evicted index
      batchIndexToResult[evictedBatchIndex] = EXCLUDE;
      indexToBatchIndex[evicted] = -1;
    }
    // Evict all results grouped with this index; it cannot be any key further in the batch.
    // If we evict a key from this batch, the keys grouped with it cannot be earlier that that key.
    // If we evict a key that is not from this batch, initial i = (-1) + 1 = 0, as intended.
    int evictedForward = (MAY_FORWARD - evicted);
    for (int i = evictedBatchIndex + 1; i < batchIndex && (batchNumForwards > 0); ++i) {
      if (batchIndexToResult[i] == evictedForward) {
        batchIndexToResult[i] = EXCLUDE;
        --batchNumForwards;
      }
    }
  }

  /**
   * Get vectorized batch result for particular index.
   * @param batchIndex index of the key in the batch.
   * @return the result, same as from {@link #tryStoreKey(HiveKey)}
   */
  public int getVectorizedBatchResult(int batchIndex) {
    int result = batchIndexToResult[batchIndex];
    return (result <= MAY_FORWARD) ? FORWARD : result;
  }

  /**
   * After vectorized batch is processed, can return the key that caused a particular row
   * to be forwarded. Because the row could only be marked to forward because it has
   * the same key with some row already in the heap (for GBY), we can use that key from the
   * heap to emit the forwarded row.
   * @param batchIndex index of the key in the batch.
   * @return The key corresponding to the index.
   */
  public HiveKey getVectorizedKeyToForward(int batchIndex) {
    int index = MAY_FORWARD - batchIndexToResult[batchIndex];
    HiveKey hk = new HiveKey();
    hk.set(keys[index], 0, keys[index].length);
    hk.setDistKeyLength(distKeyLengths[index]);
    return hk;
  }

  /**
   * After vectorized batch is processed, can return distribution keys length of a key.
   * @param batchIndex index of the key in the batch.
   * @return The distribution length corresponding to the key.
   */
  public int getVectorizedKeyDistLength(int batchIndex) {
    return distKeyLengths[batchIndexToResult[batchIndex]];
  }

  /**
   * Stores the value for the key in the heap.
   * @param index The index, either from tryStoreKey or from tryStoreVectorizedKey result.
   * @param value The value to store.
   * @param keyHash The key hash to store.
   * @param vectorized Whether the result is coming from a vectorized batch.
   */
  public void storeValue(int index, BytesWritable value, int keyHash, boolean vectorized) {
    values[index] = Arrays.copyOf(value.getBytes(), value.getLength());
    hashes[index] = keyHash;
    // Vectorized doesn't adjust usage for the keys while processing the batch
    usage += values[index].length + (vectorized ? keys[index].length : 0);
  }

  /**
   * Flushes all the rows cached in the heap.
   */
  public void flush() throws HiveException {
    if (!isEnabled || (topN == 0)) return;
    try {
      flushInternal();
    } catch (IOException ex) {
      throw new HiveException(ex);
    }
  }

  /**
   * returns index for key/value/hashcode if it's acceptable.
   * -1, -2, -3, -4 can be returned for other actions.
   * <p/>
   * -1 for FORWARD   : should be forwarded to output collector (for GBY)
   * -2 for EXCLUDED  : not in top-k. ignore it
   */
  private int insertKeyIntoHeap(HiveKey key) throws IOException, HiveException {
    if (usage > threshold) {
      flushInternal();
      if (excluded == 0) {
        LOG.info("Top-N hash is disabled");
        isEnabled = false;
      }
      // we can now retry adding key/value into hash, which is flushed.
      // but for simplicity, just forward them
      return FORWARD;
    }
    int size = indexes.size();
    int index = size < topN ? size : evicted;
    keys[index] = Arrays.copyOf(key.getBytes(), key.getLength());
    distKeyLengths[index] = key.getDistKeyLength();
    if (null != indexes.store(index)) {
      // it's only for GBY which should forward all values associated with the key in the range
      // of limit. new value should be attatched with the key but in current implementation,
      // only one values is allowed. with map-aggreagtion which is true by default,
      // this is not common case, so just forward new key/value and forget that (todo)
      return FORWARD;
    }
    if (size == topN) {
      evicted = indexes.removeBiggest();  // remove the biggest key
      if (index == evicted) {
        excluded++;
        return EXCLUDE;          // input key is bigger than any of keys in hash
      }
      removed(evicted);
    }
    return index;
  }

  // key/value of the index is removed. retrieve memory usage
  private void removed(int index) {
    usage -= keys[index].length;
    keys[index] = null;
    if (values[index] != null) {
      usage -= values[index].length;
      values[index] = null;
    }
    hashes[index] = -1;
    distKeyLengths[index] = -1;
  }

  private void flushInternal() throws IOException, HiveException {
    for (int index : indexes.indexes()) {
      if (index != evicted && values[index] != null) {
        collector.collect(keys[index], values[index], hashes[index]);
        usage -= values[index].length;
        values[index] = null;
        hashes[index] = -1;
      }
    }
    excluded = 0;
  }

  private interface IndexStore {
    int size();
    /**
     * @return the index which caused the item to be rejected; or null if accepted
     */
    Integer store(int index);
    int removeBiggest();
    Iterable<Integer> indexes();
  }

  /**
   * for order by, same keys are counted (For 1-2-2-3-4, limit 3 is 1-2-2)
   * MinMaxPriorityQueue is used because it alows duplication and fast access to biggest one
   */
  private class HashForRow implements IndexStore {
    private final MinMaxPriorityQueue<Integer> indexes = MinMaxPriorityQueue.orderedBy(C).create();

    public int size() {
      return indexes.size();
    }

    // returns null always
    public Integer store(int index) {
      boolean result = indexes.add(index);
      assert result;
      return null;
    }

    public int removeBiggest() {
      return indexes.removeLast();
    }

    public Iterable<Integer> indexes() {
      Integer[] array = indexes.toArray(new Integer[indexes.size()]);
      Arrays.sort(array, 0, array.length, C);
      return Arrays.asList(array);
    }
  }

  /**
   * for group by, same keys are not counted (For 1-2-2-3-4, limit 3 is 1-2-(2)-3)
   * simple TreeMap is used because group by does not need keep duplicated keys
   */
  private class HashForGroup implements IndexStore {
    // TreeSet anyway uses TreeMap; so use plain TreeMap to be able to get value in collisions.
    private final TreeMap<Integer, Integer> indexes = new TreeMap<Integer, Integer>(C);

    public int size() {
      return indexes.size();
    }

    // returns false if index already exists in map
    public Integer store(int index) {
      return indexes.put(index, index);
    }

    public int removeBiggest() {
      Integer last = indexes.lastKey();
      indexes.remove(last);
      return last;
    }

    public Iterable<Integer> indexes() {
      return indexes.keySet();
    }
  }
}
