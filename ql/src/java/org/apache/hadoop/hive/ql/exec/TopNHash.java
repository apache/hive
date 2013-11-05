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

  public static final int FORWARD = -1;
  public static final int EXCLUDED = -2;
  private static final int FLUSH = -3;
  private static final int DISABLE = -4;
  private static final int MAY_FORWARD = -5;

  private BinaryCollector collector;
  private int topN;

  private long threshold;   // max heap size
  private long usage;

  // binary keys, values and hashCodes of rows, lined up by index
  private byte[][] keys;
  private byte[][] values;
  private int[] hashes;
  private IndexStore indexes; // The heap over the keys, storing indexes in the array.

  private int evicted; // recently evicted index (used for next key/value)
  private int excluded; // count of excluded rows from previous flush

  // temporary stuff used for vectorization
  private int batchNumForwards = 0; // whether current batch has any forwarded keys
  private int[] indexToBatchIndex; // mapping of index (lined up w/keys) to index in the batch

  private boolean isEnabled = false;

  private final Comparator<Integer> C = new Comparator<Integer>() {
    public int compare(Integer o1, Integer o2) {
      byte[] key1 = keys[o1];
      byte[] key2 = keys[o2];
      return WritableComparator.compareBytes(key1, 0, key1.length, key2, 0, key2.length);
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
  public int tryStoreKey(BytesWritable key) throws HiveException, IOException {
    if (!isEnabled) {
      return FORWARD; // short-circuit quickly - forward all rows
    }
    if (topN == 0) {
      return EXCLUDED; // short-circuit quickly - eat all rows
    }
    int index = insertKeyIntoHeap(key);
    if (index >= 0) {
      usage += key.getLength();
      return index;
    }
    // IndexStore is trying to tell us something.
    switch (index) {
      case DISABLE: {
        LOG.info("Top-N hash is disabled");
        flushInternal();
        isEnabled = false;
        return FORWARD;
      }
      case FLUSH: {
        LOG.info("Top-N hash is flushed");
        flushInternal();
        // we can now retry adding key/value into hash, which is flushed.
        // but for simplicity, just forward them
        return FORWARD;
      }
      case FORWARD:  return FORWARD;
      case EXCLUDED: return EXCLUDED; // skip the row.
      default: {
        assert false;
        throw new HiveException("Invalid result trying to store the key: " + index);
      }
    }
  }


  /**
   * Perform basic checks and initialize TopNHash for the new vectorized row batch.
   * @return TopNHash.FORWARD if all rows should be forwarded w/o trying to call TopN;
   *         TopNHash.EXCLUDED if all rows should be discarded w/o trying to call TopN;
   *         any other result means the batch has been started.
   */
  public int startVectorizedBatch() throws IOException, HiveException {
    if (!isEnabled) {
      return FORWARD; // short-circuit quickly - forward all rows
    } else if (topN == 0) {
      return EXCLUDED; // short-circuit quickly - eat all rows
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
    if (indexToBatchIndex == null) {
      indexToBatchIndex = new int[topN + 1]; // for current batch, contains key index in the batch
    }
    Arrays.fill(indexToBatchIndex, -1);
    batchNumForwards = 0;
    return 0;
  }

  /**
   * Try to put the key from the current vectorized batch into the heap.
   * @param key the key.
   * @param batchIndex The index of the key in the vectorized batch (sequential, not .selected).
   * @param results The results; the number of elements equivalent to vrg.size, by kindex.
   *   The result should be the same across the calls for the batch; in then end, for each k-index:
   *     - TopNHash.EXCLUDED - discard the row.
   *     - positive index - store the row using storeValue, same as tryStoreRow.
   *     - negative index - forward the row. getVectorizedKeyToForward called w/this index will
   *        return the key to use so it doesn't have to be rebuilt.
   */
  public void tryStoreVectorizedKey(BytesWritable key, int batchIndex, int[] results)
          throws HiveException, IOException {
    // Assumption - batchIndex is increasing; startVectorizedBatch was called
    int size = indexes.size();
    int index = size < topN ? size : evicted;
    keys[index] = Arrays.copyOf(key.getBytes(), key.getLength());
    Integer collisionIndex = indexes.store(index);
    if (null != collisionIndex) {
      // forward conditional on the survival of the corresponding key currently in indexes.
      ++batchNumForwards;
      results[batchIndex] = MAY_FORWARD - collisionIndex;
      return;
    }
    indexToBatchIndex[index] = batchIndex;
    results[batchIndex] = index;
    if (size != topN) return;
    evicted = indexes.removeBiggest();  // remove the biggest key
    if (index == evicted) {
      excluded++;
      results[batchIndex] = EXCLUDED;
      indexToBatchIndex[index] = -1;
      return; // input key is bigger than any of keys in hash
    }
    removed(evicted);
    int evictedBatchIndex = indexToBatchIndex[evicted];
    if (evictedBatchIndex >= 0) {
      // reset the result for the evicted index
      results[evictedBatchIndex] = EXCLUDED;
      indexToBatchIndex[evicted] = -1;
    }
    // Also evict all results grouped with this index; cannot be current key or before it.
    if (batchNumForwards > 0) {
      int evictedForward = (MAY_FORWARD - evicted);
      boolean forwardRemoved = false;
      for (int i = evictedBatchIndex + 1; i < batchIndex; ++i) {
        if (results[i] == evictedForward) {
          results[i] = EXCLUDED;
          forwardRemoved = true;
        }
      }
      if (forwardRemoved) {
        --batchNumForwards;
      }
    }
  }

  /**
   * After vectorized batch is processed, can return the key that caused a particular row
   * to be forwarded. Because the row could only be marked to forward because it has
   * the same key with some row already in the heap (for GBY), we can use that key from the
   * heap to emit the forwarded row.
   * @param index Negative index from the vectorized result. See tryStoreVectorizedKey.
   * @return The key corresponding to the row.
   */
  public byte[] getVectorizedKeyToForward(int index) {
    assert index <= MAY_FORWARD;
    return keys[MAY_FORWARD - index];
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
   * -3 for FLUSH     : memory is not enough. flush values (keep keys only)
   * -4 for DISABLE   : hash is not effective. flush and disable it
   */
  private int insertKeyIntoHeap(BinaryComparable key) {
    if (usage > threshold) {
      return excluded == 0 ? DISABLE : FLUSH;
    }
    int size = indexes.size();
    int index = size < topN ? size : evicted;
    keys[index] = Arrays.copyOf(key.getBytes(), key.getLength());
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
        return EXCLUDED;          // input key is bigger than any of keys in hash
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
