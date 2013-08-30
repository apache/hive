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
import java.util.Collections;
import java.util.Comparator;
import java.util.SortedSet;
import java.util.TreeSet;

import com.google.common.collect.MinMaxPriorityQueue;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.WritableComparator;
import org.apache.hadoop.mapred.OutputCollector;

/**
 * Stores binary key/value in sorted manner to get top-n key/value
 */
abstract class TopNHash {

  /**
   * For interaction between operator and top-n hash.
   * Currently only used to forward key/values stored in hash.
   */
  public static interface BinaryCollector extends OutputCollector<BytesWritable, BytesWritable> {
  }

  protected static final int FORWARD = -1;
  protected static final int EXCLUDED = -2;
  protected static final int FLUSH = -3;
  protected static final int DISABLE = -4;

  protected final int topN;
  protected final BinaryCollector collector;

  protected final long threshold;   // max heap size
  protected long usage;             // heap usage (not exact)

  // binary keys, binary values and hashcodes of keys, lined up by index
  protected final byte[][] keys;
  protected final byte[][] values;
  protected final int[] hashes;

  protected int evicted;    // recetly evicted index (the biggest one. used for next key/value)
  protected int excluded;   // count of excluded rows from previous flush

  protected final Comparator<Integer> C = new Comparator<Integer>() {
    public int compare(Integer o1, Integer o2) {
      byte[] key1 = keys[o1];
      byte[] key2 = keys[o2];
      return WritableComparator.compareBytes(key1, 0, key1.length, key2, 0, key2.length);
    }
  };

  public static TopNHash create0() {
    return new HashForLimit0();
  }

  public static TopNHash create(boolean grouped, int topN, long threshold,
      BinaryCollector collector) {
    if (topN == 0) {
      return new HashForLimit0();
    }
    if (grouped) {
      return new HashForGroup(topN, threshold, collector);
    }
    return new HashForRow(topN, threshold, collector);
  }

  TopNHash(int topN, long threshold, BinaryCollector collector) {
    this.topN = topN;
    this.threshold = threshold;
    this.collector = collector;
    this.keys = new byte[topN + 1][];
    this.values = new byte[topN + 1][];
    this.hashes = new int[topN + 1];
    this.evicted = topN;
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
  public int indexOf(HiveKey key) {
    int size = size();
    if (usage > threshold) {
      return excluded == 0 ? DISABLE : FLUSH;
    }
    int index = size < topN ? size : evicted;
    keys[index] = Arrays.copyOf(key.getBytes(), key.getLength());
    hashes[index] = key.hashCode();
    if (!store(index)) {
      // it's only for GBY which should forward all values associated with the key in the range
      // of limit. new value should be attatched with the key but in current implementation,
      // only one values is allowed. with map-aggreagtion which is true by default,
      // this is not common case, so just forward new key/value and forget that (todo)
      return FORWARD;
    }
    if (size == topN) {
      evicted = removeBiggest();  // remove the biggest key
      if (index == evicted) {
        excluded++;
        return EXCLUDED;          // input key is bigger than any of keys in hash
      }
      removed(evicted);
    }
    return index;
  }

  protected abstract int size();

  protected abstract boolean store(int index);

  protected abstract int removeBiggest();

  protected abstract Iterable<Integer> indexes();

  // key/value of the index is removed. retrieve memory usage
  public void removed(int index) {
    usage -= keys[index].length;
    keys[index] = null;
    if (values[index] != null) {
      // value can be null if hash is flushed, which only keeps keys for limiting rows
      usage -= values[index].length;
      values[index] = null;
    }
    hashes[index] = -1;
  }

  public void set(int index, BytesWritable value) {
    values[index] = Arrays.copyOf(value.getBytes(), value.getLength());
    usage += keys[index].length + values[index].length;
  }

  public void flush() throws IOException {
    for (int index : indexes()) {
      flush(index);
    }
    excluded = 0;
  }

  protected void flush(int index) throws IOException {
    if (index != evicted && values[index] != null) {
      // BytesWritable copies array for set method. So just creats new one
      HiveKey keyWritable = new HiveKey(keys[index], hashes[index]);
      BytesWritable valueWritable = new BytesWritable(values[index]);
      collector.collect(keyWritable, valueWritable);
      usage -= values[index].length;
      values[index] = null;
    }
  }
}

/**
 * for order by, same keys are counted (For 1-2-2-3-4, limit 3 is 1-2-2)
 * MinMaxPriorityQueue is used because it alows duplication and fast access to biggest one
 */
class HashForRow extends TopNHash {

  private final MinMaxPriorityQueue<Integer> indexes;

  HashForRow(int topN, long threshold, BinaryCollector collector) {
    super(topN, threshold, collector);
    this.indexes = MinMaxPriorityQueue.orderedBy(C).create();
  }

  protected int size() {
    return indexes.size();
  }

  // returns true always
  protected boolean store(int index) {
    return indexes.add(index);
  }

  protected int removeBiggest() {
    return indexes.removeLast();
  }

  protected Iterable<Integer> indexes() {
    Integer[] array = indexes.toArray(new Integer[indexes.size()]);
    Arrays.sort(array, 0, array.length, C);
    return Arrays.asList(array);
  }
}

/**
 * for group by, same keys are not counted (For 1-2-2-3-4, limit 3 is 1-2-(2)-3)
 * simple TreeMap is used because group by does not need keep duplicated keys
 */
class HashForGroup extends TopNHash {

  private final SortedSet<Integer> indexes;

  HashForGroup(int topN, long threshold, BinaryCollector collector) {
    super(topN, threshold, collector);
    this.indexes = new TreeSet<Integer>(C);
  }

  protected int size() {
    return indexes.size();
  }

  // returns false if index already exists in map
  protected boolean store(int index) {
    return indexes.add(index);
  }

  protected int removeBiggest() {
    Integer last = indexes.last();
    indexes.remove(last);
    return last;
  }

  protected Iterable<Integer> indexes() {
    return indexes;
  }
}

class HashForLimit0 extends TopNHash {

  HashForLimit0() {
    super(0, 0, null);
  }

  @Override
  public int indexOf(HiveKey key) {
    return EXCLUDED;
  }

  protected int size() { return 0; }
  protected boolean store(int index) { return false; }
  protected int removeBiggest() { return 0; }
  protected Iterable<Integer> indexes() { return Collections.emptyList(); }
}

