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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.ql.io.parquet.vector.probe;

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.JoinUtil;
import org.apache.hadoop.hive.ql.exec.vector.ColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMapResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashMultiSetResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashSetResult;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMap;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashMultiSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashTable;

/**
 * Probe a {@link LongColumnVector} against a single-long-key
 * {@link org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashTable}.
 *
 * <p>Covers the three concrete long-key hash table flavours emitted by Hive for a mapjoin:
 * <ul>
 *   <li>hash map (inner / left-outer with payload)</li>
 *   <li>hash multi-set (semijoin with duplicates)</li>
 *   <li>hash set (semijoin)</li>
 * </ul>
 * All three funnel their probe result through {@code JoinUtil.JoinResult}; a row survives when
 * the probe returns {@code MATCH} (and, for a multi-set/set, {@code SPILL} which we treat as a
 * hit to be safe -- we prefer over-decoding to under-decoding).
 *
 * <p>The probe iterates every row exactly once, so it is O(batchSize). NULL keys never match,
 * matching Hive's join semantics.
 */
public final class ParquetProbeLongHashTable implements ParquetProbeHashTable {

  private enum Kind { MAP, MULTISET, SET }

  private final Kind kind;
  private final VectorMapJoinLongHashMap map;
  private final VectorMapJoinLongHashMultiSet multiSet;
  private final VectorMapJoinLongHashSet set;
  private final boolean useMinMax;
  private final long min;
  private final long max;
  private final VectorMapJoinHashMapResult mapResult;
  private final VectorMapJoinHashMultiSetResult multiSetResult;
  private final VectorMapJoinHashSetResult setResult;

  public static ParquetProbeLongHashTable of(VectorMapJoinHashTable ht) {
    if (ht instanceof VectorMapJoinLongHashMap) {
      return new ParquetProbeLongHashTable(Kind.MAP, (VectorMapJoinLongHashTable) ht);
    }
    if (ht instanceof VectorMapJoinLongHashMultiSet) {
      return new ParquetProbeLongHashTable(Kind.MULTISET, (VectorMapJoinLongHashTable) ht);
    }
    if (ht instanceof VectorMapJoinLongHashSet) {
      return new ParquetProbeLongHashTable(Kind.SET, (VectorMapJoinLongHashTable) ht);
    }
    throw new IllegalArgumentException("Not a long-key hash table: " + ht.getClass().getName());
  }

  private ParquetProbeLongHashTable(Kind kind, VectorMapJoinLongHashTable longHT) {
    this.kind = kind;
    this.useMinMax = longHT.useMinMax();
    this.min = longHT.min();
    this.max = longHT.max();
    if (kind == Kind.MAP) {
      this.map = (VectorMapJoinLongHashMap) longHT;
      this.multiSet = null;
      this.set = null;
      this.mapResult = map.createHashMapResult();
      this.multiSetResult = null;
      this.setResult = null;
    } else if (kind == Kind.MULTISET) {
      this.map = null;
      this.multiSet = (VectorMapJoinLongHashMultiSet) longHT;
      this.set = null;
      this.mapResult = null;
      this.multiSetResult = multiSet.createHashMultiSetResult();
      this.setResult = null;
    } else {
      this.map = null;
      this.multiSet = null;
      this.set = (VectorMapJoinLongHashSet) longHT;
      this.mapResult = null;
      this.multiSetResult = null;
      this.setResult = set.createHashSetResult();
    }
  }

  @Override
  public ParquetProbeFilter probe(ColumnVector keyColumn, int batchSize) throws IOException {
    if (!(keyColumn instanceof LongColumnVector)) {
      throw new IllegalArgumentException(
          "Expected LongColumnVector for long-key probe, got " + keyColumn.getClass().getName());
    }
    LongColumnVector v = (LongColumnVector) keyColumn;
    boolean[] bitmap = new boolean[batchSize];

    if (v.isRepeating) {
      // Single-value fast path: probe once, splat the result over the whole batch. Cheap dictionary
      // encoded columns land here often.
      boolean nullKey = !v.noNulls && v.isNull[0];
      boolean hit = !nullKey && probeOne(v.vector[0]);
      if (hit) {
        Arrays.fill(bitmap, true);
      }
      // else all filtered out; bitmap already false
      return ParquetProbeFilter.newBitmap(bitmap);
    }

    for (int i = 0; i < batchSize; i++) {
      if (!v.noNulls && v.isNull[i]) {
        continue; // NULL keys never match
      }
      long key = v.vector[i];
      if (useMinMax && (key < min || key > max)) {
        continue; // small-table min/max exclusion -- pure arithmetic, no hash-table touch
      }
      bitmap[i] = probeOne(key);
    }
    return ParquetProbeFilter.newBitmap(bitmap);
  }

  private boolean probeOne(long key) throws IOException {
    JoinUtil.JoinResult r;
    switch (kind) {
      case MAP:
        r = map.lookup(key, mapResult);
        break;
      case MULTISET:
        r = multiSet.contains(key, multiSetResult);
        break;
      case SET:
        r = set.contains(key, setResult);
        break;
      default:
        throw new AssertionError(kind);
    }
    // Treat SPILL as a hit: if the small-table row is on a spilled partition we can't know
    // whether it matches without going to disk, so we let the row through and let the join
    // operator handle it downstream -- prefer over-decoding to filtering a valid row.
    return r == JoinUtil.JoinResult.MATCH || r == JoinUtil.JoinResult.SPILL;
  }
}
