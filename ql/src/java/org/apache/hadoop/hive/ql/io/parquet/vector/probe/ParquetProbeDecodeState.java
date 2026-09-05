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

import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.TableScanOperator.ProbeDecodeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinTableContainer;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Runtime state for the Parquet ProbeDecode path: given a {@link MapWork} that carries a
 * {@link ProbeDecodeContext}, resolve the small-side hash table from the {@link ObjectCache} and
 * wrap it in a {@link ParquetProbeHashTable} so the vectorized Parquet reader can apply a probe
 * filter on every batch.
 *
 * <p>The state is intentionally null-safe: any failure to resolve (no probe context, cache miss,
 * unsupported key type) produces a {@link #disabled()} instance whose {@link #isEnabled()} is
 * {@code false}, and the reader falls back to the original decode path unchanged.
 *
 * <p>The MVP handles only single-long / int keys through {@link ParquetProbeLongHashTable}.
 * Bytes-key and multi-key variants will land in follow-up commits and plug in via {@link #of}.
 */
public final class ParquetProbeDecodeState {

  private static final Logger LOG = LoggerFactory.getLogger(ParquetProbeDecodeState.class);

  private static final ParquetProbeDecodeState DISABLED = new ParquetProbeDecodeState(-1, null);

  private final int keyColumnIndex;
  private final ParquetProbeHashTable probe;

  private ParquetProbeDecodeState(int keyColumnIndex, ParquetProbeHashTable probe) {
    this.keyColumnIndex = keyColumnIndex;
    this.probe = probe;
  }

  public static ParquetProbeDecodeState disabled() {
    return DISABLED;
  }

  /**
   * Resolve probe state from the current job's {@link MapWork}. Returns {@link #disabled()} if
   * anything is missing (no probe context, unknown key column, missing hash table, non-long key)
   * so the caller can unconditionally consult {@link #isEnabled()} without special-casing.
   *
   * @param conf              the job conf
   * @param mapWork           the MapWork, whose {@code probeDecodeContext} names the small table
   * @param projectedColumns  the projected column names in reader order -- used to locate the
   *                          probe key column within the batch
   */
  public static ParquetProbeDecodeState of(Configuration conf, MapWork mapWork,
      List<String> projectedColumns) {
    if (mapWork == null) {
      return DISABLED;
    }
    ProbeDecodeContext ctx = mapWork.getProbeDecodeContext();
    if (ctx == null) {
      return DISABLED;
    }
    String keyCol = ctx.getMjBigTableKeyColName();
    if (keyCol == null || projectedColumns == null) {
      return DISABLED;
    }
    int keyIdx = projectedColumns.indexOf(keyCol);
    if (keyIdx < 0) {
      // The probe key column isn't in the projected schema for this reader -- e.g. because
      // column pruning already dropped it. Nothing to probe against; run un-probed.
      LOG.debug("ProbeDecode: probe key column {} not in projected columns {}",
          keyCol, projectedColumns);
      return DISABLED;
    }

    String queryId = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_QUERY_ID);
    try {
      ObjectCache cache = ObjectCacheFactory.getCache(conf, queryId, false);
      // MapJoinOperator.initializeOp stores the hash table under either the raw MapJoinDesc cache
      // key (when conf's cacheKey was null at compile time) or `cacheKey + "_" + concreteOpClass`
      // (when Shared Work Optimization / ProbeDecode compilation set the cache key upfront). The
      // ProbeDecodeContext only carries the raw key -- resolve the concrete-class suffix by
      // matching the MapJoinOperator in the MapWork tree, mirroring the loader's key exactly.
      String actualKey = resolveActualCacheKey(mapWork, ctx.getMjSmallTableCacheKey());
      Object cached = cache.retrieve(actualKey);
      if (cached == null) {
        LOG.debug("ProbeDecode: no cached hash table for key {} (base {})",
            actualKey, ctx.getMjSmallTableCacheKey());
        return DISABLED;
      }
      // MapJoinOperator caches the loaded hash tables as a
      // `Pair<MapJoinTableContainer[], MapJoinTableContainerSerDe[]>` -- unwrap and pick the
      // small-table container by its position.
      MapJoinTableContainer container = unwrapContainer(cached, ctx.getMjSmallTablePos());
      if (container == null) {
        LOG.debug("ProbeDecode: could not extract small-table container from cached {} (pos {})",
            cached.getClass().getName(), ctx.getMjSmallTablePos());
        return DISABLED;
      }
      VectorMapJoinHashTable ht;
      if (container instanceof VectorMapJoinTableContainer) {
        ht = ((VectorMapJoinTableContainer) container).vectorMapJoinHashTable();
      } else {
        // Non-vectorized container -- probe-decode has no dictionary-of-keys to intersect with,
        // so bail out and let the plain decode path run.
        LOG.debug("ProbeDecode: cached container is non-vectorized ({}); skipping probe",
            container.getClass().getName());
        return DISABLED;
      }

      if (ht instanceof VectorMapJoinLongHashTable) {
        LOG.info("ProbeDecode: enabled for key column {} (idx {}) via {}",
            keyCol, keyIdx, ht.getClass().getSimpleName());
        return new ParquetProbeDecodeState(keyIdx, ParquetProbeLongHashTable.of(ht));
      }
      // TODO: bytes-key + multi-key variants when the corresponding ParquetProbeHashTable
      // implementations land.
      LOG.debug("ProbeDecode: no ParquetProbeHashTable for key type {}", ht.getClass().getName());
      return DISABLED;
    } catch (Exception e) {
      LOG.warn("ProbeDecode: hash-table resolution failed, falling back to plain decode", e);
      return DISABLED;
    }
  }

  /**
   * Peel a cached hash-table entry stored by {@link MapJoinOperator#loadHashTable} back to the
   * small-side container. The op caches a {@code Pair<MapJoinTableContainer[], ...>}; some code
   * paths (older tests, refactored fixtures) store a bare container or a bare hash table -- keep
   * those working too.
   */
  private static MapJoinTableContainer unwrapContainer(Object cached, byte smallPos) {
    if (cached instanceof Pair) {
      Object left = ((Pair<?, ?>) cached).getLeft();
      if (left instanceof MapJoinTableContainer[]) {
        MapJoinTableContainer[] tables = (MapJoinTableContainer[]) left;
        if (smallPos >= 0 && smallPos < tables.length && tables[smallPos] != null) {
          return tables[smallPos];
        }
        // Fall back to the first non-null entry -- some plans leave big-table slots null.
        for (MapJoinTableContainer t : tables) {
          if (t != null) {
            return t;
          }
        }
      }
      return null;
    }
    if (cached instanceof MapJoinTableContainer) {
      return (MapJoinTableContainer) cached;
    }
    return null;
  }

  /**
   * Mirror {@code MapJoinOperator.initializeOp}'s cacheKey computation so our lookup finds the
   * hash table the loader stored. When the ProbeDecodeContext's raw cacheKey is non-null, the
   * operator appends {@code "_" + this.getClass().getName()} -- and the vectorizer has by now
   * substituted the {@link MapJoinOperator} with a concrete {@code VectorMapJoin*Operator}
   * subclass, so we read the class off the operator in the MapWork tree.
   */
  static String resolveActualCacheKey(MapWork mapWork, String baseCacheKey) {
    if (baseCacheKey == null || mapWork == null) {
      return baseCacheKey;
    }
    try {
      Set<MapJoinOperator> mjs = OperatorUtils.findOperators(mapWork.getWorks(), MapJoinOperator.class);
      for (MapJoinOperator mj : mjs) {
        if (mj.getConf() != null && baseCacheKey.equals(mj.getConf().getCacheKey())) {
          return baseCacheKey + "_" + mj.getClass().getName();
        }
      }
    } catch (Exception e) {
      LOG.debug("ProbeDecode: could not resolve concrete cacheKey suffix, falling back to raw key",
          e);
    }
    return baseCacheKey;
  }

  public boolean isEnabled() {
    return probe != null;
  }

  /** @return column index (within the reader's projected columns) of the probe key column. */
  public int getKeyColumnIndex() {
    return keyColumnIndex;
  }

  public ParquetProbeHashTable getProbe() {
    return probe;
  }
}
