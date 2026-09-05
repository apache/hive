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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.UUID;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator.ProbeDecodeContext;
import org.apache.hadoop.hive.ql.exec.persistence.MapJoinTableContainer;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashTable;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinLongHashSet;
import org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinTableContainer;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Regression coverage for the cacheKey-suffix contract between {@link MapJoinOperator} (the writer
 * that seeds the {@link ObjectCache}) and {@link ParquetProbeDecodeState#of} (the reader that
 * looks it up). The vectorized Parquet reader must reproduce the loader's key exactly, else it
 * looks up under the raw base key, finds nothing, and silently disables probe-decode -- the
 * pre-fix behaviour of HIVE-30019.
 *
 * <p>{@link MapJoinOperator#initializeOp} computes:
 * <pre>{@code cacheKey = conf.getCacheKey() + "_" + this.getClass().getName();}</pre>
 * The {@link ProbeDecodeContext} that Tez compilation writes into {@link MapWork} carries only
 * the raw {@code conf.getCacheKey()} half. {@link ParquetProbeDecodeState#resolveActualCacheKey}
 * closes that gap by finding the concrete {@link MapJoinOperator} subclass in the
 * {@code MapWork} tree at read time and appending its class name.
 *
 * <p>These tests pin both halves of the contract:
 * <ul>
 *   <li>{@link #resolveActualCacheKeyAppendsConcreteClass()} — the resolver returns
 *       {@code base + "_" + <concrete class name>} when {@code MapJoinOperator}'s
 *       {@code MapJoinDesc.cacheKey} equals {@code base}. If a future change removes the suffix
 *       from either side without updating the other, this test fails loudly.</li>
 *   <li>{@link #ofResolvesSeededHashTableUnderSuffixedCacheKey()} — end-to-end: seed the
 *       {@link ObjectCache} under the fully-suffixed key, call {@link ParquetProbeDecodeState#of},
 *       and assert it comes back enabled with the correct key column index.</li>
 *   <li>{@link #ofDisabledWhenOnlyRawKeyPresent()} — negative: seed the same cache under the raw
 *       base only. If the resolver ever silently reverts to the raw key, this test flips to green
 *       instead of red, but paired with the seeded suffixed-key test it still traps the
 *       regression: the reader is finding the value only because it looked up the suffixed key.</li>
 * </ul>
 */
public class TestParquetProbeDecodeState {

  private static final String BASE_CACHE_KEY = "HASH_MAP_MAPJOIN_25_container";
  private static final byte SMALL_TABLE_POS = (byte) 1;
  private static final String KEY_COL = "key2";
  private static final List<String> PROJECTED_COLS = Arrays.asList("nokey", KEY_COL, "dt");

  private HiveConf conf;
  private String queryId;
  private boolean priorIsDaemon;

  @Before
  public void before() {
    // Route ObjectCacheFactory through the LLAP daemon branch so a single LlapObjectCache instance
    // is shared for both our seed and the state's lookup (the MR branch hands out a fresh cache
    // on every getCache call, so the seed would never be visible to the state).
    priorIsDaemon = LlapProxy.isDaemon();
    LlapProxy.setDaemon(true);

    conf = new HiveConf();
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    conf.setBoolVar(HiveConf.ConfVars.LLAP_OBJECT_CACHE_ENABLED, true);
    queryId = "test-probe-decode-" + UUID.randomUUID();
    conf.setVar(HiveConf.ConfVars.HIVE_QUERY_ID, queryId);
  }

  @After
  public void after() {
    ObjectCacheFactory.removeLlapQueryCache(queryId);
    LlapProxy.setDaemon(priorIsDaemon);
  }

  /**
   * Pin the resolver in isolation: given a {@link MapWork} whose {@link MapJoinOperator}'s
   * {@code MapJoinDesc.cacheKey} equals {@code base}, {@code resolveActualCacheKey} must return
   * {@code base + "_" + <concrete MapJoinOperator subclass name>}, matching what
   * {@link MapJoinOperator#initializeOp} would have stored.
   *
   * <p>Uses the plain {@code MapJoinOperator} class here — the vectorizer swaps it for a
   * concrete {@code VectorMapJoin*Operator} subclass at runtime, but the resolver only reads
   * {@code Class#getName()} off whatever operator is in the tree, so this is representative.
   */
  @Test
  public void resolveActualCacheKeyAppendsConcreteClass() {
    MapWork mapWork = mapWorkWith(mapJoinOperatorWithCacheKey(BASE_CACHE_KEY));

    String resolved = ParquetProbeDecodeState.resolveActualCacheKey(mapWork, BASE_CACHE_KEY);

    assertEquals(BASE_CACHE_KEY + "_" + MapJoinOperator.class.getName(), resolved);
  }

  /**
   * End-to-end: seed the ObjectCache under the fully-suffixed key that
   * {@link MapJoinOperator#initializeOp} would store under, then call
   * {@link ParquetProbeDecodeState#of}. The state must resolve the suffix, find the entry, unwrap
   * the {@link ImmutablePair} to the small-side container, and expose an enabled probe with the
   * key column at index 1 (the position of {@code KEY_COL} in {@code PROJECTED_COLS}).
   */
  @Test
  public void ofResolvesSeededHashTableUnderSuffixedCacheKey() throws Exception {
    MapWork mapWork = mapWorkWith(mapJoinOperatorWithCacheKey(BASE_CACHE_KEY));
    seedObjectCache(BASE_CACHE_KEY + "_" + MapJoinOperator.class.getName(),
        pairWithSmallContainerAtPos(SMALL_TABLE_POS));
    mapWork.setProbeDecodeContext(
        new ProbeDecodeContext(BASE_CACHE_KEY, SMALL_TABLE_POS, KEY_COL, 1.0));

    ParquetProbeDecodeState state = ParquetProbeDecodeState.of(conf, mapWork, PROJECTED_COLS);

    assertTrue("probe-decode should be enabled once the suffixed key resolves",
        state.isEnabled());
    assertEquals("probe key column index must match position in projected columns",
        PROJECTED_COLS.indexOf(KEY_COL), state.getKeyColumnIndex());
  }

  /**
   * Negative case: the {@link ObjectCache} carries an entry only under the raw base key -- no
   * suffix. This is the pre-fix layout (and also what happens if a future refactor accidentally
   * strips the suffix from {@link MapJoinOperator#initializeOp}). The state must report
   * {@code isEnabled() == false} rather than picking up the un-suffixed entry, keeping the
   * suffixed-key path the sole lookup mechanism.
   */
  @Test
  public void ofDisabledWhenOnlyRawKeyPresent() throws Exception {
    MapWork mapWork = mapWorkWith(mapJoinOperatorWithCacheKey(BASE_CACHE_KEY));
    seedObjectCache(BASE_CACHE_KEY, pairWithSmallContainerAtPos(SMALL_TABLE_POS));
    mapWork.setProbeDecodeContext(
        new ProbeDecodeContext(BASE_CACHE_KEY, SMALL_TABLE_POS, KEY_COL, 1.0));

    ParquetProbeDecodeState state = ParquetProbeDecodeState.of(conf, mapWork, PROJECTED_COLS);

    assertFalse("state must not resolve when only the raw (unsuffixed) key is cached",
        state.isEnabled());
  }

  private static MapJoinOperator mapJoinOperatorWithCacheKey(String cacheKey) {
    MapJoinOperator mj = new MapJoinOperator(new CompilationOpContext());
    MapJoinDesc desc = new MapJoinDesc();
    desc.setCacheKey(cacheKey);
    mj.setConf(desc);
    return mj;
  }

  private static MapWork mapWorkWith(MapJoinOperator mj) {
    MapWork mapWork = new MapWork();
    LinkedHashMap<String, Operator<? extends OperatorDesc>> aliasToWork = new LinkedHashMap<>();
    // OperatorUtils#findOperators walks child operators from each entry in aliasToWork, and also
    // checks the entry itself -- putting the MapJoinOperator directly in the map is enough for
    // the resolver, and avoids the full TS → RS → MJ chain a real plan would carry.
    aliasToWork.put("test-alias", mj);
    mapWork.setAliasToWork(aliasToWork);
    return mapWork;
  }

  private void seedObjectCache(String key, Object value) throws Exception {
    ObjectCache cache = ObjectCacheFactory.getCache(conf, queryId, false);
    // ObjectCache exposes no public put(); retrieve(key, Callable) stores the callable's result
    // when the key is absent, which is what the loader effectively does too.
    cache.retrieve(key, () -> value);
  }

  private static ImmutablePair<MapJoinTableContainer[], Object> pairWithSmallContainerAtPos(
      byte smallPos) {
    // The pre-fix bug was that the cached object is a Pair whose left slot is a
    // MapJoinTableContainer[] indexed by the mapjoin position -- mirror that exact shape so the
    // state's unwrap logic is exercised, not shortcut.
    MapJoinTableContainer[] tables = new MapJoinTableContainer[Math.max(2, smallPos + 1)];
    tables[smallPos] = smallSideVectorContainer();
    return ImmutablePair.of(tables, /* serdes, unused by the state */ null);
  }

  private static VectorMapJoinTableContainer smallSideVectorContainer() {
    VectorMapJoinLongHashSet ht = mock(VectorMapJoinLongHashSet.class);
    // ParquetProbeLongHashTable's constructor reads useMinMax / min / max and calls
    // createHashSetResult on the SET branch -- give it just enough to run without NPE.
    when(ht.useMinMax()).thenReturn(false);
    when(ht.createHashSetResult()).thenReturn(new StubHashSetResult());
    VectorMapJoinTableContainer c = mock(VectorMapJoinTableContainer.class);
    when(c.vectorMapJoinHashTable()).thenReturn((VectorMapJoinHashTable) ht);
    return c;
  }

  /**
   * Minimal concrete {@code VectorMapJoinHashSetResult}. The parent classes are abstract but have
   * no unimplemented methods, so a bare subclass instantiates cleanly.
   */
  private static final class StubHashSetResult
      extends org.apache.hadoop.hive.ql.exec.vector.mapjoin.hashtable.VectorMapJoinHashSetResult {
  }
}
