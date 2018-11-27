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

package org.apache.hadoop.hive.ql.optimizer;

import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.junit.Test;

import java.util.EnumSet;

import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.AUTOPARALLEL;
import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.FIXED;
import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.UNIFORM;
import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.UNSET;
import static org.junit.Assert.*;

public class TestSharedWorkOptimizer {

  private static final EnumSet<ReduceSinkDesc.ReducerTraits> unset = EnumSet.of(UNSET);
  private static final EnumSet<ReduceSinkDesc.ReducerTraits> fixed = EnumSet.of(FIXED);
  private static final EnumSet<ReduceSinkDesc.ReducerTraits> uniform = EnumSet.of(UNIFORM);
  private static final EnumSet<ReduceSinkDesc.ReducerTraits> autoparallel = EnumSet.of(AUTOPARALLEL);
  private static final EnumSet<ReduceSinkDesc.ReducerTraits> uniformAutoparallel = EnumSet.of(UNIFORM, AUTOPARALLEL);

  private void ensureDeduplicate(
      EnumSet<ReduceSinkDesc.ReducerTraits> traits1, int numReducers1,
      EnumSet<ReduceSinkDesc.ReducerTraits> traits2, int numReducers2,
      EnumSet<ReduceSinkDesc.ReducerTraits> expectedTraits, int expectedNumReducers) {

    ReduceSinkDesc rsConf1;
    ReduceSinkDesc rsConf2;
    boolean deduplicated;

    rsConf1 = new ReduceSinkDesc();
    rsConf1.setReducerTraits(traits1);
    rsConf1.setNumReducers(numReducers1);
    rsConf2 = new ReduceSinkDesc();
    rsConf2.setReducerTraits(traits2);
    rsConf2.setNumReducers(numReducers2);
    deduplicated = SharedWorkOptimizer.deduplicateReduceTraits(rsConf1, rsConf2);
    assertTrue(deduplicated);
    assertEquals(expectedTraits, rsConf1.getReducerTraits());
    assertEquals(expectedNumReducers, rsConf1.getNumReducers());

    rsConf1 = new ReduceSinkDesc();
    rsConf1.setReducerTraits(traits1);
    rsConf1.setNumReducers(numReducers1);
    rsConf2 = new ReduceSinkDesc();
    rsConf2.setReducerTraits(traits2);
    rsConf2.setNumReducers(numReducers2);
    deduplicated = SharedWorkOptimizer.deduplicateReduceTraits(rsConf2, rsConf1);
    assertTrue(deduplicated);
    assertEquals(expectedTraits, rsConf2.getReducerTraits());
    assertEquals(expectedNumReducers, rsConf2.getNumReducers());
  }

  private void ensureNotDeduplicate(
      EnumSet<ReduceSinkDesc.ReducerTraits> traits1, int numReducers1,
      EnumSet<ReduceSinkDesc.ReducerTraits> traits2, int numReducers2) {

    ReduceSinkDesc rsConf1;
    ReduceSinkDesc rsConf2;
    boolean deduplicated;

    rsConf1 = new ReduceSinkDesc();
    rsConf1.setReducerTraits(traits1);
    rsConf1.setNumReducers(numReducers1);
    rsConf2 = new ReduceSinkDesc();
    rsConf2.setReducerTraits(traits2);
    rsConf2.setNumReducers(numReducers2);
    deduplicated = SharedWorkOptimizer.deduplicateReduceTraits(rsConf1, rsConf2);
    assertFalse(deduplicated);

    rsConf1 = new ReduceSinkDesc();
    rsConf1.setReducerTraits(traits1);
    rsConf1.setNumReducers(numReducers1);
    rsConf2 = new ReduceSinkDesc();
    rsConf2.setReducerTraits(traits2);
    rsConf2.setNumReducers(numReducers2);
    deduplicated = SharedWorkOptimizer.deduplicateReduceTraits(rsConf1, rsConf2);
    assertFalse(deduplicated);
  }

  @Test
  public void testDeduplicate() {
    // UNSET
    ensureDeduplicate(unset, 0, unset, 0, unset, 0);
    ensureDeduplicate(unset, 0, fixed, 1, fixed, 1);
    ensureDeduplicate(unset, 0, uniform, 1, uniform, 1);
    ensureDeduplicate(unset, 0, autoparallel, 1, autoparallel, 1);
    ensureDeduplicate(unset, 0, uniformAutoparallel, 1, uniformAutoparallel, 1);

    // FIXED
    ensureDeduplicate(fixed, 1, fixed, 1, fixed, 1);
    ensureNotDeduplicate(fixed, 1, fixed, 2);
    ensureDeduplicate(fixed, 1, uniform, 1, fixed, 1);
    ensureDeduplicate(fixed, 1, autoparallel, 2, fixed, 1);
    ensureDeduplicate(fixed, 1, uniformAutoparallel, 2, fixed, 1);

    // UNIFORM
    ensureDeduplicate(uniform, 1, uniform, 2, uniform, 2);
    ensureNotDeduplicate(uniform, 1, autoparallel, 2);
    ensureDeduplicate(uniform, 1, uniformAutoparallel, 2, uniform, 2);

    // AUTOPARALLEL
    ensureDeduplicate(autoparallel, 1, uniformAutoparallel, 2, autoparallel, 2);

    // UNIFORM and AUTOPARALLEL
    ensureDeduplicate(uniformAutoparallel, 1, uniformAutoparallel, 2, uniformAutoparallel, 2);
  }
}