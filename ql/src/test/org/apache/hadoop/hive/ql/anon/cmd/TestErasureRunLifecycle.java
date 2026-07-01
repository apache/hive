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

package org.apache.hadoop.hive.ql.anon.cmd;

import org.apache.hadoop.hive.metastore.api.ErasureRunAudit;
import org.apache.hadoop.hive.metastore.api.ErasureRunStatus;
import org.apache.hadoop.hive.ql.anon.BaseTest;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.List;

public class TestErasureRunLifecycle extends BaseTest {

  private long realTable() throws Exception {
    final String name = "erl_" + Long.toString(System.nanoTime(), 36);
    execute("CREATE TABLE IF NOT EXISTS %s (m INT, o BIGINT, b STRING) STORED AS ORC".formatted(name));
    return hive.getTable(name).getTTable().getId();
  }

  private long seedInitiated(long tblId, long startedTs) throws Exception {
    final ErasureRunAudit run = new ErasureRunAudit();
    run.setTblId(tblId);
    run.setColumnName("body");
    run.setBindingId(0L);
    run.setPrincipal("test");
    run.setStartedTs(startedTs);
    run.setIdentityValues("subj-" + Long.toString(startedTs, 36));
    run.setStatus(ErasureRunStatus.INITIATED);
    hive.recordErasureRun(run);
    return startedTs;
  }

  private ErasureRunAudit onlyRun(long tblId) throws Exception {
    final List<ErasureRunAudit> rows =
        hive.getErasureRunsForTable(tblId, Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    Assertions.assertEquals(1, rows.size(), "expected exactly one seeded run for tblId " + tblId);
    return rows.get(0);
  }

  @Test
  public void initiatedEnumRoundTrips() {
    Assertions.assertEquals(7, ErasureRunStatus.INITIATED.getValue());
    Assertions.assertEquals(ErasureRunStatus.INITIATED, ErasureRunStatus.findByValue(7));
    Assertions.assertEquals(ErasureRunStatus.INITIATED, ErasureRunStatus.valueOf("INITIATED"));
  }

  @Test
  public void initiatedTransitionsToSucceeded() throws Exception {
    final long tblId = realTable();
    final long startedTs = System.nanoTime();
    seedInitiated(tblId, startedTs);

    final ErasureRunAudit before = onlyRun(tblId);
    Assertions.assertEquals(ErasureRunStatus.INITIATED, before.getStatus(),
        "the row is INITIATED before the completion hook runs");
    Assertions.assertTrue(before.getCompletedTs() == 0L || !before.isSetCompletedTs(),
        "an uncompleted run carries no completion time");

    final long completedTs = startedTs + 5L;
    hive.updateErasureRunCompletion(tblId, startedTs, completedTs, ErasureRunStatus.SUCCEEDED);

    final ErasureRunAudit after = onlyRun(tblId);
    Assertions.assertEquals(ErasureRunStatus.SUCCEEDED, after.getStatus(),
        "the post-execution hook transitions INITIATED to SUCCEEDED");
    Assertions.assertEquals(completedTs, after.getCompletedTs(),
        "completion time is stamped on the transition");
  }

  @Test
  public void completionStampsFilesRewritten() throws Exception {
    final long tblId = realTable();
    final long startedTs = System.nanoTime();
    seedInitiated(tblId, startedTs);

    hive.updateErasureRunCompletion(tblId, startedTs, startedTs + 5L, ErasureRunStatus.SUCCEEDED);

    final ErasureRunAudit patch = new ErasureRunAudit();
    patch.setTblId(tblId);
    patch.setStartedTs(startedTs);
    patch.setFilesRewritten(7);
    hive.recordErasureRun(patch);

    final ErasureRunAudit after = onlyRun(tblId);
    Assertions.assertTrue(after.isSetFilesRewritten() && after.getFilesRewritten() == 7,
        "the gathered file count is stamped on the run; got: " + after);
    Assertions.assertEquals(ErasureRunStatus.SUCCEEDED, after.getStatus(),
        "stamping filesRewritten must not disturb the completion status");
    Assertions.assertEquals("subj-" + Long.toString(startedTs, 36), after.getIdentityValues(),
        "stamping filesRewritten must not disturb identityValues");
  }

  @Test
  public void initiatedTransitionsToFailed() throws Exception {
    final long tblId = realTable();
    final long startedTs = System.nanoTime();
    seedInitiated(tblId, startedTs);

    hive.updateErasureRunCompletion(tblId, startedTs, startedTs + 9L, ErasureRunStatus.FAILED);

    Assertions.assertEquals(ErasureRunStatus.FAILED, onlyRun(tblId).getStatus(),
        "a failed run is recorded FAILED, never left as a false SUCCEEDED");
  }
}
