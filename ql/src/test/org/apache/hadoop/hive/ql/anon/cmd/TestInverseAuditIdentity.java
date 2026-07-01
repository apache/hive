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

public class TestInverseAuditIdentity extends BaseTest {

  private long realTable() throws Exception {
    final String name = "iai_" + Long.toString(System.nanoTime(), 36);
    execute("CREATE TABLE IF NOT EXISTS %s (m INT, o BIGINT, b STRING) STORED AS ORC".formatted(name));
    return hive.getTable(name).getTTable().getId();
  }

  private void seed(long tblId, long startedTs, String identityValues) throws Exception {
    final ErasureRunAudit run = new ErasureRunAudit();
    run.setTblId(tblId);
    run.setColumnName("body");
    run.setBindingId(0L);
    run.setPrincipal("test");
    run.setStartedTs(startedTs);
    run.setIdentityValues(identityValues);
    run.setStatus(ErasureRunStatus.SUCCEEDED);
    hive.recordErasureRun(run);
  }

  private List<ErasureRunAudit> byIdentity(String identity) throws Exception {
    return hive.getErasureRunsForTable(0L, Long.MIN_VALUE, Long.MAX_VALUE, null, identity);
  }

  @Test
  public void indexedLookupIsExactNotSubstring() throws Exception {
    final String n = "n" + Long.toString(System.nanoTime(), 36);
    final String idA  = n + "10012";
    final String idA2 = n + "10087";
    final String idB  = n + "99999";
    final String probe = n + "1001";
    final long ts = System.nanoTime();

    seed(realTable(), ts,     idA + "," + idA2);
    seed(realTable(), ts + 1, idB);

    final List<ErasureRunAudit> a = byIdentity(idA);
    Assertions.assertEquals(1, a.size(), "idA must match exactly one run");
    Assertions.assertTrue(a.get(0).getIdentityValues().contains(idA));

    Assertions.assertEquals(1, byIdentity(idA2).size(), "idA2 must match run A");

    final List<ErasureRunAudit> b = byIdentity(idB);
    Assertions.assertEquals(1, b.size(), "idB must match exactly one run");
    Assertions.assertEquals(idB, b.get(0).getIdentityValues());

    Assertions.assertTrue(byIdentity(probe).isEmpty(),
        "a substring of an identity value must not match the exact lookup");
  }
}
