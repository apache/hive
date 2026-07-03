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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.metastore.api.ErasureRunAudit;
import org.apache.hadoop.hive.metastore.api.ErasureRunStatus;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.anon.FileType;
import org.apache.hadoop.hive.ql.anon.builders.EraseStatementBuilder;
import org.apache.hadoop.hive.ql.anon.e2e.BaseEndToEndTest;
import org.apache.hadoop.hive.ql.anon.hooks.ErasureRunCompletionHook;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;

public class TestErasureRunFilesRewritten extends BaseEndToEndTest {

  private final String policy = "trfr_pol_" + Long.toString(System.nanoTime(), 36);

  private final String policyDsl =
      """
      VERSION v1
      IDENTITY userId TYPE INT
      SCHEMA   TYPE INT
      FOR SCHEMA 3
          ERASE country, city, telephone, ipList
      """;

  @Override
  protected String[] managedErasurePolicies() {
    return new String[] { policy };
  }

  @Test
  public void eraseGathersFilesRewrittenAndStampsTheAuditRow() throws Exception {
    this.internalFormat = ColumnInternalFormat.JSON;
    this.fileType = FileType.ORC;
    this.tblName = "t_trfr_" + Long.toString(System.nanoTime(), 36);
    this.policyName = policy;

    create();
    truncate();
    insert();
    provisionAndAttach();

    final long tblId = hive.getTable(tblName).getTTable().getId();

    final HiveConf hookConf = new HiveConf(conf);
    hookConf.setVar(HiveConf.ConfVars.POST_EXEC_HOOKS, ErasureRunCompletionHook.class.getName());
    final Driver hookDriver = new Driver(hookConf);
    try {
      hookDriver.run(new EraseStatementBuilder(tblName).withIds(userId).build());
    } finally {
      hookDriver.close();
    }

    final List<ErasureRunAudit> runs =
        hive.getErasureRunsForTable(tblId, Long.MIN_VALUE, Long.MAX_VALUE, null, null);
    Assertions.assertEquals(1, runs.size(), "exactly one ERASE run for the fresh table");
    final ErasureRunAudit run = runs.get(0);
    Assertions.assertEquals(ErasureRunStatus.SUCCEEDED, run.getStatus(),
        "the post-exec hook completes the run");
    Assertions.assertTrue(run.isSetFilesRewritten() && run.getFilesRewritten() >= 1,
        "the run records the authoritative count of rewritten files; got: " + run);
  }

  private void provisionAndAttach() throws CommandProcessorException, IOException {
    final Path policyFile = Files.createTempFile(policy + "_", ".erp");
    Files.write(policyFile, policyDsl.getBytes());
    execute("LOAD ERASURE POLICY " + policy + " FROM '" + policyFile + "'");
    execute("VALIDATE ERASURE POLICY " + policy);
    execute("ACTIVATE ERASURE POLICY " + policy);
    execute(("ATTACH DATA ERASURE POLICY %s ON TABLE %s COLUMN %s"
        + " WITH ( SCHEMA FIELD (%s), ROW LOCATOR (%s),"
        + " COLUMN FORMAT (JSON) )"
        + " RESOLUTION ( EXPLICIT )").formatted(policy, tblName, bColName, mColName, oColName));
  }
}
