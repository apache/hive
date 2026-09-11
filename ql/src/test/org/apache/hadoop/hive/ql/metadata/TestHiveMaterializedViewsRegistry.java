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
package org.apache.hadoop.hive.ql.metadata;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConfForTest;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.Test;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;

/**
 * Tests for {@link HiveMaterializedViewsRegistry}.
 */
public class TestHiveMaterializedViewsRegistry {
  private static HiveConf conf = new HiveConfForTest(TestHiveMaterializedViewsRegistry.class);

  @Before
  public void setUp() {
    // Use the real (non test-only) authorizer factory, since the test-only
    // one lives in the itests module and is not on this module's classpath.
    conf.set("hive.security.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    SessionState.start(conf);
  }

  /**
   * HIVE-27636: if createMaterialization() encounters an exception while
   * parsing/analyzing the materialized view's query (e.g. an invalid
   * column reference), any scratch/staging directories created during
   * that attempt were previously left behind, relying solely on
   * fs.deleteOnExit() (i.e. only cleaned up at JVM exit). This is
   * problematic for long-running processes like HiveServer2, where such
   * directories can accumulate over time.
   *
   * This test uses a query with a CTE referenced enough times to trigger
   * CTE materialization (which creates a directory tracked by the
   * Context), followed by an invalid column reference that fails during
   * semantic analysis - to verify that the directory is removed eagerly
   * as part of exception handling, rather than being left for JVM exit.
   */
  @Test
  public void testStagingDirIsCleanedUpOnParseException() throws Exception {
    Table table = new Table("default", "bad_mv");
    table.setViewExpandedText(
        "WITH cte1 AS (select 1 as x) "
        + "select a.nonexistent_column_xyz_123 from cte1 a "
        + "join cte1 b on a.x = b.x "
        + "join cte1 c on a.x = c.x");

    HiveMaterializedViewsRegistry registry = HiveMaterializedViewsRegistry.get();
    HiveRelOptMaterialization result = registry.createMaterialization(conf, table);
    assertNull("Expected createMaterialization to return null on parse/semantic failure", result);

    Path localScratchDir = new Path(HiveConf.getVar(conf, HiveConf.ConfVars.LOCAL_SCRATCH_DIR));
    FileSystem fs = localScratchDir.getFileSystem(conf);
    // No per-query scratch subdirectory should remain once
    // createMaterialization() has returned - it should have been removed
    // eagerly by Context.clear(), not left for JVM exit.
    if (fs.exists(localScratchDir)) {
      for (var status : fs.listStatus(localScratchDir)) {
        assertFalse("Leftover scratch directory found: " + status.getPath()
                + ". Staging directories should be cleaned up eagerly on "
                + "parse/semantic exceptions in createMaterialization().",
            status.isDirectory() && fs.listStatus(status.getPath()).length > 0
                && hasHiveScratchSubdir(fs, status.getPath()));
      }
    }
  }

  private static boolean hasHiveScratchSubdir(FileSystem fs, Path path) throws Exception {
    for (var status : fs.listStatus(path)) {
      if (status.isDirectory() && status.getPath().getName().startsWith("hive_")) {
        return true;
      }
    }
    return false;
  }
}
