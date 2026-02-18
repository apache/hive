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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.junit.Assert;
import org.junit.Test;

import java.util.Arrays;
import java.util.List;

import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.execSelectAndDumpData;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriver;
import static org.apache.hadoop.hive.ql.txn.compactor.TestCompactor.executeStatementOnDriverSilently;

public class TestQueryRewrite extends CompactorOnTezTest {

  private static final String DB = "default";
  private static final String TABLE1 = "t1";
  private static final String MV1 = "mat1";

  private static final List<String> ORIGINAL_QUERY_PLAN = Arrays.asList(
          "CBO PLAN:",
          "HiveProject(a=[$0], b=[$1])",
          "  HiveFilter(condition=[>($0, 0)])",
          "    HiveTableScan(table=[[" + DB + ", " + TABLE1 + "]], table:alias=[" + TABLE1 + "])",
          ""
  );

  @Override
  public void setup() throws Exception {
    super.setup();

    executeStatementOnDriverSilently("drop materialized view if exists " + MV1, driver);
    executeStatementOnDriverSilently("drop table if exists " + TABLE1, driver);

    executeStatementOnDriver("create table " + TABLE1 + "(a int, b string, c float) stored as orc TBLPROPERTIES ('transactional'='true')", driver);
    executeStatementOnDriver("insert into " + TABLE1 + "(a,b, c) values (1, 'one', 1.1), (2, 'two', 2.2), (NULL, NULL, NULL)", driver);
    executeStatementOnDriver("create materialized view " + MV1 + " stored by iceberg tblproperties('format-version'='2') as " +
            "select a,b,c from " + TABLE1 + " where a > 0 or a is null", driver);
  }

  @Override
  public void tearDown() {
    executeStatementOnDriverSilently("drop materialized view " + MV1, driver);
    executeStatementOnDriverSilently("drop table " + TABLE1 , driver);

    super.tearDown();
  }

  @Test
  public void testQueryIsNotRewrittenWhenMVIsDropped() throws Exception {

    // Simulate a multi HS2 cluster.
    // Drop the MV using a direct API call to HMS. This is similar to what happens when the drop MV is executed by
    // another HS2.
    // In this case the MV is not removed from HiveMaterializedViewsRegistry of HS2 which runs the explain query.
    msClient.dropTable(DB, MV1);

    List<String> result = execSelectAndDumpData("explain cbo select a, b from " + TABLE1 + " where a > 0", driver, "");
    Assert.assertEquals(ORIGINAL_QUERY_PLAN, result);
  }

  @Test
  public void testQueryIsNotRewrittenWhenMVIsDisabledForRewrite() throws Exception {
    Table mvTable = Hive.get().getTable(DB, MV1);
    mvTable.setRewriteEnabled(false);

    EnvironmentContext environmentContext = new EnvironmentContext();
    environmentContext.putToProperties(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);
    Hive.get().alterTable(mvTable, false, environmentContext, true);

    List<String> result = execSelectAndDumpData("explain cbo select a, b from " + TABLE1 + " where a > 0", driver, "");
    Assert.assertEquals(ORIGINAL_QUERY_PLAN, result);
  }
}
