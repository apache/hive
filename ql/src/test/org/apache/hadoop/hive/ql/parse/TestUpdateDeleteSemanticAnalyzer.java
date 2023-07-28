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
package org.apache.hadoop.hive.ql.parse;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryPlan;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.ExplainWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestUpdateDeleteSemanticAnalyzer {

  static final private Logger LOG = LoggerFactory.getLogger(TestUpdateDeleteSemanticAnalyzer.class.getName());

  private QueryState queryState;
  private HiveConf conf;
  private Hive db;

  // All of the insert, update, and delete tests assume two tables, T and U, each with columns a,
  // and b.  U it partitioned by an additional column ds.  These are created by parseAndAnalyze
  // and removed by cleanupTables().

  @Test
  public void testInsertSelect() throws Exception {
    try {
      ReturnInfo rc = parseAndAnalyze("insert into table T select a, b from U", "testInsertSelect");

      LOG.info(explain((SemanticAnalyzer)rc.sem, rc.plan));

    } finally {
      cleanupTables();
    }
  }

  @Test
  public void testDeleteAllNonPartitioned() throws Exception {
    try {
      ReturnInfo rc = parseAndAnalyze("delete from T", "testDeleteAllNonPartitioned");
      LOG.info(explain((SemanticAnalyzer)rc.sem, rc.plan));
    } finally {
      cleanupTables();
    }
  }

  @Test
  public void testDeleteWhereNoPartition() throws Exception {
    try {
      ReturnInfo rc = parseAndAnalyze("delete from T where a > 5", "testDeleteWhereNoPartition");
      LOG.info(explain((SemanticAnalyzer)rc.sem, rc.plan));
    } finally {
      cleanupTables();
    }
  }

  @Test
  public void testDeleteAllPartitioned() throws Exception {
    try {
      ReturnInfo rc = parseAndAnalyze("delete from U", "testDeleteAllPartitioned");
      LOG.info(explain((SemanticAnalyzer)rc.sem, rc.plan));
    } finally {
      cleanupTables();
    }
  }

  @Test
  public void testDeleteAllWherePartitioned() throws Exception {
    try {
      ReturnInfo rc = parseAndAnalyze("delete from U where a > 5", "testDeleteAllWherePartitioned");
      LOG.info(explain((SemanticAnalyzer)rc.sem, rc.plan));
    } finally {
      cleanupTables();
    }
  }

  @Test
  public void testDeleteOnePartition() throws Exception {
    try {
      ReturnInfo rc = parseAndAnalyze("delete from U where ds = 'today'",
          "testDeleteFromPartitionOnly");
      LOG.info(explain((SemanticAnalyzer)rc.sem, rc.plan));
    } finally {
      cleanupTables();
    }
  }

  @Test
  public void testDeleteOnePartitionWhere() throws Exception {
    try {
      ReturnInfo rc = parseAndAnalyze("delete from U where ds = 'today' and a > 5",
          "testDeletePartitionWhere");
      LOG.info(explain((SemanticAnalyzer)rc.sem, rc.plan));
    } finally {
      cleanupTables();
    }
  }

  @Test
  public void testUpdateAllNonPartitioned() throws Exception {
    try {
      ReturnInfo rc = parseAndAnalyze("update T set b = 5", "testUpdateAllNonPartitioned");
      LOG.info(explain((SemanticAnalyzer)rc.sem, rc.plan));
    } finally {
      cleanupTables();
    }
  }

  @Test
  public void testUpdateAllNonPartitionedWhere() throws Exception {
    try {
      ReturnInfo rc = parseAndAnalyze("update T set b = 5 where b > 5",
          "testUpdateAllNonPartitionedWhere");
      LOG.info(explain((SemanticAnalyzer)rc.sem, rc.plan));
    } finally {
      cleanupTables();
    }
  }

  @Test
  public void testUpdateAllPartitioned() throws Exception {
    try {
      ReturnInfo rc = parseAndAnalyze("update U set b = 5", "testUpdateAllPartitioned");
      LOG.info(explain((SemanticAnalyzer)rc.sem, rc.plan));
    } finally {
      cleanupTables();
    }
  }

  @Test
  public void testUpdateAllPartitionedWhere() throws Exception {
    try {
      ReturnInfo rc = parseAndAnalyze("update U set b = 5 where b > 5",
          "testUpdateAllPartitionedWhere");
      LOG.info(explain((SemanticAnalyzer)rc.sem, rc.plan));
    } finally {
      cleanupTables();
    }
  }

  @Test
  public void testUpdateOnePartition() throws Exception {
    try {
      ReturnInfo rc = parseAndAnalyze("update U set b = 5 where ds = 'today'",
          "testUpdateOnePartition");
      LOG.info(explain((SemanticAnalyzer)rc.sem, rc.plan));
    } finally {
      cleanupTables();
    }
  }

  @Test
  public void testUpdateOnePartitionWhere() throws Exception {
    try {
      ReturnInfo rc = parseAndAnalyze("update U set b = 5 where ds = 'today' and b > 5",
          "testUpdateOnePartitionWhere");
      LOG.info(explain((SemanticAnalyzer)rc.sem, rc.plan));
    } finally {
      cleanupTables();
    }
  }

  @Test
  public void testInsertValues() throws Exception {
    try {
      ReturnInfo rc = parseAndAnalyze("insert into table T values ('abc', 3), ('ghi', null)",
          "testInsertValues");

      LOG.info(explain((SemanticAnalyzer)rc.sem, rc.plan));

    } finally {
      cleanupTables();
    }
  }

  @Test
  public void testInsertValuesPartitioned() throws Exception {
    try {
      ReturnInfo rc = parseAndAnalyze("insert into table U partition (ds) values " +
              "('abc', 3, 'today'), ('ghi', 5, 'tomorrow')",
          "testInsertValuesPartitioned");

      LOG.info(explain((SemanticAnalyzer) rc.sem, rc.plan));

    } finally {
      cleanupTables();
    }
  }

  @Before
  public void setup() {
    queryState = new QueryState.Builder().build();
    conf = queryState.getConf();
    conf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    conf.setVar(HiveConf.ConfVars.DYNAMICPARTITIONINGMODE, "nonstrict");
    conf.setVar(HiveConf.ConfVars.HIVEMAPREDMODE, "nonstrict");
    conf.setVar(HiveConf.ConfVars.HIVE_TXN_MANAGER, "org.apache.hadoop.hive.ql.lockmgr.DbTxnManager");
  }

  public void cleanupTables() throws HiveException {
    if (db != null) {
      db.dropTable("T");
      db.dropTable("U");
    }
  }

  private class ReturnInfo {
    BaseSemanticAnalyzer sem;
    QueryPlan plan;

    ReturnInfo(BaseSemanticAnalyzer s, QueryPlan p) {
      sem = s;
      plan = p;
    }
  }

  private ReturnInfo parseAndAnalyze(String query, String testName)
      throws IOException, ParseException, HiveException {

    SessionState.start(conf);
    Context ctx = new Context(conf);
    ctx.setCmd(query);
    ctx.setHDFSCleanup(true);

    ASTNode tree = ParseUtils.parse(query, ctx);

    BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(queryState, tree);
    SessionState.get().initTxnMgr(conf);
    SessionState.get().getTxnMgr().openTxn(ctx, conf.getUser());
    db = sem.getDb();

    // I have to create the tables here (rather than in setup()) because I need the Hive
    // connection, which is conveniently created by the semantic analyzer.
    Map<String, String> params = new HashMap<String, String>(1);
    params.put(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true");
    db.createTable("T", Arrays.asList("a", "b"), null, OrcInputFormat.class,
        OrcOutputFormat.class, 2, Arrays.asList("a"), params);
    db.createTable("U", Arrays.asList("a", "b"), Arrays.asList("ds"), OrcInputFormat.class,
        OrcOutputFormat.class, 2, Arrays.asList("a"), params);
    Table u = db.getTable("U");
    Map<String, String> partVals = new HashMap<String, String>(2);
    partVals.put("ds", "yesterday");
    db.createPartition(u, partVals);
    partVals.clear();
    partVals.put("ds", "today");
    db.createPartition(u, partVals);
    sem.analyze(tree, ctx);
    // validate the plan
    sem.validate();

    QueryPlan plan = new QueryPlan(query, sem, 0L, testName, null, null);

    return new ReturnInfo(sem, plan);
  }

  private String explain(SemanticAnalyzer sem, QueryPlan plan) throws
      IOException {
    FileSystem fs = FileSystem.get(conf);
    File f = File.createTempFile("TestSemanticAnalyzer", "explain");
    Path tmp = new Path(f.getPath());
    fs.create(tmp);
    fs.deleteOnExit(tmp);
    ExplainConfiguration config = new ExplainConfiguration();
    config.setExtended(true);
    ExplainWork work = new ExplainWork(tmp, sem.getParseContext(), sem.getRootTasks(),
        sem.getFetchTask(), sem, config, null, plan.getOptimizedQueryString());
    ExplainTask task = new ExplainTask();
    task.setWork(work);
    task.initialize(queryState, plan, null, null);
    task.execute(null);
    FSDataInputStream in = fs.open(tmp);
    StringBuilder builder = new StringBuilder();
    final int bufSz = 4096;
    byte[] buf = new byte[bufSz];
    long pos = 0L;
    while (true) {
      int bytesRead = in.read(pos, buf, 0, bufSz);
      if (bytesRead > 0) {
        pos += bytesRead;
        builder.append(new String(buf, 0, bytesRead));
      } else {
        // Reached end of file
        in.close();
        break;
      }
    }
    return builder.toString()
        .replaceAll("pfile:/.*\n", "pfile:MASKED-OUT\n")
        .replaceAll("location file:/.*\n", "location file:MASKED-OUT\n")
        .replaceAll("file:/.*\n", "file:MASKED-OUT\n")
        .replaceAll("transient_lastDdlTime.*\n", "transient_lastDdlTime MASKED-OUT\n");
  }
}
