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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.table.storage.compact.AlterTableCompactDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import org.junit.Assert;

/**
 * Tests for parsing and semantic analysis of ALTER TABLE ... compact.
 */
public class TestQBCompact {
  static QueryState queryState;
  static HiveConf conf;

  @BeforeClass
  public static void init() throws Exception {
    queryState = new QueryState.Builder().build();
    conf = queryState.getConf();
    conf
    .setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER,
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    SessionState.start(conf);

    // Create a table so we can work against it
    Hive h = Hive.get(conf);
    List<String> cols = new ArrayList<String>();
    cols.add("a");
    List<String> partCols = new ArrayList<String>();
    partCols.add("ds");
    h.createTable("foo", cols, partCols, OrcInputFormat.class, OrcOutputFormat.class);
    Table t = h.getTable("foo");
    Map<String, String> partSpec = new HashMap<String, String>();
    partSpec.put("ds", "today");
    h.createPartition(t, partSpec);
  }

  @AfterClass
  public static void deInit() throws Exception {
    Hive h = Hive.get(conf);
    h.dropTable("foo");
  }

  private void parseAndAnalyze(String query) throws Exception {
    ParseDriver hd = new ParseDriver();
    ASTNode head = (ASTNode)hd.parse(query).getTree().getChild(0);
    BaseSemanticAnalyzer a = SemanticAnalyzerFactory.get(queryState, head);
    a.analyze(head, new Context(conf));
    List<Task<?>> roots = a.getRootTasks();
    Assert.assertEquals(1, roots.size());
  }

  private AlterTableCompactDesc parseAndAnalyzeAlterTable(String query) throws Exception {
    ParseDriver hd = new ParseDriver();
    ASTNode head = (ASTNode)hd.parse(query).getTree().getChild(0);
    BaseSemanticAnalyzer a = SemanticAnalyzerFactory.get(queryState, head);
    a.analyze(head, new Context(conf));
    List<Task<?>> roots = a.getRootTasks();
    Assert.assertEquals(1, roots.size());
    return (AlterTableCompactDesc)((DDLWork)roots.get(0).getWork()).getDDLDesc();
  }

  @Test
  public void testNonPartitionedTable() throws Exception {
    AlterTableCompactDesc desc = parseAndAnalyzeAlterTable("alter table foo compact 'major'");
    Assert.assertEquals("major", desc.getCompactionType());
    Assert.assertEquals("default.foo", desc.getTableName());
  }

  @Test
  public void testBogusLevel() throws Exception {
    boolean sawException = false;
    try {
      parseAndAnalyze("alter table foo partition(ds = 'today') compact 'bogus'");
    } catch (SemanticException e) {
      sawException = true;
      Assert.assertEquals(ErrorMsg.INVALID_COMPACTION_TYPE.getMsg(), e.getMessage());
    }
    Assert.assertTrue(sawException);
  }

  @Test
  public void testMajor() throws Exception {
    AlterTableCompactDesc desc =
        parseAndAnalyzeAlterTable("alter table foo partition(ds = 'today') compact 'major'");
    Assert.assertEquals("major", desc.getCompactionType());
    Assert.assertEquals("default.foo", desc.getTableName());
    Map<String, String> parts = desc.getPartitionSpec();
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals("today", parts.get("ds"));
  }

  @Test
  public void testMinor() throws Exception {
    AlterTableCompactDesc desc =
        parseAndAnalyzeAlterTable("alter table foo partition(ds = 'today') compact 'minor'");
    Assert.assertEquals("minor", desc.getCompactionType());
    Assert.assertEquals("default.foo", desc.getTableName());
    Map<String, String> parts = desc.getPartitionSpec();
    Assert.assertEquals(1, parts.size());
    Assert.assertEquals("today", parts.get("ds"));
  }

  @Test
  public void showCompactions() throws Exception {
    parseAndAnalyze("show compactions");
  }

  @Test
  public void showTxns() throws Exception {
    parseAndAnalyze("show transactions");
  }
}
