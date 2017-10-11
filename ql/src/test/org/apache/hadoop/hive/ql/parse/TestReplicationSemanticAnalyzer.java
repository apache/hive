/*
  Licensed to the Apache Software Foundation (ASF) under one
  or more contributor license agreements.  See the NOTICE file
  distributed with this work for additional information
  regarding copyright ownership.  The ASF licenses this file
  to you under the Apache License, Version 2.0 (the
  "License"); you may not use this file except in compliance
  with the License.  You may obtain a copy of the License at

      http://www.apache.org/licenses/LICENSE-2.0

  Unless required by applicable law or agreed to in writing, software
  distributed under the License is distributed on an "AS IS" BASIS,
  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
  See the License for the specific language governing permissions and
  limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse;


import java.io.Serializable;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.io.orc.OrcInputFormat;
import org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

public class TestReplicationSemanticAnalyzer {
  private static QueryState queryState;
  static HiveConf conf;
  private static String defaultDB = "default";
  private static String tblName = "testReplSA";
  private static ArrayList<String> cols =  new ArrayList<String>(Arrays.asList("col1", "col2"));

  @BeforeClass
  public static void initialize() throws HiveException {
    queryState =
        new QueryState.Builder().withHiveConf(new HiveConf(SemanticAnalyzer.class)).build();
    conf = queryState.getConf();
    conf.set("hive.security.authorization.manager", "");
    SessionState.start(conf);
    Hive hiveDb = Hive.get(conf);
    hiveDb.createTable(defaultDB + "." + tblName, cols, null, OrcInputFormat.class, OrcOutputFormat.class);
    Table t = hiveDb.getTable(tblName);
  }

  @AfterClass
  public static void teardown() throws HiveException {
  }

  @Test
  public void testReplDumpParse() throws Exception {
    ParseDriver pd = new ParseDriver();
    String fromEventId = "100";
    String toEventId = "200";
    String maxEventLimit = "50";
    ASTNode root;
    ASTNode child;

    String query = "repl dump " + defaultDB;
    root = (ASTNode) pd.parse(query).getChild(0);
    assertEquals(root.getText(), "TOK_REPL_DUMP");
    assertEquals(root.getChildCount(), 1);

    child =  (ASTNode) root.getChild(0);
    assertEquals(child.getText(), defaultDB);
    assertEquals(child.getChildCount(), 0);

    query = "repl dump " + defaultDB + "." + tblName;
    root = (ASTNode) pd.parse(query).getChild(0);
    assertEquals(root.getChildCount(), 2);

    child =  (ASTNode) root.getChild(0);
    assertEquals(child.getText(), defaultDB);
    assertEquals(child.getChildCount(), 0);

    child =  (ASTNode) root.getChild(1);
    assertEquals(child.getText(), tblName);
    assertEquals(child.getChildCount(), 0);

    query = "repl dump " + defaultDB + "." + tblName + " from " + fromEventId;
    root = (ASTNode) pd.parse(query).getChild(0);
    assertEquals(root.getChildCount(), 3);

    child =  (ASTNode) root.getChild(0);
    assertEquals(child.getText(), defaultDB);
    assertEquals(child.getChildCount(), 0);

    child =  (ASTNode) root.getChild(1);
    assertEquals(child.getText(), tblName);
    assertEquals(child.getChildCount(), 0);

    root =  (ASTNode) root.getChild(2);
    assertEquals(root.getText(), "TOK_FROM");
    assertEquals(root.getChildCount(), 1);

    child =  (ASTNode) root.getChild(0);
    assertEquals(child.getText(), fromEventId);
    assertEquals(child.getChildCount(), 0);

    query = "repl dump " + defaultDB + "." + tblName + " from " + fromEventId + " to " + toEventId;

    root = (ASTNode) pd.parse(query).getChild(0);
    assertEquals(root.getChildCount(), 3);

    child =  (ASTNode) root.getChild(0);
    assertEquals(child.getText(), defaultDB);
    assertEquals(child.getChildCount(), 0);

    child =  (ASTNode) root.getChild(1);
    assertEquals(child.getText(), tblName);
    assertEquals(child.getChildCount(), 0);

    root =  (ASTNode) root.getChild(2);
    assertEquals(root.getText(), "TOK_FROM");
    assertEquals(root.getChildCount(), 3);

    child =  (ASTNode) root.getChild(0);
    assertEquals(child.getText(), fromEventId);
    assertEquals(child.getChildCount(), 0);

    child =  (ASTNode) root.getChild(1);
    assertEquals(child.getText(), "TOK_TO");
    assertEquals(child.getChildCount(), 0);

    child =  (ASTNode) root.getChild(2);
    assertEquals(child.getText(), toEventId);
    assertEquals(child.getChildCount(), 0);

    query =
        "repl dump " + defaultDB + "." + tblName + " from " + fromEventId + " to " + toEventId
            + " limit " + maxEventLimit;

    root = (ASTNode) pd.parse(query).getChild(0);
    assertEquals(root.getChildCount(), 3);

    child =  (ASTNode) root.getChild(0);
    assertEquals(child.getText(), defaultDB);
    assertEquals(child.getChildCount(), 0);

    child =  (ASTNode) root.getChild(1);
    assertEquals(child.getText(), tblName);
    assertEquals(child.getChildCount(), 0);

    root =  (ASTNode) root.getChild(2);
    assertEquals(root.getText(), "TOK_FROM");
    assertEquals(root.getChildCount(), 5);

    child =  (ASTNode) root.getChild(0);
    assertEquals(child.getText(), fromEventId);
    assertEquals(child.getChildCount(), 0);

    child =  (ASTNode) root.getChild(1);
    assertEquals(child.getText(), "TOK_TO");
    assertEquals(child.getChildCount(), 0);

    child =  (ASTNode) root.getChild(2);
    assertEquals(child.getText(), toEventId);
    assertEquals(child.getChildCount(), 0);

    child =  (ASTNode) root.getChild(3);
    assertEquals(child.getText(), "TOK_LIMIT");
    assertEquals(child.getChildCount(), 0);

    child =  (ASTNode) root.getChild(4);
    assertEquals(child.getText(), maxEventLimit);
    assertEquals(child.getChildCount(), 0);
  }

  @Test
  public void testReplLoadParse() throws Exception {
    // FileSystem fs = FileSystem.get(conf);
    ParseDriver pd = new ParseDriver();
    ASTNode root;
    ASTNode child;
    ASTNode subChild;
    ASTNode configNode;
    String replRoot = conf.getVar(HiveConf.ConfVars.REPLDIR);
    Path dumpRoot = new Path(replRoot, "next");
    System.out.println(replRoot);
    System.out.println(dumpRoot);
    String newDB = "default_bak";
    String newDB2= "default_bak_2";

    String query = "repl load  from '" + dumpRoot.toString() + "'";
    root = (ASTNode) pd.parse(query).getChild(0);
    assertEquals(root.getText(), "TOK_REPL_LOAD");
    assertEquals(root.getChildCount(), 1);
    child =  (ASTNode) root.getChild(0);
    assertEquals(child.getText(), "'" + dumpRoot.toString() + "'");
    assertEquals(child.getChildCount(), 0);

    query = "repl load " + newDB + " from '" + dumpRoot.toString() + "'";
    root = (ASTNode) pd.parse(query).getChild(0);
    assertEquals(root.getText(), "TOK_REPL_LOAD");
    assertEquals(root.getChildCount(), 2);
    child =  (ASTNode) root.getChild(0);
    assertEquals(child.getText(), "'" + dumpRoot.toString() + "'");
    assertEquals(child.getChildCount(), 0);
    child =  (ASTNode) root.getChild(1);
    assertEquals(child.getText(), "TOK_DBNAME");
    assertEquals(child.getChildCount(), 1);
    subChild = (ASTNode) child.getChild(0);
    assertEquals(subChild.getText(), newDB);
    assertEquals(subChild.getChildCount(), 0);

    query = "repl load " + newDB2 + " from '" + dumpRoot.toString()
            + "' with ('mapred.job.queue.name'='repl','hive.repl.approx.max.load.tasks'='100')";
    root = (ASTNode) pd.parse(query).getChild(0);
    assertEquals(root.getText(), "TOK_REPL_LOAD");
    assertEquals(root.getChildCount(), 3);
    child =  (ASTNode) root.getChild(0);
    assertEquals(child.getText(), "'" + dumpRoot.toString() + "'");
    assertEquals(child.getChildCount(), 0);
    child =  (ASTNode) root.getChild(1);
    assertEquals(child.getText(), "TOK_DBNAME");
    assertEquals(child.getChildCount(), 1);
    subChild = (ASTNode) child.getChild(0);
    assertEquals(subChild.getText(), newDB2);
    assertEquals(subChild.getChildCount(), 0);
    child =  (ASTNode) root.getChild(2);
    assertEquals(child.getText(), "TOK_REPL_CONFIG");
    assertEquals(child.getChildCount(), 1);
    subChild = (ASTNode) child.getChild(0);
    assertEquals(subChild.getText(), "TOK_REPL_CONFIG_LIST");
    assertEquals(subChild.getChildCount(), 2);
    configNode = (ASTNode) subChild.getChild(0);
    assertEquals(configNode.getText(), "TOK_TABLEPROPERTY");
    assertEquals(configNode.getChildCount(), 2);
    assertEquals(configNode.getChild(0).getText(), "'mapred.job.queue.name'");
    assertEquals(configNode.getChild(1).getText(), "'repl'");
    configNode = (ASTNode) subChild.getChild(1);
    assertEquals(configNode.getText(), "TOK_TABLEPROPERTY");
    assertEquals(configNode.getChildCount(), 2);
    assertEquals(configNode.getChild(0).getText(), "'hive.repl.approx.max.load.tasks'");
    assertEquals(configNode.getChild(1).getText(), "'100'");
  }

  //@Test
  public void testReplLoadAnalyze() throws Exception {
    ParseDriver pd = new ParseDriver();
    ASTNode root;
    String replRoot = conf.getVar(HiveConf.ConfVars.REPLDIR);
    FileSystem fs = FileSystem.get(conf);
    Path dumpRoot = new Path(replRoot, "next");
    System.out.println(replRoot);
    System.out.println(dumpRoot);
    String newDB = "default_bak";

    // First create a dump
    String query = "repl dump " + defaultDB;
    root = (ASTNode) pd.parse(query).getChild(0);
    ReplicationSemanticAnalyzer rs = (ReplicationSemanticAnalyzer) SemanticAnalyzerFactory.get(queryState, root);
    rs.analyze(root, new Context(conf));

    // Then analyze load
    query = "repl load  from '" + dumpRoot.toString() + "'";
    root = (ASTNode) pd.parse(query).getChild(0);
    rs = (ReplicationSemanticAnalyzer) SemanticAnalyzerFactory.get(queryState, root);
    rs.analyze(root, new Context(conf));
    List<Task<? extends Serializable>> roots = rs.getRootTasks();
    assertEquals(1, roots.size());

    query = "repl load " + newDB + " from '" + dumpRoot.toString() + "'";
    root = (ASTNode) pd.parse(query).getChild(0);
    rs = (ReplicationSemanticAnalyzer) SemanticAnalyzerFactory.get(queryState, root);
    rs.analyze(root, new Context(conf));
    roots = rs.getRootTasks();
    assertEquals(1, roots.size());
  }

  @Test
  public void testReplStatusAnalyze() throws Exception {
    ParseDriver pd = new ParseDriver();
    ASTNode root;

    // Repl status command
    String query = "repl status " + defaultDB;
    root = (ASTNode) pd.parse(query).getChild(0);
    ReplicationSemanticAnalyzer rs = (ReplicationSemanticAnalyzer) SemanticAnalyzerFactory.get(queryState, root);
    rs.analyze(root, new Context(conf));

    FetchTask fetchTask = rs.getFetchTask();
    assertNotNull(fetchTask);
  }
}
