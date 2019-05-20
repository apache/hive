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

import org.junit.Test;
import org.junit.experimental.runners.Enclosed;
import org.junit.runner.RunWith;

import static org.junit.Assert.assertEquals;

@RunWith(Enclosed.class)
public class TestReplicationSemanticAnalyzer {
  private static ParseDriver driver = new ParseDriver();

  private static ASTNode parse(String command) throws ParseException {
    return (ASTNode) driver.parse(command).getChild(0);
  }

  private static void assertWithClause(ASTNode root, int replConfigIndex) {
    ASTNode replConfig = (ASTNode) root.getChild(replConfigIndex);
    assertEquals("TOK_REPL_CONFIG", replConfig.getText());
    assertEquals(1, replConfig.getChildCount());
    ASTNode replConfigList = (ASTNode) replConfig.getChild(0);
    assertEquals("TOK_REPL_CONFIG_LIST", replConfigList.getText());
    assertEquals(2, replConfigList.getChildCount());

    assertConfig(replConfigList, 0, "'key.1'", "'value.1'");
    assertConfig(replConfigList, 1, "'key.2'", "'value.2'");
  }

  private static void assertConfig(ASTNode replConfigList, int atIndex, String expectedKey,
      String expectedValue) {
    ASTNode configOne = (ASTNode) replConfigList.getChild(atIndex);
    assertEquals("TOK_TABLEPROPERTY", configOne.getText());
    assertEquals(2, configOne.getChildCount());
    assertEquals(expectedKey, configOne.getChild(0).getText());
    assertEquals(expectedValue, configOne.getChild(1).getText());
  }

  private static void assertToEventId(ASTNode fromClauseRootNode) {
    ASTNode child = (ASTNode) fromClauseRootNode.getChild(1);
    assertEquals("TOK_TO", child.getText());
    assertEquals(0, child.getChildCount());

    child = (ASTNode) fromClauseRootNode.getChild(2);
    assertEquals("200", child.getText());
    assertEquals(0, child.getChildCount());
  }

  private static ASTNode assertFromEvent(final int expectedNumberOfChildren, ASTNode root) {
    ASTNode child = (ASTNode) root.getChild(2);
    assertEquals("TOK_FROM", child.getText());
    assertEquals(child.getChildCount(), expectedNumberOfChildren);

    ASTNode fromClauseChild = (ASTNode) child.getChild(0);
    assertEquals("100", fromClauseChild.getText());
    assertEquals(0, fromClauseChild.getChildCount());
    return child;
  }

  private static void assertTableName(ASTNode root) {
    ASTNode child = (ASTNode) root.getChild(1);
    assertEquals("TOK_TABNAME", child.getText());
    assertEquals(1, child.getChildCount());
    assertEquals("test_table", child.getChild(0).getText());
  }

  private static void assertDatabase(final int expectedNumberOfChildren, ASTNode root) {
    assertEquals("TOK_REPL_DUMP", root.getText());
    assertEquals(expectedNumberOfChildren, root.getChildCount());
    ASTNode child = (ASTNode) root.getChild(0);
    assertEquals("testDb", child.getText());
    assertEquals(0, child.getChildCount());
  }

  public static class ReplDump {

    @Test
    public void parseDbPattern() throws ParseException {
      ASTNode root = parse("repl dump `*`");
      assertEquals("TOK_REPL_DUMP", root.getText());
      assertEquals(1, root.getChildCount());
      ASTNode child = (ASTNode) root.getChild(0);
      assertEquals("`*`", child.getText());
      assertEquals(0, child.getChildCount());
    }

    @Test
    public void parseDb() throws ParseException {
      ASTNode root = parse("repl dump testDb");
      assertDatabase(1, root);
    }

    @Test
    public void parseTableName() throws ParseException {
      ASTNode root = parse("repl dump testDb.test_table");
      assertDatabase(2, root);
      assertTableName(root);
    }

    @Test
    public void parseFromEventId() throws ParseException {
      ASTNode root = parse("repl dump testDb.test_table from 100");
      assertDatabase(3, root);
      assertTableName(root);
      assertFromEvent(1, root);
    }

    @Test
    public void parseToEventId() throws ParseException {
      ASTNode root = parse("repl dump testDb.test_table from 100 to 200");
      assertDatabase(3, root);
      assertTableName(root);
      ASTNode fromClauseRootNode = assertFromEvent(3, root);
      assertToEventId(fromClauseRootNode);
    }

    @Test
    public void parseLimit() throws ParseException {
      ASTNode root = parse("repl dump testDb.test_table from 100 to 200 limit 10");
      assertDatabase(3, root);
      assertTableName(root);
      ASTNode fromClauseRootNode = assertFromEvent(5, root);
      assertToEventId(fromClauseRootNode);

      ASTNode child = (ASTNode) fromClauseRootNode.getChild(3);
      assertEquals("TOK_LIMIT", child.getText());
      assertEquals(0, child.getChildCount());

      child = (ASTNode) fromClauseRootNode.getChild(4);
      assertEquals("10", child.getText());
      assertEquals(0, child.getChildCount());
    }
  }

  public static class ReplDumpWithClause {

    @Test
    public void parseDb() throws ParseException {
      ASTNode root = parse("repl dump testDb with ('key.1'='value.1','key.2'='value.2')");
      assertDatabase(2, root);
      assertWithClause(root, 1);
    }

    @Test
    public void parseTableName() throws ParseException {
      ASTNode root =
          parse("repl dump testDb.test_table with ('key.1'='value.1','key.2'='value.2')");
      assertDatabase(3, root);
      assertTableName(root);
      assertWithClause(root, 2);
    }

    @Test
    public void parseFromEventId() throws ParseException {
      ASTNode root = parse("repl dump testDb.test_table from 100 "
          + "with ('key.1'='value.1','key.2'='value.2')");
      assertDatabase(4, root);
      assertTableName(root);
      assertFromEvent(1, root);
      assertWithClause(root, 3);
    }

    @Test
    public void parseToEventId() throws ParseException {
      ASTNode root = parse("repl dump testDb.test_table from 100 to 200 "
          + "with ('key.1'='value.1','key.2'='value.2')");
      assertDatabase(4, root);
      assertTableName(root);
      ASTNode fromClauseRootNode = assertFromEvent(3, root);
      assertToEventId(fromClauseRootNode);
      assertWithClause(root, 3);
    }

    @Test
    public void parseLimit() throws ParseException {
      ASTNode root = parse("repl dump testDb.test_table from 100 to 200 limit 10 "
          + "with ('key.1'='value.1','key.2'='value.2')");
      assertDatabase(4, root);
      assertTableName(root);
      ASTNode fromClauseRootNode = assertFromEvent(5, root);
      assertToEventId(fromClauseRootNode);
      assertWithClause(root, 3);

      ASTNode child = (ASTNode) fromClauseRootNode.getChild(3);
      assertEquals("TOK_LIMIT", child.getText());
      assertEquals(0, child.getChildCount());

      child = (ASTNode) fromClauseRootNode.getChild(4);
      assertEquals("10", child.getText());
      assertEquals(0, child.getChildCount());
    }
  }

  public static class ReplLoad {

    @Test
    public void parseFromLocation() throws ParseException {
      ASTNode root = parse("repl load  from '/some/location/in/hdfs/'");
      assertFromLocation(1, root);
    }

    @Test
    public void parseTargetDbName() throws ParseException {
      ASTNode root = parse("repl load targetTestDbName from '/some/location/in/hdfs/'");
      assertFromLocation(2, root);
      assertTargetDatabaseName(root);
    }

    @Test
    public void parseWithClause() throws ParseException {
      ASTNode root = parse("repl load targetTestDbName from '/some/location/in/hdfs/'"
          + " with ('mapred.job.queue.name'='repl','hive.repl.approx.max.load.tasks'='100')");
      assertFromLocation(3, root);
      assertTargetDatabaseName(root);

      ASTNode child = (ASTNode) root.getChild(2);
      assertEquals("TOK_REPL_CONFIG", child.getText());
      assertEquals(1, child.getChildCount());
      child = (ASTNode) child.getChild(0);
      assertEquals("TOK_REPL_CONFIG_LIST", child.getText());
      assertEquals(2, child.getChildCount());
      ASTNode configNode = (ASTNode) child.getChild(0);
      assertEquals("TOK_TABLEPROPERTY", configNode.getText());
      assertEquals(2, configNode.getChildCount());
      assertEquals("'mapred.job.queue.name'", configNode.getChild(0).getText());
      assertEquals("'repl'", configNode.getChild(1).getText());
      configNode = (ASTNode) child.getChild(1);
      assertEquals("TOK_TABLEPROPERTY", configNode.getText());
      assertEquals(2, configNode.getChildCount());
      assertEquals("'hive.repl.approx.max.load.tasks'", configNode.getChild(0).getText());
      assertEquals("'100'", configNode.getChild(1).getText());
    }

    private void assertFromLocation(final int expectedNumberOfChildren, ASTNode root) {
      assertEquals("TOK_REPL_LOAD", root.getText());
      assertEquals(expectedNumberOfChildren, root.getChildCount());
      ASTNode child = (ASTNode) root.getChild(0);
      assertEquals("'/some/location/in/hdfs/'", child.getText());
      assertEquals(0, child.getChildCount());
    }

    private void assertTargetDatabaseName(ASTNode root) {
      ASTNode child = (ASTNode) root.getChild(1);
      assertEquals("TOK_DBNAME", child.getText());
      assertEquals(1, child.getChildCount());
      child = (ASTNode) child.getChild(0);
      assertEquals("targetTestDbName", child.getText());
      assertEquals(0, child.getChildCount());
    }
  }

  public static class ReplStatus {

    @Test
    public void parseTargetDbName() throws ParseException {
      ASTNode root = parse("repl status targetTestDbName");
      assertTargetDatabaseName(root);
    }

    @Test
    public void parseWithClause() throws ParseException {
      ASTNode root = parse("repl status targetTestDbName with"
          + "('hive.metastore.uris'='thrift://localhost:12341')");
      assertTargetDatabaseName(root);

      ASTNode child = (ASTNode) root.getChild(1);
      assertEquals("TOK_REPL_CONFIG", child.getText());
      assertEquals(1, child.getChildCount());
      child = (ASTNode) child.getChild(0);
      assertEquals("TOK_REPL_CONFIG_LIST", child.getText());
      ASTNode configNode = (ASTNode) child.getChild(0);
      assertEquals("TOK_TABLEPROPERTY", configNode.getText());
      assertEquals(2, configNode.getChildCount());
      assertEquals("'hive.metastore.uris'", configNode.getChild(0).getText());
      assertEquals("'thrift://localhost:12341'", configNode.getChild(1).getText());
    }

    private void assertTargetDatabaseName(ASTNode root) {
      ASTNode child = (ASTNode) root.getChild(0);
      assertEquals("targetTestDbName", child.getText());
      assertEquals(0, child.getChildCount());
    }
  }
}