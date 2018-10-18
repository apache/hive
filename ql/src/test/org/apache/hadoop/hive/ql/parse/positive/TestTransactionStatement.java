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
package org.apache.hadoop.hive.ql.parse.positive;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;

/**
 * Basic parser tests for multi-statement transactions
 */
public class TestTransactionStatement {
  private static SessionState sessionState;
  private ParseDriver pd;

  @BeforeClass
  public static void initialize() {
    HiveConf conf = new HiveConf(SemanticAnalyzer.class);
    sessionState = SessionState.start(conf);
  }
  @AfterClass
  public static void cleanUp() throws IOException {
    if(sessionState != null) {
      sessionState.close();
    }
  }

  @Before
  public void setup() throws SemanticException {
    pd = new ParseDriver();
  }

  ASTNode parse(String query) throws ParseException {
    ASTNode nd = pd.parse(query);
    return (ASTNode) nd.getChild(0);
  }
  @Test
  public void testTxnStart() throws ParseException {
    ASTNode ast = parse("START TRANSACTION");
    Assert.assertEquals("AST doesn't match",
      "tok_start_transaction", ast.toStringTree());

    ast = parse("START TRANSACTION ISOLATION LEVEL SNAPSHOT");
    Assert.assertEquals("AST doesn't match",
      "(tok_start_transaction (tok_isolation_level tok_isolation_snapshot))", ast.toStringTree());

    ast = parse("START TRANSACTION READ ONLY");
    Assert.assertEquals("AST doesn't match",
      "(tok_start_transaction (tok_txn_access_mode tok_txn_read_only))", ast.toStringTree());

    ast = parse("START TRANSACTION READ WRITE, ISOLATION LEVEL SNAPSHOT");
    Assert.assertEquals("AST doesn't match",
      "(tok_start_transaction (tok_txn_access_mode tok_txn_read_write) (tok_isolation_level tok_isolation_snapshot))", ast.toStringTree());

  }
  @Test
  public void testTxnCommitRollback() throws ParseException {
    ASTNode ast = parse("COMMIT");
    Assert.assertEquals("AST doesn't match", "tok_commit", ast.toStringTree());
    ast = parse("COMMIT WORK");
    Assert.assertEquals("AST doesn't match", "tok_commit", ast.toStringTree());
    ast = parse("ROLLBACK");
    Assert.assertEquals("AST doesn't match", "tok_rollback", ast.toStringTree());
    ast = parse("ROLLBACK WORK");
    Assert.assertEquals("AST doesn't match", "tok_rollback", ast.toStringTree());
  }

  @Test
  public void testAutoCommit() throws ParseException {
    ASTNode ast = parse("SET AUTOCOMMIT TRUE");
    Assert.assertEquals("AST doesn't match", "(tok_set_autocommit tok_true)", ast.toStringTree());
    ast = parse("SET AUTOCOMMIT FALSE");
    Assert.assertEquals("AST doesn't match", "(tok_set_autocommit tok_false)", ast.toStringTree());
  }
}
