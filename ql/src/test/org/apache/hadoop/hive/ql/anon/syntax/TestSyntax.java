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

package org.apache.hadoop.hive.ql.anon.syntax;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.anon.TestUtils;
import org.apache.hadoop.hive.ql.anon.builders.AlterIndexStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.EraseStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.CreateDataErasurePolicyStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.CreateIndexStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.CreateTableStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.DropIndexStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.DropPolicyStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.DropTableStatementBuilder;
import org.apache.hadoop.hive.ql.anon.builders.SelectStatementBuilder;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.tez.dag.api.TezConfiguration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

@TestInstance(TestInstance.Lifecycle.PER_CLASS)
public class TestSyntax {

  private static final Logger LOG = LoggerFactory.getLogger(TestSyntax.class);
  private HiveConf conf;

  @BeforeAll
  public void before() throws Exception {
    conf = new HiveConf();
    conf.set("hive.user.install.directory", "/tmp");
    conf.setVar(HiveConf.ConfVars.HIVE_AUTHORIZATION_MANAGER, "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdHiveAuthorizerFactory");
    conf.setVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE, "tez");
    conf.setBoolean(TezConfiguration.TEZ_LOCAL_MODE, true);
    SessionState.start(conf);
  }

  @AfterAll
  public void after() throws Exception {
    SessionState.get().close();
  }

  @Test
  public void testCreateBtreeIndex() {
    Assertions.assertDoesNotThrow(
      () -> {
        final String cmd = new CreateIndexStatementBuilder("ix", "tab", "body").withBtreeOptions(8192, 10, "int").build();
        final ASTNode command = ParseUtils.parse(cmd, new Context(conf));
      }
    );
  }

  @Test
  public void testCreateDirectoryIndex() throws ParseException {
    Assertions.assertDoesNotThrow(
      () -> {
        final String cmd = new CreateIndexStatementBuilder("ix", "tab", "body").withDirectoryOptions("int").build();
        final ASTNode command = ParseUtils.parse(cmd, new Context(conf));
      }
    );
  }

  @Test
  public void testCreateTabularIndex() throws ParseException {
    Assertions.assertDoesNotThrow(
      () -> {
        final String cmd = new CreateIndexStatementBuilder("ix", "tab", "body").withTabularOptions().build();
        final ASTNode command = ParseUtils.parse(cmd, new Context(conf));
      }
    );
  }

  @Test
  public void testAlterIndex() throws ParseException {
    Assertions.assertDoesNotThrow(
      () -> {
        final String cmd = new AlterIndexStatementBuilder("ix", "tab").build();
        final ASTNode command = ParseUtils.parse(cmd, new Context(conf));
      }
    );
  }

  @Test
  public void testCreatePolicy() throws ParseException {
    Assertions.assertDoesNotThrow(
      () -> {
        final String cmd = new CreateDataErasurePolicyStatementBuilder()
          .withPolicyName("p1")
          .withPolicySource(TestUtils.getTestPolicyDsl())
          .withIfNotExists()
          .build();
        final ASTNode command = ParseUtils.parse(cmd, new Context(conf));
      }
    );
  }

  @Test
  public void testAnonymizeTable() throws ParseException {
    Assertions.assertDoesNotThrow(
      () -> {
        final String cmd = new EraseStatementBuilder("t").withIds(1, 2, 3).build();
        final ASTNode command = ParseUtils.parse(cmd, new Context(conf));
      }
    );
  }

  @Test
  public void testDropIndex() throws ParseException {
    Assertions.assertDoesNotThrow(
      () -> {
        final String cmd = new DropIndexStatementBuilder("ix", "tbl").withIfExists().build();
        final ASTNode command = ParseUtils.parse(cmd, new Context(conf));
      }
    );
  }

  @Test
  public void testCreateTable() throws ParseException {
    Assertions.assertDoesNotThrow(
      () -> {
        final String cmd = new CreateTableStatementBuilder("tbl", "m", "o", "b")
          .withIfNotExists()
          .withInternalFormat(ColumnInternalFormat.JSON)
          .build();
        final ASTNode command = ParseUtils.parse(cmd, new Context(conf));
      }
    );
  }

  @Test
  public void testDropTable() throws ParseException {
    Assertions.assertDoesNotThrow(
      () -> {
        final String cmd = new DropTableStatementBuilder("tbl").withIfExists().build();
        final ASTNode command = ParseUtils.parse(cmd, new Context(conf));
      }
    );
  }

  @Test
  public void testDropPolicy() throws ParseException {
    Assertions.assertDoesNotThrow(
      () -> {
        final String cmd = new DropPolicyStatementBuilder("policy1").withIfExists().build();
        LOG.info(cmd);
        final ASTNode command = ParseUtils.parse(cmd, new Context(conf));
      }
    );
  }

  @Test
  public void testSelect() throws ParseException {
    Assertions.assertDoesNotThrow(
      () -> {
        final String cmd = new SelectStatementBuilder("t", "m", "o", "b", ColumnInternalFormat.AVRO).build();
        LOG.info(cmd);
        final ASTNode command = ParseUtils.parse(cmd, new Context(conf));
      }
    );
  }
}
