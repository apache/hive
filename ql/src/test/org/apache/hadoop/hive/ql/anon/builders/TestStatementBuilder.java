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

package org.apache.hadoop.hive.ql.anon.builders;

import org.apache.hadoop.hive.metastore.api.ColumnInternalFormat;
import org.apache.hadoop.hive.metastore.api.IndexType;
import org.apache.hadoop.hive.ql.anon.TestUtils;
import org.apache.hadoop.hive.ql.anon.FileType;
import org.apache.hadoop.hive.ql.anon.TestBodyType;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


public class TestStatementBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(TestStatementBuilder.class);

  @Test
  public void testBtreeBuilder() {
    final CreateIndexStatementBuilder builder = new CreateIndexStatementBuilder();
    final String cmd = builder
      .withIndexName("ix")
      .withTableName("t")
      .withColumnName("b")
      .withIndexType(IndexType.BTREE)
      .withPageSize(8192)
      .withBufferPoolSize(100)
      .withPointerType("int")
      .build();

    LOG.info(cmd);
    Assertions.assertEquals("CREATE IDENTITY INDEX ix ON TABLE t (b) STORED AS BTREE WITH (PAGE SIZE = 8192, BUFFER POOL SIZE = 100, POINTER TYPE int)", cmd);
  }

  @Test
  public void testBtreeIfNotExistsBuilder() {
    final CreateIndexStatementBuilder builder = new CreateIndexStatementBuilder();
    final String cmd = builder
      .withIndexName("ix")
      .withTableName("t")
      .withColumnName("b")
      .withIndexType(IndexType.BTREE)
      .withPageSize(8192)
      .withBufferPoolSize(100)
      .withPointerType("int")
      .withIfNotExists()
      .build();

    LOG.info(cmd);
    Assertions.assertEquals("CREATE IDENTITY INDEX IF NOT EXISTS ix ON TABLE t (b) STORED AS BTREE WITH (PAGE SIZE = 8192, BUFFER POOL SIZE = 100, POINTER TYPE int)", cmd);
  }

  @Test
  public void testDirectoryBuilder() {
    final CreateIndexStatementBuilder builder = new CreateIndexStatementBuilder();
    final String cmd = builder
      .withIndexName("ix")
      .withTableName("t")
      .withColumnName("b")
      .withIndexType(IndexType.DIRECTORY)
      .withPointerType("int")
      .build();

    LOG.info(cmd);
    Assertions.assertEquals("CREATE IDENTITY INDEX ix ON TABLE t (b) STORED AS DIRECTORY WITH (POINTER TYPE int)", cmd);
  }

  @Test
  public void testTabularBuilder() {
    final CreateIndexStatementBuilder builder = new CreateIndexStatementBuilder();
    final String cmd = builder
      .withIndexName("ix")
      .withTableName("t")
      .withColumnName("b")
      .withIndexType(IndexType.TABULAR)
      .build();

    LOG.info(cmd);
    Assertions.assertEquals("CREATE IDENTITY INDEX ix ON TABLE t (b) STORED AS TABULAR", cmd);
  }

  @Test
  public void testAlterIndexBuilder() {
    final String cmd = new AlterIndexStatementBuilder("ix", "tab").build();
    LOG.info(cmd);
    Assertions.assertEquals("ALTER IDENTITY INDEX ix ON TABLE tab REBUILD", cmd);
  }

  @Test
  public void testLoadPolicyBuilder() {
    final CreateDataErasurePolicyStatementBuilder builder = new CreateDataErasurePolicyStatementBuilder();
    final String cmd = builder
      .withPolicyName("p1")
      .withPolicySource(TestUtils.getTestPolicyDsl())
      .withIfNotExists()
      .build();

    LOG.info(cmd);
    Assertions.assertTrue(cmd.startsWith("LOAD ERASURE POLICY p1 FROM '")
        && cmd.endsWith(".erp'"),
        "expected LOAD ... FROM '<tempfile>.erp', got: " + cmd);
  }

  @Test
  public void testAnonymizeBuilder() {
    final String cmd = new EraseStatementBuilder("tab").withIds(1, 2, 3).build();
    LOG.info(cmd);
    Assertions.assertEquals("ERASE FROM TABLE tab FOR IDENTITY VALUES (1, 2, 3)", cmd);
  }

  @Test
  public void testDropIndexBuilder() {
    final DropIndexStatementBuilder builder = new DropIndexStatementBuilder("ix", "tbl");
    final String cmd = builder.withIfExists().build();

    LOG.info(cmd);
    Assertions.assertEquals("DROP IDENTITY INDEX IF EXISTS ix ON tbl", cmd);
  }

  @Test
  public void testCreateTableBuilder() {
    final CreateTableStatementBuilder builder = new CreateTableStatementBuilder("tbl", "m", "o", "b");
    final String cmd = builder.withIfNotExists().withInternalFormat(ColumnInternalFormat.JSON).withFileType(FileType.ORC).build();

    LOG.info(cmd);
    Assertions.assertEquals("CREATE TABLE IF NOT EXISTS tbl (m int, o bigint, b STRING) STORED AS ORC", cmd);
  }

  @Test
  public void testDropTableBuilder() {
    final DropTableStatementBuilder builder = new DropTableStatementBuilder("tbl");
    final String cmd = builder.withIfExists().build();

    LOG.info(cmd);
    Assertions.assertEquals("DROP TABLE IF EXISTS tbl", cmd);
  }

  @Test
  public void testDropPolicyBuilder() {
    final DropPolicyStatementBuilder builder = new DropPolicyStatementBuilder("policy1");
    final String cmd = builder.withIfExists().build();

    LOG.info(cmd);
    Assertions.assertEquals("DROP DATA ERASURE POLICY IF EXISTS policy1", cmd);
  }

  @Test
  public void testSelectBuilder() {
    final SelectStatementBuilder builder = new SelectStatementBuilder("t", "m", "o", "b", ColumnInternalFormat.JSON);
    final String cmd = builder.build();

    LOG.info(cmd);
    Assertions.assertEquals("SELECT m, o, b FROM t", cmd);
  }

  @Test
  public void testSelectHexBuilder() {
    final SelectStatementBuilder builder = new SelectStatementBuilder("t", "m", "o", "b", ColumnInternalFormat.MSGPACK);
    final String cmd = builder.build();

    LOG.info(cmd);
    Assertions.assertEquals("SELECT m, o, HEX(b) FROM t", cmd);
  }

  @Test
  public void testInsertBuilder() {
    final InsertStatementBuilder builder = new InsertStatementBuilder(1, "t", 1, 11, "val", TestBodyType.STRING);
    final String cmd = builder.build();

    LOG.info(cmd);
    Assertions.assertEquals("INSERT INTO TABLE t VALUES (1,11,'val')", cmd);
  }


  @Test
  public void testExplainAttachPolicyBuilder() {
    final String cmd = new ExplainAttachPolicyStatementBuilder()
      .withPolicies("p1")
      .withTableName("t")
      .withColumnName("b")
      .build();

    LOG.info(cmd);
    Assertions.assertEquals("EXPLAIN ATTACH DATA ERASURE POLICY p1 ON TABLE t COLUMN b", cmd);
  }

  @Test
  public void testExplainAttachPolicyMultiBuilder() {
    final String cmd = new ExplainAttachPolicyStatementBuilder()
      .withPolicies("p1", "p2", "p3")
      .withTableName("t")
      .withColumnName("b")
      .withBindingOpts("m", "o", ColumnInternalFormat.JSON,
                       ExplainAttachPolicyStatementBuilder.ResolutionMode.STRICTEST)
      .build();

    LOG.info(cmd);
    Assertions.assertEquals(
      "EXPLAIN ATTACH DATA ERASURE POLICY p1, p2, p3 ON TABLE t COLUMN b "
        + "WITH (SCHEMA FIELD (m), ROW LOCATOR (o), COLUMN FORMAT (JSON)) "
        + "RESOLUTION (STRICTEST)",
      cmd);
  }

  @Test
  public void testValidateErasureBindingBuilder() {
    final String cmd = new ValidateErasureBindingStatementBuilder("t", "b").build();
    LOG.info(cmd);
    Assertions.assertEquals("VALIDATE ERASURE BINDING ON TABLE t COLUMN b", cmd);
  }

  @Test
  public void testAuditErasurePolicyBuilder() {
    final String cmd = new AuditErasurePolicyStatementBuilder("p1").build();
    LOG.info(cmd);
    Assertions.assertEquals("AUDIT ERASURE POLICY p1", cmd);
  }

  @Test
  public void testAuditErasurePolicyBetweenBuilder() {
    final String cmd = new AuditErasurePolicyStatementBuilder("p1")
      .withTimeRange(TimeRange.between("2024-01-01", "2024-06-30"))
      .build();
    LOG.info(cmd);
    Assertions.assertEquals(
      "AUDIT ERASURE POLICY p1 FROM '2024-01-01' UNTIL '2024-06-30'", cmd);
  }

  @Test
  public void testAuditErasureBindingBuilder() {
    final String cmd = new AuditErasureBindingStatementBuilder("t", "b")
      .withTimeRange(TimeRange.from("2024-01-01"))
      .build();
    LOG.info(cmd);
    Assertions.assertEquals(
      "AUDIT ERASURE BINDING ON TABLE t COLUMN b FROM '2024-01-01'", cmd);
  }

  @Test
  public void testAuditErasureExecutionsBuilder() {
    final String cmd = new AuditErasureExecutionsStatementBuilder("t").build();
    LOG.info(cmd);
    Assertions.assertEquals("AUDIT ERASURE EXECUTIONS ON TABLE t", cmd);
  }

  @Test
  public void testAuditErasureExecutionsFullBuilder() {
    final String cmd = new AuditErasureExecutionsStatementBuilder("t")
      .withTimeRange(TimeRange.between("2024-01-01", "2024-06-30"))
      .withByUser("alice")
      .withForIdentity(42)
      .build();
    LOG.info(cmd);
    Assertions.assertEquals(
      "AUDIT ERASURE EXECUTIONS ON TABLE t FROM '2024-01-01' UNTIL '2024-06-30' "
        + "BY USER alice FOR IDENTITY 42",
      cmd);
  }

  @Test
  public void testAuditErasureConflictsBuilder() {
    final String cmd = new AuditErasureConflictsStatementBuilder()
      .withTimeRange(TimeRange.until("2024-06-30"))
      .build();
    LOG.info(cmd);
    Assertions.assertEquals("AUDIT ERASURE CONFLICTS UNTIL '2024-06-30'", cmd);
  }

  @Test
  public void testAuditErasureComplianceBuilder() {
    final String cmd = new AuditErasureComplianceStatementBuilder()
      .withAsOf("2024-12-31")
      .build();
    LOG.info(cmd);
    Assertions.assertEquals("AUDIT ERASURE COMPLIANCE AS OF '2024-12-31'", cmd);
  }

  @Test
  public void testAuditErasureComplianceCurrentBuilder() {
    final String cmd = new AuditErasureComplianceStatementBuilder().build();
    LOG.info(cmd);
    Assertions.assertEquals("AUDIT ERASURE COMPLIANCE", cmd);
  }

}
