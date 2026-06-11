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
package org.apache.hadoop.hive.metastore.tools.schematool;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

import org.apache.hadoop.hive.metastore.HiveMetaException;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Oracle;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

/**
 * Integration tests for {@link OracleIndexRebuilder}.
 *
 * <p>Covers index discovery and rebuild behavior, including FK-backed PKs.
 * Oracle DDL auto-commits; {@code @After} rollback affects only DML.
 * Unquoted identifiers are stored uppercase, so assertions use uppercase names.
 */
@Category(MetastoreCheckinTest.class)
public class TestOracleIndexRebuilder {

  @ClassRule
  public static final Oracle oracle = new Oracle();

  // Dedicated test user keeps USER_INDEXES scoped to test objects.
  private static final String TEST_USER = "IDX_TEST_USER";
  private static final String TEST_PASSWORD = "TestPass1";

  private static Connection conn;

  // Names are uppercase in the catalog for unquoted identifiers.
  private static final String[] DDL_CREATE_TABLES = {
      "CREATE TABLE PK_TABLE ("
          + "  ID NUMBER NOT NULL,"
          + "  NAME VARCHAR2(256),"
          + "  CONSTRAINT PK_TABLE_PK PRIMARY KEY (ID)"
          + ")",
      "CREATE TABLE UNIQUE_TABLE ("
          + "  ID NUMBER,"
          + "  NAME VARCHAR2(256)"
          + ")",
      "CREATE UNIQUE INDEX IDX_UNIQUE_NAME ON UNIQUE_TABLE (NAME)",
      "CREATE TABLE PLAIN_TABLE ("
          + "  ID NUMBER,"
          + "  NAME VARCHAR2(256)"
          + ")",
      "CREATE INDEX IDX_PLAIN_NAME ON PLAIN_TABLE (NAME)",
      "CREATE TABLE MULTI_COL_TABLE ("
          + "  PART_NAME VARCHAR2(256),"
          + "  TBL_ID NUMBER,"
          + "  VALUE VARCHAR2(256)"
          + ")",
      "CREATE UNIQUE INDEX IDX_MULTI_COL ON MULTI_COL_TABLE (PART_NAME, TBL_ID)",
      "CREATE TABLE FK_PARENT_TABLE ("
          + "  ID NUMBER NOT NULL,"
          + "  VAL VARCHAR2(256),"
          + "  CONSTRAINT FK_PARENT_PK PRIMARY KEY (ID)"
          + ")",
      "CREATE TABLE FK_CHILD_TABLE ("
          + "  ID NUMBER,"
          + "  PARENT_ID NUMBER,"
          + "  CONSTRAINT FK_CHILD_FK FOREIGN KEY (PARENT_ID)"
          + "    REFERENCES FK_PARENT_TABLE (ID)"
          + ")",
      // Inline PRIMARY KEY creates an auto-named index/constraint (SYS_Cxxx).
      "CREATE TABLE INLINE_PK_TABLE ("
          + "  ID NUMBER PRIMARY KEY,"
          + "  NAME VARCHAR2(256)"
          + ")",
      // Inline UNIQUE also creates an auto-named index/constraint (SYS_Cxxx).
      "CREATE TABLE INLINE_UNIQUE_TABLE ("
          + "  ID NUMBER,"
          + "  NAME VARCHAR2(256),"
          + "  UNIQUE (NAME)"
          + ")"
  };

  private OracleIndexRebuilder rebuilder;

  @BeforeClass
  public static void setUpClass() throws Exception {
    Class.forName(oracle.getJdbcDriver());

    try (Connection sysConn = DriverManager.getConnection(
        oracle.getInitialJdbcUrl(), oracle.getDbRootUser(), oracle.getDbRootPassword());
        Statement stmt = sysConn.createStatement()) {
      try {
        stmt.execute("DROP USER " + TEST_USER + " CASCADE");
      } catch (SQLException ignored) {
        // Ignore when user does not exist.
      }
      stmt.execute("CREATE USER " + TEST_USER + " IDENTIFIED BY " + TEST_PASSWORD);
      stmt.execute("GRANT CONNECT, RESOURCE TO " + TEST_USER);
      stmt.execute("GRANT UNLIMITED TABLESPACE TO " + TEST_USER);
    }

    // Reconnect as test user.
    String userUrl = oracle.getInitialJdbcUrl();
    conn = DriverManager.getConnection(userUrl, TEST_USER, TEST_PASSWORD);
    conn.setAutoCommit(false);

    for (String ddl : DDL_CREATE_TABLES) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
      }
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (conn != null) {
      conn.close();
    }
    try (Connection sysConn = DriverManager.getConnection(
        oracle.getInitialJdbcUrl(), oracle.getDbRootUser(), oracle.getDbRootPassword());
        Statement stmt = sysConn.createStatement()) {
      stmt.execute("DROP USER " + TEST_USER + " CASCADE");
    }
  }

  @Before
  public void setUp() {
    rebuilder = new OracleIndexRebuilder(conn, true, "\"");
  }

  @After
  public void tearDown() throws Exception {
    conn.rollback();
  }

  // -------------------------------------------------------------------------
  // Query correctness — loadIndexes
  // -------------------------------------------------------------------------

  @Test
  public void primaryKeyIndexIsUniqueAndNotConstraintBacked() throws Exception {
    IndexInfo pk = findByName("PK_TABLE_PK");
    assertNotNull("PK-backing index should be present", pk);
    assertEquals("PK_TABLE", pk.tableName());
    assertTrue("PK-backing index should be unique", pk.unique());
    assertFalse("Oracle always returns constraintBacked=false", pk.constraintBacked());
  }

  @Test
  public void uniqueIndexIsUniqueAndNotConstraintBacked() throws Exception {
    IndexInfo idx = findByName("IDX_UNIQUE_NAME");
    assertNotNull("UNIQUE index should be present", idx);
    assertEquals("UNIQUE_TABLE", idx.tableName());
    assertTrue(idx.unique());
    assertFalse(idx.constraintBacked());
  }

  @Test
  public void nonUniqueIndexIsNotUnique() throws Exception {
    IndexInfo idx = findByName("IDX_PLAIN_NAME");
    assertNotNull("Non-unique index should be present", idx);
    assertEquals("PLAIN_TABLE", idx.tableName());
    assertFalse(idx.unique());
    assertFalse(idx.constraintBacked());
  }

  @Test
  public void multiColumnIndexColumnsReturnedInDefinitionOrder() throws Exception {
    IndexInfo idx = findByName("IDX_MULTI_COL");
    assertNotNull(idx);
    assertEquals(List.of("PART_NAME", "TBL_ID"), idx.columns());
  }

  @Test
  public void describeDdlUsesAlterIndexRebuild() throws Exception {
    IndexInfo idx = findByName("IDX_PLAIN_NAME");
    String ddl = rebuilder.describeRebuildDDL(idx).toUpperCase();
    assertTrue("DDL should use ALTER INDEX", ddl.contains("ALTER INDEX"));
    assertTrue("DDL should use REBUILD", ddl.contains("REBUILD"));
    assertFalse("DDL should not use DROP INDEX", ddl.contains("DROP INDEX"));
    assertFalse("DDL should not use CREATE INDEX", ddl.contains("CREATE INDEX"));
  }

  // -------------------------------------------------------------------------
  // Rebuild correctness — each index type
  // -------------------------------------------------------------------------

  @Test
  public void rebuildPrimaryKeyIndexExistsAfterRebuild() throws Exception {
    IndexInfo pk = findByName("PK_TABLE_PK");
    assertNotNull(pk);
    rebuilder.rebuildIndex(pk);
    assertTrue("PK-backing index should exist after rebuild", indexExists("PK_TABLE_PK"));
  }

  @Test
  public void rebuildUniqueIndexExistsAndEnforcesUniquenessAfterRebuild() throws Exception {
    IndexInfo idx = findByName("IDX_UNIQUE_NAME");
    rebuilder.rebuildIndex(idx);
    assertTrue("UNIQUE index should exist after rebuild", indexExists("IDX_UNIQUE_NAME"));
    assertTrue("Uniqueness should be enforced after rebuild",
        uniquenessEnforced("UNIQUE_TABLE", "NAME"));
  }

  @Test
  public void rebuildNonUniqueIndexExistsAfterRebuild() throws Exception {
    IndexInfo idx = findByName("IDX_PLAIN_NAME");
    rebuilder.rebuildIndex(idx);
    assertTrue("Non-unique index should exist after rebuild", indexExists("IDX_PLAIN_NAME"));
  }

  @Test
  public void rebuildMultiColumnUniqueIndexEnforcesUniquenessAfterRebuild() throws Exception {
    IndexInfo idx = findByName("IDX_MULTI_COL");
    assertNotNull(idx);
    assertTrue(idx.unique());
    assertEquals(List.of("PART_NAME", "TBL_ID"), idx.columns());

    rebuilder.rebuildIndex(idx);
    assertTrue("Multi-column index should exist after rebuild", indexExists("IDX_MULTI_COL"));
    assertTrue("Uniqueness should be enforced on multi-column index after rebuild",
        uniquenessEnforcedMultiCol("MULTI_COL_TABLE", "PART_NAME", "TBL_ID"));
  }

  @Test
  public void inlinePrimaryKeyCreatesSystemNamedIndexThatIsUnique() throws Exception {
    // Inline PRIMARY KEY should appear with an auto-generated index name.
    IndexInfo pk = findByTable("INLINE_PK_TABLE");
    assertNotNull("System-named PK index should be present in USER_INDEXES", pk);
    assertEquals("INLINE_PK_TABLE", pk.tableName());
    assertTrue("Auto-named PK backing index should be unique", pk.unique());
  }

  @Test
  public void inlineUniqueConstraintCreatesSystemNamedIndexThatIsUnique() throws Exception {
    // Inline UNIQUE should appear with an auto-generated index name.
    IndexInfo idx = findByTable("INLINE_UNIQUE_TABLE");
    assertNotNull("System-named UNIQUE constraint index should be present in USER_INDEXES", idx);
    assertEquals("INLINE_UNIQUE_TABLE", idx.tableName());
    assertTrue("Auto-named UNIQUE constraint backing index should be unique", idx.unique());
  }

  @Test
  public void rebuildSystemNamedPrimaryKeyIndexSucceeds() throws Exception {
    IndexInfo pk = findByTable("INLINE_PK_TABLE");
    assertNotNull(pk);
    rebuilder.rebuildIndex(pk);
    assertTrue("System-named PK index should exist after rebuild", indexExists(pk.indexName()));
  }

  @Test
  public void rebuildSystemNamedUniqueIndexSucceeds() throws Exception {
    IndexInfo idx = findByTable("INLINE_UNIQUE_TABLE");
    assertNotNull(idx);
    rebuilder.rebuildIndex(idx);
    assertTrue("System-named UNIQUE index should exist after rebuild",
        indexExists(idx.indexName()));
  }

  @Test
  public void rebuildFkReferencedPrimaryKeySucceedsWithAlterIndexRebuild() throws Exception {
    // ALTER INDEX REBUILD is in-place and keeps FK references intact.
    IndexInfo pk = findByName("FK_PARENT_PK");
    assertNotNull("FK-backed PK index should be present", pk);

    rebuilder.rebuildIndex(pk);
    assertTrue("PK-backing index should exist after in-place rebuild",
        indexExists("FK_PARENT_PK"));
  }

  // -------------------------------------------------------------------------
  // findDuplicates
  // -------------------------------------------------------------------------

  @Test
  public void findDuplicatesNonUniqueIndexReturnsZeroWithoutQueryingDb() throws Exception {
    Connection closedConn = DriverManager.getConnection(
        oracle.getInitialJdbcUrl(), TEST_USER, TEST_PASSWORD);
    closedConn.close();
    OracleIndexRebuilder localRebuilder = new OracleIndexRebuilder(closedConn, true, "\"");
    IndexInfo nonUnique = new IndexInfo("IDX_PLAIN_NAME", "PLAIN_TABLE", false, false,
        List.of("NAME"));
    assertEquals(0, localRebuilder.findDuplicates(nonUnique));
  }

  @Test
  public void findDuplicatesTableWithDuplicateValuesReturnsPositiveCount() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSERT INTO PLAIN_TABLE VALUES (1, 'alice')");
      stmt.execute("INSERT INTO PLAIN_TABLE VALUES (2, 'alice')");
    }
    IndexInfo idx = new IndexInfo("FAKE_IDX", "PLAIN_TABLE", true, false, List.of("NAME"));
    assertTrue("Should detect duplicate NAME values", rebuilder.findDuplicates(idx) > 0);
  }

  @Test
  public void findDuplicatesTableWithNoDuplicatesReturnsZero() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSERT INTO PLAIN_TABLE VALUES (1, 'alice')");
      stmt.execute("INSERT INTO PLAIN_TABLE VALUES (2, 'bob')");
    }
    IndexInfo idx = new IndexInfo("FAKE_IDX", "PLAIN_TABLE", true, false, List.of("NAME"));
    assertEquals(0, rebuilder.findDuplicates(idx));
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private Map<String, IndexInfo> loadAllByName() throws HiveMetaException {
    return rebuilder.loadIndexes()
        .stream()
        .collect(Collectors.toMap(IndexInfo::indexName, Function.identity()));
  }

  private IndexInfo findByName(String indexName) throws HiveMetaException {
    return loadAllByName().get(indexName);
  }

  /** Finds the first index on the given table (for system-named indexes). */
  private IndexInfo findByTable(String tableName) throws HiveMetaException {
    return rebuilder.loadIndexes().stream()
        .filter(i -> i.tableName().equals(tableName))
        .findFirst()
        .orElse(null);
  }

  /** Returns true if the named index exists in USER_INDEXES. */
  private boolean indexExists(String indexName) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(
        "SELECT 1 FROM USER_INDEXES WHERE INDEX_NAME = ?")) {
      ps.setString(1, indexName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  /** Returns true when duplicate inserts fail with SQLSTATE 23000. */
  private boolean uniquenessEnforced(String table, String column) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSERT INTO " + table + " (" + column + ") VALUES ('__dup_test__')");
      stmt.execute("INSERT INTO " + table + " (" + column + ") VALUES ('__dup_test__')");
      conn.rollback();
      return false;
    } catch (SQLException e) {
      conn.rollback();
      return "23000".equals(e.getSQLState());
    }
  }

  private boolean uniquenessEnforcedMultiCol(String table, String col1, String col2)
      throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute(
          "INSERT INTO " + table + " (" + col1 + ", " + col2 + ") VALUES ('p', 1)");
      stmt.execute(
          "INSERT INTO " + table + " (" + col1 + ", " + col2 + ") VALUES ('p', 1)");
      conn.rollback();
      return false;
    } catch (SQLException e) {
      conn.rollback();
      return "23000".equals(e.getSQLState());
    }
  }
}

