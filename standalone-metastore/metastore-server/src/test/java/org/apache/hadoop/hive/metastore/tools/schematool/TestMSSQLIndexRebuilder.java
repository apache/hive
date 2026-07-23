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
import org.apache.hadoop.hive.metastore.dbinstall.rules.Mssql;
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
 * Integration tests for {@link MSSQLIndexRebuilder}.
 *
 * <p>Covers index discovery and rebuild behavior, including INCLUDE columns and FK-backed PKs.
 * SQL Server DDL is committed in {@code @BeforeClass}; {@code @After} rollback affects only DML.
 */
@Category(MetastoreCheckinTest.class)
public class TestMSSQLIndexRebuilder {

  @ClassRule
  public static final Mssql MSSQL = new Mssql();

  private static final String TEST_DB = "idx_rebuild_test";

  private static Connection conn;

  // Includes PK, UNIQUE, non-unique, multi-column, covering, and FK-backed PK cases.
  private static final String[] DDL_CREATE_TABLES = {
      "CREATE TABLE pk_table (id BIGINT NOT NULL, name NVARCHAR(256), "
          + "CONSTRAINT pk_table_pk PRIMARY KEY (id))",
      "CREATE TABLE unique_table (id BIGINT, name NVARCHAR(256))",
      "CREATE UNIQUE INDEX idx_unique_name ON unique_table (name)",
      "CREATE TABLE plain_table (id BIGINT, name NVARCHAR(256))",
      "CREATE INDEX idx_plain_name ON plain_table (name)",
      "CREATE TABLE multi_col_table (part_name NVARCHAR(256), tbl_id BIGINT, val NVARCHAR(256))",
      "CREATE UNIQUE INDEX idx_multi_col ON multi_col_table (part_name, tbl_id)",
      "CREATE TABLE covering_table (id BIGINT, key_col NVARCHAR(256), payload NVARCHAR(256))",
      // INCLUDE column should not appear in idx.columns().
      "CREATE INDEX idx_covering ON covering_table (key_col) INCLUDE (payload)",
      "CREATE TABLE fk_parent_table (id BIGINT NOT NULL, val NVARCHAR(256), "
          + "CONSTRAINT fk_parent_pk PRIMARY KEY (id))",
      "CREATE TABLE fk_child_table (id BIGINT, parent_id BIGINT, "
          + "CONSTRAINT fk_child_fk FOREIGN KEY (parent_id) REFERENCES fk_parent_table (id))"
  };

  private MSSQLIndexRebuilder rebuilder;

  @BeforeClass
  public static void setUpClass() throws Exception {
    Class.forName(MSSQL.getJdbcDriver());

    try (Connection masterConn = DriverManager.getConnection(
        MSSQL.getInitialJdbcUrl(), MSSQL.getDbRootUser(), MSSQL.getDbRootPassword());
        Statement stmt = masterConn.createStatement()) {
      stmt.execute("IF DB_ID('" + TEST_DB + "') IS NOT NULL DROP DATABASE " + TEST_DB);
      stmt.execute("CREATE DATABASE " + TEST_DB);
    }

    // Reconnect to the test database.
    String testDbUrl = MSSQL.getInitialJdbcUrl()
        .replace("DatabaseName=master", "DatabaseName=" + TEST_DB);
    conn = DriverManager.getConnection(testDbUrl, MSSQL.getDbRootUser(), MSSQL.getDbRootPassword());
    conn.setAutoCommit(false);

    for (String ddl : DDL_CREATE_TABLES) {
      try (Statement stmt = conn.createStatement()) {
        stmt.execute(ddl);
      }
    }
    conn.commit();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    if (conn != null) {
      conn.close();
    }
    try (Connection masterConn = DriverManager.getConnection(
        MSSQL.getInitialJdbcUrl(), MSSQL.getDbRootUser(), MSSQL.getDbRootPassword());
        Statement stmt = masterConn.createStatement()) {
      stmt.execute("IF DB_ID('" + TEST_DB + "') IS NOT NULL DROP DATABASE " + TEST_DB);
    }
  }

  @Before
  public void setUp() {
    rebuilder = new MSSQLIndexRebuilder(conn, false, "\"");
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
    IndexInfo pk = findByTableAndIndex("pk_table", "pk_table_pk");
    assertNotNull("PK-backing index should be present", pk);
    assertEquals("pk_table", pk.tableName());
    assertTrue("PK-backing index should be unique", pk.unique());
    assertFalse("MSSQL always returns constraintBacked=false", pk.constraintBacked());
  }

  @Test
  public void uniqueIndexIsUniqueAndNotConstraintBacked() throws Exception {
    IndexInfo idx = findByTableAndIndex("unique_table", "idx_unique_name");
    assertNotNull("UNIQUE index should be present", idx);
    assertTrue(idx.unique());
    assertFalse(idx.constraintBacked());
  }

  @Test
  public void nonUniqueIndexIsNotUnique() throws Exception {
    IndexInfo idx = findByTableAndIndex("plain_table", "idx_plain_name");
    assertNotNull("Non-unique index should be present", idx);
    assertFalse(idx.unique());
    assertFalse(idx.constraintBacked());
  }

  @Test
  public void multiColumnIndexColumnsReturnedInDefinitionOrder() throws Exception {
    IndexInfo idx = findByTableAndIndex("multi_col_table", "idx_multi_col");
    assertNotNull(idx);
    assertEquals(List.of("part_name", "tbl_id"), idx.columns());
  }

  @Test
  public void coveringIndexExcludesIncludeColumns() throws Exception {
    // INCLUDE columns are not key columns.
    IndexInfo idx = findByTableAndIndex("covering_table", "idx_covering");
    assertNotNull("Covering index should be present", idx);
    assertEquals("Only key column should appear; INCLUDE column must be excluded",
        List.of("key_col"), idx.columns());
  }

  @Test
  public void describeDdlUsesAlterIndexRebuildWithTableName() throws Exception {
    IndexInfo idx = findByTableAndIndex("plain_table", "idx_plain_name");
    String ddl = rebuilder.describeRebuildDDL(idx).toUpperCase();
    assertTrue("DDL should use ALTER INDEX", ddl.contains("ALTER INDEX"));
    assertTrue("DDL should use REBUILD", ddl.contains("REBUILD"));
    // SQL Server requires table name in ALTER INDEX ... REBUILD.
    assertTrue("DDL should include the table name", ddl.contains("PLAIN_TABLE"));
    assertFalse("DDL should not use DROP INDEX", ddl.contains("DROP INDEX"));
    assertFalse("DDL should not use CREATE INDEX", ddl.contains("CREATE INDEX"));
  }

  // -------------------------------------------------------------------------
  // Rebuild correctness — each index type
  // -------------------------------------------------------------------------

  @Test
  public void rebuildPrimaryKeyIndexExistsAfterRebuild() throws Exception {
    IndexInfo pk = findByTableAndIndex("pk_table", "pk_table_pk");
    assertNotNull(pk);
    rebuilder.rebuildIndex(pk);
    assertTrue("PK-backing index should exist after rebuild",
        indexExists("pk_table_pk", "pk_table"));
  }

  @Test
  public void rebuildUniqueIndexExistsAndEnforcesUniquenessAfterRebuild() throws Exception {
    IndexInfo idx = findByTableAndIndex("unique_table", "idx_unique_name");
    rebuilder.rebuildIndex(idx);
    assertTrue("UNIQUE index should exist after rebuild",
        indexExists("idx_unique_name", "unique_table"));
    assertTrue("Uniqueness should be enforced after rebuild",
        uniquenessEnforced("unique_table", "name"));
  }

  @Test
  public void rebuildNonUniqueIndexExistsAfterRebuild() throws Exception {
    IndexInfo idx = findByTableAndIndex("plain_table", "idx_plain_name");
    rebuilder.rebuildIndex(idx);
    assertTrue("Non-unique index should exist after rebuild",
        indexExists("idx_plain_name", "plain_table"));
  }

  @Test
  public void rebuildMultiColumnUniqueIndexEnforcesUniquenessAfterRebuild() throws Exception {
    IndexInfo idx = findByTableAndIndex("multi_col_table", "idx_multi_col");
    assertNotNull(idx);
    assertTrue(idx.unique());
    assertEquals(List.of("part_name", "tbl_id"), idx.columns());

    rebuilder.rebuildIndex(idx);
    assertTrue("Multi-column index should exist after rebuild",
        indexExists("idx_multi_col", "multi_col_table"));
    assertTrue("Uniqueness should be enforced on multi-column index after rebuild",
        uniquenessEnforcedMultiCol("multi_col_table", "part_name", "tbl_id"));
  }

  @Test
  public void rebuildFkReferencedPrimaryKeySucceedsWithAlterIndexRebuild() throws Exception {
    // ALTER INDEX REBUILD is in-place and keeps FK references intact.
    IndexInfo pk = findByTableAndIndex("fk_parent_table", "fk_parent_pk");
    assertNotNull("FK-backed PK index should be present", pk);

    rebuilder.rebuildIndex(pk);
    assertTrue("PK-backing index should exist after in-place rebuild",
        indexExists("fk_parent_pk", "fk_parent_table"));
  }

  // -------------------------------------------------------------------------
  // findDuplicates
  // -------------------------------------------------------------------------

  @Test
  public void findDuplicatesNonUniqueIndexReturnsZeroWithoutQueryingDb() throws Exception {
    String testDbUrl = MSSQL.getInitialJdbcUrl()
        .replace("DatabaseName=master", "DatabaseName=" + TEST_DB);
    Connection closedConn = DriverManager.getConnection(
        testDbUrl, MSSQL.getDbRootUser(), MSSQL.getDbRootPassword());
    closedConn.close();
    MSSQLIndexRebuilder localRebuilder = new MSSQLIndexRebuilder(closedConn, false, "\"");
    IndexInfo nonUnique = new IndexInfo("idx_plain_name", "plain_table", false, false,
        List.of("name"));
    assertEquals(0, localRebuilder.findDuplicates(nonUnique));
  }

  @Test
  public void findDuplicatesTableWithDuplicateValuesReturnsPositiveCount() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSERT INTO plain_table VALUES (1, 'alice')");
      stmt.execute("INSERT INTO plain_table VALUES (2, 'alice')");
    }
    IndexInfo idx = new IndexInfo("fake_idx", "plain_table", true, false, List.of("name"));
    assertTrue("Should detect duplicate name values", rebuilder.findDuplicates(idx) > 0);
  }

  @Test
  public void findDuplicatesTableWithNoDuplicatesReturnsZero() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSERT INTO plain_table VALUES (1, 'alice')");
      stmt.execute("INSERT INTO plain_table VALUES (2, 'bob')");
    }
    IndexInfo idx = new IndexInfo("fake_idx", "plain_table", true, false, List.of("name"));
    assertEquals(0, rebuilder.findDuplicates(idx));
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private Map<String, IndexInfo> loadAllByName() throws HiveMetaException {
    // SQL Server index names are unique per-table, not globally.
    return rebuilder.loadIndexes()
        .stream()
        .collect(Collectors.toMap(
            i -> i.tableName() + "." + i.indexName(),
            Function.identity()));
  }

  private IndexInfo findByTableAndIndex(String tableName, String indexName)
      throws HiveMetaException {
    return loadAllByName().get(tableName + "." + indexName);
  }

  /** Returns true if the named index exists on the given table. */
  private boolean indexExists(String indexName, String tableName) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("""
        SELECT 1 FROM sys.indexes i
          JOIN sys.tables t ON t.object_id = i.object_id
         WHERE i.name = ? AND t.name = ?""")) {
      ps.setString(1, indexName);
      ps.setString(2, tableName);
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
      stmt.execute("INSERT INTO " + table + " (" + col1 + ", " + col2 + ") VALUES ('p', 1)");
      stmt.execute("INSERT INTO " + table + " (" + col1 + ", " + col2 + ") VALUES ('p', 1)");
      conn.rollback();
      return false;
    } catch (SQLException e) {
      conn.rollback();
      return "23000".equals(e.getSQLState());
    }
  }
}
