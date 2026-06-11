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
import org.apache.hadoop.hive.metastore.dbinstall.rules.Mysql;
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
 * Integration tests for {@link MySQLIndexRebuilder}.
 *
 * <p>Covers index discovery and rebuild behavior.
 * MySQL DDL auto-commits, so {@code @After} rollback only affects DML.
 */
@Category(MetastoreCheckinTest.class)
public class TestMySQLIndexRebuilder {

  @ClassRule
  public static final Mysql mysql = new Mysql();

  private static final String TEST_DB = "test_idx_rebuild";

  private static Connection conn;

  // plain_table is used for duplicate checks; fk_* tables verify FK-safe atomic PK rebuild.
  private static final String DDL_CREATE_TABLES = """
      CREATE TABLE pk_table (id BIGINT, name VARCHAR(256), PRIMARY KEY (id));
      CREATE TABLE unique_table (id BIGINT, name VARCHAR(256));
      ALTER TABLE unique_table ADD UNIQUE KEY uq_unique_table (name);
      CREATE TABLE plain_table (id BIGINT, name VARCHAR(256));
      CREATE INDEX idx_plain_btree ON plain_table (name);
      CREATE TABLE multi_unique_table (part_name VARCHAR(256), tbl_id BIGINT, value VARCHAR(256));
      ALTER TABLE multi_unique_table ADD UNIQUE KEY uq_multi_unique (part_name, tbl_id);
      CREATE TABLE create_unique_table (id BIGINT, name VARCHAR(256));
      CREATE UNIQUE INDEX idx_create_unique ON create_unique_table (name);
      CREATE TABLE fk_parent_table (id BIGINT PRIMARY KEY, val VARCHAR(256));
      CREATE TABLE fk_child_table (id BIGINT, parent_id BIGINT,
        FOREIGN KEY (parent_id) REFERENCES fk_parent_table(id))""";

  private MySQLIndexRebuilder rebuilder;

  @BeforeClass
  public static void setUpClass() throws Exception {
    Class.forName(mysql.getJdbcDriver());
    conn = DriverManager.getConnection(
        mysql.getInitialJdbcUrl(), mysql.getDbRootUser(), mysql.getDbRootPassword());
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("CREATE DATABASE IF NOT EXISTS " + TEST_DB);
      stmt.execute("USE " + TEST_DB);
    }
    conn.setAutoCommit(false);
    for (String ddl : DDL_CREATE_TABLES.split(";")) {
      String sql = ddl.trim();
      if (!sql.isEmpty()) {
        try (Statement stmt = conn.createStatement()) {
          stmt.execute(sql);
        }
      }
    }
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("DROP DATABASE IF EXISTS " + TEST_DB);
    }
    conn.close();
  }

  @Before
  public void setUp() {
    rebuilder = new MySQLIndexRebuilder(conn, true, "`");
  }

  @After
  public void tearDown() throws Exception {
    conn.rollback();
  }

  // -------------------------------------------------------------------------
  // Query correctness — loadIndexes
  // -------------------------------------------------------------------------

  @Test
  public void primaryKeyIsConstraintBackedAndUnique() throws Exception {
    IndexInfo pk = findByTableAndIndex("pk_table", "PRIMARY");
    assertNotNull("PK index should be present", pk);
    assertEquals("pk_table", pk.tableName());
    assertTrue("PK should be constraint-backed", pk.constraintBacked());
    assertTrue("PK should be unique", pk.unique());
  }

  @Test
  public void uniqueKeyIsNotConstraintBackedButIsUnique() throws Exception {
    Map<String, IndexInfo> byName = loadAllByName();
    IndexInfo idx = byName.get("unique_table.uq_unique_table");
    assertNotNull("UNIQUE KEY index should be present", idx);
    assertFalse(idx.constraintBacked());
    assertTrue(idx.unique());
  }

  @Test
  public void standaloneCreateUniqueIndexIsUniqueAndNotConstraintBacked() throws Exception {
    // CREATE UNIQUE INDEX and ADD UNIQUE KEY should map the same way in the catalog.
    Map<String, IndexInfo> byName = loadAllByName();
    IndexInfo idx = byName.get("create_unique_table.idx_create_unique");
    assertNotNull("CREATE UNIQUE INDEX should be present", idx);
    assertFalse(idx.constraintBacked());
    assertTrue(idx.unique());
  }

  @Test
  public void nonUniqueIndexIsNotUniqueAndNotConstraintBacked() throws Exception {
    Map<String, IndexInfo> byName = loadAllByName();
    IndexInfo idx = byName.get("plain_table.idx_plain_btree");
    assertNotNull("Non-unique btree index should be present", idx);
    assertFalse(idx.constraintBacked());
    assertFalse(idx.unique());
  }

  @Test
  public void primaryKeyDescribeDdlUsesAtomicAlterTable() throws Exception {
    IndexInfo pk = findByTableAndIndex("pk_table", "PRIMARY");
    String ddl = rebuilder.describeRebuildDDL(pk).toUpperCase();
    assertTrue("PK rebuild DDL should use ALTER TABLE", ddl.contains("ALTER TABLE"));
    assertTrue("PK rebuild DDL should drop and add primary key in one statement",
        ddl.contains("DROP PRIMARY KEY") && ddl.contains("ADD PRIMARY KEY"));
  }

  @Test
  public void uniqueKeyDescribeDdlUsesAtomicAlterTable() throws Exception {
    IndexInfo idx = loadAllByName().get("unique_table.uq_unique_table");
    String ddl = rebuilder.describeRebuildDDL(idx).toUpperCase();
    assertTrue("UNIQUE KEY rebuild DDL should use ALTER TABLE", ddl.contains("ALTER TABLE"));
    assertTrue("UNIQUE KEY rebuild DDL should drop and add index in one statement",
        ddl.contains("DROP INDEX") && ddl.contains("ADD UNIQUE INDEX"));
  }

  @Test
  public void multiColumnIndexColumnsReturnedInDefinitionOrder() throws Exception {
    IndexInfo idx = loadAllByName().get("multi_unique_table.uq_multi_unique");
    assertNotNull(idx);
    assertEquals(List.of("part_name", "tbl_id"), idx.columns());
  }

  // -------------------------------------------------------------------------
  // Rebuild correctness — each index type
  // -------------------------------------------------------------------------

  @Test
  public void rebuildUniqueKeyExistsAndEnforcesUniquenessAfterRebuild() throws Exception {
    IndexInfo idx = loadAllByName().get("unique_table.uq_unique_table");
    rebuilder.rebuildIndex(idx);
    assertTrue("Index should exist after rebuild", indexExists("uq_unique_table"));
    assertTrue(uniquenessEnforced("unique_table", "name"));
  }

  @Test
  public void rebuildPrimaryKeyExistsAfterRebuild() throws Exception {
    IndexInfo pk = findByTableAndIndex("pk_table", "PRIMARY");
    assertNotNull(pk);
    rebuilder.rebuildIndex(pk);
    assertTrue(indexExistsOnTable("PRIMARY", "pk_table"));
  }

  @Test
  public void rebuildNonUniqueIndexExistsAfterRebuild() throws Exception {
    IndexInfo idx = loadAllByName().get("plain_table.idx_plain_btree");
    rebuilder.rebuildIndex(idx);
    assertTrue(indexExists("idx_plain_btree"));
  }

  @Test
  public void rebuildMultiColumnUniqueKeyExistsAndEnforcesUniquenessAfterRebuild()
      throws Exception {
    IndexInfo idx = loadAllByName().get("multi_unique_table.uq_multi_unique");
    assertNotNull(idx);
    assertTrue(idx.unique());
    assertEquals(List.of("part_name", "tbl_id"), idx.columns());

    rebuilder.rebuildIndex(idx);
    assertTrue("Index should exist after rebuild", indexExists("uq_multi_unique"));
    assertTrue(uniquenessEnforcedMultiCol("multi_unique_table", "part_name", "tbl_id"));
  }

  @Test
  public void rebuildFkReferencedPrimaryKeySucceedsWithAtomicAlterTable() throws Exception {
    // Atomic DROP PRIMARY KEY + ADD PRIMARY KEY avoids ER_DROP_INDEX_FK.
    IndexInfo pk = findByTableAndIndex("fk_parent_table", "PRIMARY");
    assertNotNull("FK-backed PK should be present", pk);

    rebuilder.rebuildIndex(pk);
    assertTrue("PK should exist after atomic rebuild",
        indexExistsOnTable("PRIMARY", "fk_parent_table"));
  }

  // -------------------------------------------------------------------------
  // findDuplicates
  // -------------------------------------------------------------------------

  @Test
  public void findDuplicatesNonUniqueIndexReturnsZeroWithoutQueryingDb() throws Exception {
    Connection closedConn = DriverManager.getConnection(
        mysql.getInitialJdbcUrl(), mysql.getDbRootUser(), mysql.getDbRootPassword());
    closedConn.close();
    MySQLIndexRebuilder localRebuilder = new MySQLIndexRebuilder(closedConn, true, "`");
    IndexInfo nonUnique = new IndexInfo("idx", "plain_table", false, false, List.of("name"));
    assertEquals(0, localRebuilder.findDuplicates(nonUnique));
  }

  @Test
  public void findDuplicatesTableWithDuplicateValuesReturnsPositiveCount() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSERT INTO plain_table VALUES (1, 'alice')");
      stmt.execute("INSERT INTO plain_table VALUES (2, 'alice')");
    }
    IndexInfo idx = new IndexInfo(
        "fake_idx", "plain_table", true, false, List.of("name"));
    assertTrue("Should detect duplicate name values", rebuilder.findDuplicates(idx) > 0);
  }

  @Test
  public void findDuplicatesTableWithNoDuplicatesReturnsZero() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSERT INTO plain_table VALUES (1, 'alice')");
      stmt.execute("INSERT INTO plain_table VALUES (2, 'bob')");
    }
    IndexInfo idx = new IndexInfo(
        "fake_idx", "plain_table", true, false, List.of("name"));
    assertEquals(0, rebuilder.findDuplicates(idx));
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private Map<String, IndexInfo> loadAllByName() throws HiveMetaException {
    return rebuilder.loadIndexes()
        .stream()
        // PRIMARY is reused across tables, so include table name in the map key.
        .collect(Collectors.toMap(
            i -> i.tableName() + "." + i.indexName(),
            Function.identity()));
  }

  /** Looks up an index by its name on a specific table. */
  private IndexInfo findByTableAndIndex(String tableName, String indexName)
      throws HiveMetaException {
    return rebuilder.loadIndexes().stream()
        .filter(i -> i.tableName().equals(tableName) && i.indexName().equals(indexName))
        .findFirst()
        .orElse(null);
  }


  private boolean indexExists(String indexName) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("""
        SELECT 1 FROM INFORMATION_SCHEMA.STATISTICS
         WHERE TABLE_SCHEMA = DATABASE() AND INDEX_NAME = ? LIMIT 1""")) {
      ps.setString(1, indexName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private boolean indexExistsOnTable(String indexName, String tableName) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("""
        SELECT 1 FROM INFORMATION_SCHEMA.STATISTICS
         WHERE TABLE_SCHEMA = DATABASE() AND INDEX_NAME = ? AND TABLE_NAME = ? LIMIT 1""")) {
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
      stmt.execute("INSERT INTO " + table + "(" + column + ") VALUES ('__dup_test__')");
      stmt.execute("INSERT INTO " + table + "(" + column + ") VALUES ('__dup_test__')");
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
      stmt.execute("INSERT INTO " + table + "(" + col1 + "," + col2 + ") VALUES ('p', 1)");
      stmt.execute("INSERT INTO " + table + "(" + col1 + "," + col2 + ") VALUES ('p', 1)");
      conn.rollback();
      return false;
    } catch (SQLException e) {
      conn.rollback();
      return "23000".equals(e.getSQLState());
    }
  }
}
