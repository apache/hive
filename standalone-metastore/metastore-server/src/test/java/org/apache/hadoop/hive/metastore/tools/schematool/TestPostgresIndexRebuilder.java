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
import org.apache.hadoop.hive.metastore.dbinstall.rules.Postgres;
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
 * Integration tests for {@link PostgresIndexRebuilder}.
 *
 * <p>Covers index discovery, generated rebuild DDL, and rebuild behavior.
 */
@Category(MetastoreCheckinTest.class)
public class TestPostgresIndexRebuilder {

  @ClassRule
  public static final Postgres POSTGRES = new Postgres();

  private static Connection conn;

  // Created once for the class; test DML is rolled back after each test.
  private static final String DDL_CREATE_TABLES = """
      CREATE TABLE pk_table (id BIGINT PRIMARY KEY, name VARCHAR(256));
      CREATE TABLE unique_table (id BIGINT, name VARCHAR(256));
      ALTER TABLE unique_table ADD CONSTRAINT uq_unique_table UNIQUE (name);
      CREATE TABLE standalone_table (id BIGINT, name VARCHAR(256));
      CREATE UNIQUE INDEX uq_standalone_table ON standalone_table (name);
      CREATE TABLE plain_table (id BIGINT, name VARCHAR(256));
      CREATE INDEX idx_plain_btree ON plain_table (name);
      CREATE INDEX idx_plain_hash ON plain_table USING hash (name);
      CREATE INDEX idx_plain_multicol ON plain_table (name, id);
      CREATE TABLE multi_unique_table (part_name VARCHAR(256), tbl_id BIGINT, value VARCHAR(256));
      ALTER TABLE multi_unique_table ADD CONSTRAINT uq_multi_unique UNIQUE (part_name, tbl_id);
      CREATE TABLE "MixedCaseTable" ("Id" BIGINT, "Name" VARCHAR(256));
      CREATE UNIQUE INDEX "uq_MixedCase" ON "MixedCaseTable" ("Name")""";

  private PostgresIndexRebuilder rebuilder;

  @BeforeClass
  public static void setUpClass() throws Exception {
    Class.forName(POSTGRES.getJdbcDriver());
    conn = DriverManager.getConnection(
        POSTGRES.getInitialJdbcUrl(), POSTGRES.getDbRootUser(), POSTGRES.getDbRootPassword());
    conn.setAutoCommit(false);
    for (String ddl : DDL_CREATE_TABLES.split(";")) {
      String sql = ddl.trim();
      if (!sql.isEmpty()) {
        try (Statement stmt = conn.createStatement()) {
          stmt.execute(sql);
        }
      }
    }
    conn.commit();
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    try (Statement stmt = conn.createStatement()) {
      for (String tbl : new String[]{"pk_table", "unique_table", "standalone_table",
          "plain_table", "multi_unique_table", "\"MixedCaseTable\""}) {
        stmt.execute("DROP TABLE IF EXISTS " + tbl + " CASCADE");
      }
    }
    conn.commit();
    conn.close();
  }

  @Before
  public void setUp() {
    rebuilder = new PostgresIndexRebuilder(conn, true, "\"");
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
    Map<String, IndexInfo> byName = loadAllByName();
    IndexInfo pk = findByTable(byName, "pk_table");
    assertNotNull("PK index should be present", pk);
    assertTrue("PK should be constraint-backed", pk.constraintBacked());
    assertTrue("PK should be unique", pk.unique());
  }

  @Test
  public void uniqueConstraintIsConstraintBackedAndUnique() throws Exception {
    Map<String, IndexInfo> byName = loadAllByName();
    IndexInfo idx = byName.get("uq_unique_table");
    assertNotNull("UNIQUE constraint index should be present", idx);
    assertTrue(idx.constraintBacked());
    assertTrue(idx.unique());
  }

  @Test
  public void standaloneUniqueIndexIsNotConstraintBacked() throws Exception {
    Map<String, IndexInfo> byName = loadAllByName();
    IndexInfo idx = byName.get("uq_standalone_table");
    assertNotNull("Standalone UNIQUE index should be present", idx);
    assertFalse(idx.constraintBacked());
    assertTrue(idx.unique());
  }

  @Test
  public void nonUniqueIndexIsNotUniqueAndNotConstraintBacked() throws Exception {
    Map<String, IndexInfo> byName = loadAllByName();
    IndexInfo idx = byName.get("idx_plain_btree");
    assertNotNull("Non-unique btree index should be present", idx);
    assertFalse(idx.constraintBacked());
    assertFalse(idx.unique());
  }

  @Test
  public void hashIndexIsExcluded() throws Exception {
    Map<String, IndexInfo> byName = loadAllByName();
    assertFalse("Hash index must not appear in results", byName.containsKey("idx_plain_hash"));
  }

  @Test
  public void multiColumnIndexColumnsReturnedInDefinitionOrder() throws Exception {
    Map<String, IndexInfo> byName = loadAllByName();
    IndexInfo idx = byName.get("idx_plain_multicol");
    assertNotNull(idx);
    assertEquals(List.of("name", "id"), idx.columns());
  }

  @Test
  public void standaloneIndexRebuildDdlUsesDropIndex() throws Exception {
    IndexInfo idx = loadAllByName().get("uq_standalone_table");
    assertTrue("Standalone index rebuild DDL should use DROP INDEX",
        rebuilder.describeRebuildDDL(idx).toUpperCase().contains("DROP INDEX"));
  }

  @Test
  public void constraintIndexRebuildDdlUsesDropConstraint() throws Exception {
    IndexInfo idx = loadAllByName().get("uq_unique_table");
    assertTrue("Constraint-backed index rebuild DDL should use DROP CONSTRAINT",
        rebuilder.describeRebuildDDL(idx).toUpperCase().contains("DROP CONSTRAINT"));
  }

  // -------------------------------------------------------------------------
  // Rebuild correctness — each index type
  // -------------------------------------------------------------------------

  @Test
  public void rebuildUniqueConstraintIndexDropsAndRecreatesWithUniquenessEnforced()
      throws Exception {
    IndexInfo idx = loadAllByName().get("uq_unique_table");
    rebuilder.rebuildIndex(idx);
    conn.commit();
    assertTrue("Index should exist after rebuild", indexExists("uq_unique_table"));
    assertTrue(indexIsValid("uq_unique_table"));
    assertTrue(uniquenessEnforced("unique_table", "name"));
  }

  @Test
  public void rebuildStandaloneUniqueIndexDropsAndRecreatesWithUniquenessEnforced()
      throws Exception {
    IndexInfo idx = loadAllByName().get("uq_standalone_table");
    rebuilder.rebuildIndex(idx);
    conn.commit();
    assertTrue(indexExists("uq_standalone_table"));
    assertTrue(indexIsValid("uq_standalone_table"));
    assertTrue(uniquenessEnforced("standalone_table", "name"));
  }

  @Test
  public void rebuildPrimaryKeyDropsAndRecreates() throws Exception {
    IndexInfo pk = findByTable(loadAllByName(), "pk_table");
    assertNotNull(pk);
    rebuilder.rebuildIndex(pk);
    conn.commit();
    assertTrue(indexExists(pk.indexName()));
    assertTrue(indexIsValid(pk.indexName()));
  }

  @Test
  public void rebuildNonUniqueIndexDropsAndRecreates() throws Exception {
    IndexInfo idx = loadAllByName().get("idx_plain_btree");
    rebuilder.rebuildIndex(idx);
    conn.commit();
    assertTrue(indexExists("idx_plain_btree"));
    assertTrue(indexIsValid("idx_plain_btree"));
  }

  @Test
  public void rebuildMultiColumnUniqueConstraintDropsAndRecreatesWithUniquenessEnforced()
      throws Exception {
    IndexInfo idx = loadAllByName().get("uq_multi_unique");
    assertNotNull(idx);
    assertTrue(idx.constraintBacked());
    assertTrue(idx.unique());
    assertEquals(List.of("part_name", "tbl_id"), idx.columns());

    rebuilder.rebuildIndex(idx);
    conn.commit();
    assertTrue(indexExists("uq_multi_unique"));
    assertTrue(indexIsValid("uq_multi_unique"));
    assertTrue(uniquenessEnforcedMultiCol("multi_unique_table", "part_name", "tbl_id"));
  }

  // -------------------------------------------------------------------------
  // findDuplicates
  // -------------------------------------------------------------------------

  @Test
  public void findDuplicatesNonUniqueIndexReturnsZeroWithoutQueryingDb() throws Exception {
    Connection closedConn = DriverManager.getConnection(
        POSTGRES.getInitialJdbcUrl(), POSTGRES.getDbRootUser(), POSTGRES.getDbRootPassword());
    closedConn.close();
    PostgresIndexRebuilder localRebuilder = new PostgresIndexRebuilder(closedConn, true, "\"");
    IndexInfo nonUnique = new IndexInfo("idx", "plain_table", false, false, List.of("name"));
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

  @Test
  public void rebuildMixedCaseIndexDropsAndRecreatesWithUniquenessEnforced() throws Exception {
    IndexInfo idx = loadAllByName().get("uq_MixedCase");
    assertNotNull(idx);
    assertFalse(idx.constraintBacked());
    assertTrue(idx.unique());

    rebuilder.rebuildIndex(idx);
    conn.commit();
    assertTrue(indexExists("uq_MixedCase"));
    assertTrue(indexIsValid("uq_MixedCase"));
  }

  // -------------------------------------------------------------------------
  // Helpers
  // -------------------------------------------------------------------------

  private Map<String, IndexInfo> loadAllByName() throws HiveMetaException {
    return rebuilder.loadIndexes()
        .stream()
        .collect(Collectors.toMap(IndexInfo::indexName, Function.identity()));
  }

  /** Finds a unique constraint-backed index for the given table. */
  private static IndexInfo findByTable(Map<String, IndexInfo> byName, String tableName) {
    return byName.values().stream()
        .filter(i -> i.tableName().equals(tableName) && i.constraintBacked() && i.unique())
        .findFirst()
        .orElse(null);
  }


  private boolean indexExists(String indexName) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement(
        "SELECT 1 FROM pg_indexes WHERE indexname = ?")) {
      ps.setString(1, indexName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next();
      }
    }
  }

  private boolean indexIsValid(String indexName) throws SQLException {
    try (PreparedStatement ps = conn.prepareStatement("""
        SELECT ix.indisvalid
          FROM pg_index ix
          JOIN pg_class ic ON ic.oid = ix.indexrelid
         WHERE ic.relname = ?""")) {
      ps.setString(1, indexName);
      try (ResultSet rs = ps.executeQuery()) {
        return rs.next() && rs.getBoolean(1);
      }
    }
  }

  /** Returns true when duplicate inserts fail with unique_violation. */
  private boolean uniquenessEnforced(String table, String column) throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSERT INTO " + table + "(" + column + ") VALUES ('__dup_test__')");
      stmt.execute("INSERT INTO " + table + "(" + column + ") VALUES ('__dup_test__')");
      conn.rollback();
      return false;
    } catch (SQLException e) {
      conn.rollback();
      return "23505".equals(e.getSQLState());
    }
  }

  /** Returns true when duplicate multi-column inserts fail with unique_violation. */
  private boolean uniquenessEnforcedMultiCol(String table, String col1, String col2)
      throws SQLException {
    try (Statement stmt = conn.createStatement()) {
      stmt.execute("INSERT INTO " + table + "(" + col1 + "," + col2 + ") VALUES ('p', 1)");
      stmt.execute("INSERT INTO " + table + "(" + col1 + "," + col2 + ") VALUES ('p', 1)");
      conn.rollback();
      return false;
    } catch (SQLException e) {
      conn.rollback();
      return "23505".equals(e.getSQLState());
    }
  }
}

