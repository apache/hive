/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.rest;

import java.security.PrivilegedExceptionAction;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.hive.metastore.ServletSecurity.AuthType;
import org.apache.hadoop.hive.metastore.annotation.MetastoreCheckinTest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.ForbiddenException;
import org.apache.iceberg.hive.HiveCatalog;
import org.apache.iceberg.rest.HMSPrivilegeHelper.AccessLevel;
import org.apache.iceberg.rest.extension.HiveRESTCatalogServerExtension;
import org.junit.experimental.categories.Category;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.extension.RegisterExtension;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

/**
 * Tests that {@link HMSCachingCatalog} enforces access-level checks for every operation, and that
 * the results are correctly cached and invalidated.
 *
 * <p>A {@link StubPrivilegeHelper} controls exactly which access level each (user, db, table) or
 * (user, db) triple receives, and counts how many times the helper was actually queried so that
 * caching behaviour can be verified.
 */
@Category(MetastoreCheckinTest.class)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class TestHMSCachingCatalogAuthz {

  private static final long CACHE_EXPIRY_MS = 5 * 60 * 1_000L;
  private static final String NS = "authz_test_ns";
  private static final Namespace NAMESPACE = Namespace.of(NS);
  private static final String TABLE = "authz_test_table";
  private static final TableIdentifier TABLE_ID = TableIdentifier.of(NAMESPACE, TABLE);
  private static final Schema SCHEMA = new Schema();

  @RegisterExtension
  private static final HiveRESTCatalogServerExtension SERVER =
      HiveRESTCatalogServerExtension.builder(AuthType.NONE)
          .configure(MetastoreConf.ConfVars.ICEBERG_CATALOG_CACHE_EXPIRY.getVarname(),
              String.valueOf(CACHE_EXPIRY_MS))
          .configure("hive.in.test", "true")
          .build();

  private HiveCatalog hiveCatalog;
  private StubPrivilegeHelper stub;
  private HMSCachingCatalog catalog;

  @BeforeAll
  void setupAll() {
    HMSCachingCatalog serverCatalog = HMSCachingCatalog.getLatestCache(null);
    Assertions.assertNotNull(serverCatalog, "HMSCachingCatalog must be initialized by the server");
    hiveCatalog = serverCatalog.getCatalog();
  }

  @BeforeEach
  void setupEach() {
    stub = new StubPrivilegeHelper();
    catalog = new HMSCachingCatalog(hiveCatalog, CACHE_EXPIRY_MS, stub);
    hiveCatalog.createNamespace(NAMESPACE);
  }

  @AfterEach
  void cleanup() {
    try { hiveCatalog.dropTable(TABLE_ID, false); } catch (Exception ignored) {}
    try { hiveCatalog.dropNamespace(NAMESPACE); } catch (Exception ignored) {}
    // Do not call catalog.close() — it would unregister the JMX MBean that other test
    // classes (e.g. TestHMSCachingCatalogStats) rely on via the server-side catalog.
  }

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------

  /** Runs {@code action} as the given short user name and returns its result. */
  private static <T> T as(String user, PrivilegedExceptionAction<T> action) throws Exception {
    return UserGroupInformation.createRemoteUser(user).doAs(action);
  }

  // ---------------------------------------------------------------------------
  // Table-level access checks
  // ---------------------------------------------------------------------------

  @Test
  void testLoadTableReadOnlyGranted() throws Exception {
    Table created = hiveCatalog.createTable(TABLE_ID, SCHEMA);
    stub.grantTable("alice", NS, TABLE, AccessLevel.READ_ONLY);

    // We must get back the very table we created, not merely a non-null result.
    Table loaded = as("alice", () -> catalog.loadTable(TABLE_ID));
    assertThat(loaded.name()).isEqualTo(created.name());
    assertThat(loaded.location()).isEqualTo(created.location());
  }

  @Test
  void testLoadTableReadWriteGranted() throws Exception {
    Table created = hiveCatalog.createTable(TABLE_ID, SCHEMA);
    stub.grantTable("alice", NS, TABLE, AccessLevel.READ_WRITE);

    Table loaded = as("alice", () -> catalog.loadTable(TABLE_ID));
    assertThat(loaded.name()).isEqualTo(created.name());
    assertThat(loaded.location()).isEqualTo(created.location());
  }

  @Test
  void testLoadTableDenied() {
    hiveCatalog.createTable(TABLE_ID, SCHEMA);
    // alice has no grant → NONE

    assertThatThrownBy(() -> as("alice", () -> { catalog.loadTable(TABLE_ID); return null; }))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void testDropTableWriteGranted() throws Exception {
    hiveCatalog.createTable(TABLE_ID, SCHEMA);
    stub.grantTable("alice", NS, TABLE, AccessLevel.READ_WRITE);

    as("alice", () -> { catalog.dropTable(TABLE_ID); return null; });

    // Table must be gone from HMS
    assertThat(hiveCatalog.tableExists(TABLE_ID)).isFalse();
  }

  @Test
  void testDropTableReadOnlyDenied() {
    hiveCatalog.createTable(TABLE_ID, SCHEMA);
    stub.grantTable("alice", NS, TABLE, AccessLevel.READ_ONLY);

    assertThatThrownBy(() -> as("alice", () -> { catalog.dropTable(TABLE_ID); return null; }))
        .isInstanceOf(ForbiddenException.class);

    // Table must still exist — the drop was vetoed
    assertThat(hiveCatalog.tableExists(TABLE_ID)).isTrue();
  }

  @Test
  void testDropTableDeniedWhenNoGrant() {
    hiveCatalog.createTable(TABLE_ID, SCHEMA);

    assertThatThrownBy(() -> as("alice", () -> { catalog.dropTable(TABLE_ID); return null; }))
        .isInstanceOf(ForbiddenException.class);
  }

  // ---------------------------------------------------------------------------
  // Namespace-level access checks
  // ---------------------------------------------------------------------------

  @Test
  void testListTablesNamespaceReadGranted() throws Exception {
    stub.grantNamespace("alice", NS, AccessLevel.READ_ONLY);

    // Must not throw; result may be empty
    as("alice", () -> catalog.listTables(NAMESPACE));
  }

  @Test
  void testListTablesNamespaceReadDenied() {
    assertThatThrownBy(() -> as("alice", () -> catalog.listTables(NAMESPACE)))
        .isInstanceOf(ForbiddenException.class);
  }

  @Test
  void testCreateTableNamespaceWriteGranted() throws Exception {
    stub.grantNamespace("alice", NS, AccessLevel.READ_WRITE);

    // buildTable checks namespace write access; the actual create goes to the underlying HiveCatalog
    TableIdentifier newTable = TableIdentifier.of(NAMESPACE, "new_table");
    Table created = as("alice", () -> catalog.buildTable(newTable, SCHEMA).create());

    // Confirm the created table was actually persisted and is the one we built. We read it back
    // through the underlying HiveCatalog (loadTable would require a separate table-level grant).
    assertThat(hiveCatalog.tableExists(newTable)).isTrue();
    assertThat(hiveCatalog.loadTable(newTable).location()).isEqualTo(created.location());
    hiveCatalog.dropTable(newTable, false);
  }

  @Test
  void testCreateTableNamespaceReadOnlyDenied() {
    stub.grantNamespace("alice", NS, AccessLevel.READ_ONLY);

    TableIdentifier newTable = TableIdentifier.of(NAMESPACE, "new_table");
    assertThatThrownBy(() -> as("alice", () -> catalog.buildTable(newTable, SCHEMA).create()))
        .isInstanceOf(ForbiddenException.class);

    assertThat(hiveCatalog.tableExists(newTable)).isFalse();
  }

  @Test
  void testCreateTableNamespaceNoGrantDenied() {
    TableIdentifier newTable = TableIdentifier.of(NAMESPACE, "new_table");
    assertThatThrownBy(() -> as("alice", () -> catalog.buildTable(newTable, SCHEMA).create()))
        .isInstanceOf(ForbiddenException.class);
  }

  // ---------------------------------------------------------------------------
  // Caching and invalidation
  // ---------------------------------------------------------------------------

  @Test
  void testAccessLevelIsCachedBetweenCalls() throws Exception {
    hiveCatalog.createTable(TABLE_ID, SCHEMA);
    stub.grantTable("alice", NS, TABLE, AccessLevel.READ_ONLY);

    as("alice", () -> catalog.loadTable(TABLE_ID));
    int countAfterFirst = stub.getCallCount();

    as("alice", () -> catalog.loadTable(TABLE_ID));
    int countAfterSecond = stub.getCallCount();

    assertThat(countAfterFirst).isEqualTo(1);
    // The access level was cached; the helper must not have been called again.
    assertThat(countAfterSecond).isEqualTo(1);
  }

  @Test
  void testInvalidateTableClearsAuthzCache() throws Exception {
    hiveCatalog.createTable(TABLE_ID, SCHEMA);
    stub.grantTable("alice", NS, TABLE, AccessLevel.READ_ONLY);

    as("alice", () -> catalog.loadTable(TABLE_ID));
    assertThat(stub.getCallCount()).isEqualTo(1);

    catalog.invalidateTable(TABLE_ID);

    as("alice", () -> catalog.loadTable(TABLE_ID));
    assertThat(stub.getCallCount()).isEqualTo(2);
  }

  @Test
  void testNamespaceAccessLevelIsCachedBetweenCalls() throws Exception {
    stub.grantNamespace("alice", NS, AccessLevel.READ_ONLY);

    as("alice", () -> catalog.listTables(NAMESPACE));
    int countAfterFirst = stub.getCallCount();

    as("alice", () -> catalog.listTables(NAMESPACE));
    int countAfterSecond = stub.getCallCount();

    assertThat(countAfterFirst).isEqualTo(1);
    assertThat(countAfterSecond).isEqualTo(1);
  }

  @Test
  void testDifferentUsersGetIndependentAccessLevels() throws Exception {
    Table created = hiveCatalog.createTable(TABLE_ID, SCHEMA);
    stub.grantTable("alice", NS, TABLE, AccessLevel.READ_ONLY);
    // bob has no grant

    Table loaded = as("alice", () -> catalog.loadTable(TABLE_ID));
    assertThat(loaded.location()).isEqualTo(created.location());
    assertThatThrownBy(() -> as("bob", () -> { catalog.loadTable(TABLE_ID); return null; }))
        .isInstanceOf(ForbiddenException.class);
  }

  // ---------------------------------------------------------------------------
  // Metadata cache behavior
  // ---------------------------------------------------------------------------

  @Test
  void testL2CacheReturnsSameTableInstance() throws Exception {
    hiveCatalog.createTable(TABLE_ID, SCHEMA);
    stub.grantTable("alice", NS, TABLE, AccessLevel.READ_ONLY);

    Table first  = as("alice", () -> catalog.loadTable(TABLE_ID));
    Table second = as("alice", () -> catalog.loadTable(TABLE_ID));

    // Caffeine stores object references; a cache hit returns the identical instance.
    assertThat(first).isSameAs(second);
  }

  @Test
  void testInvalidateTableForcesReload() throws Exception {
    hiveCatalog.createTable(TABLE_ID, SCHEMA);
    stub.grantTable("alice", NS, TABLE, AccessLevel.READ_ONLY);

    Table before = as("alice", () -> catalog.loadTable(TABLE_ID));
    assertThat(before.currentSnapshot()).isNull(); // fresh table, no snapshot yet

    // Advance the underlying table's metadata location by committing a new snapshot.
    Table raw = hiveCatalog.loadTable(TABLE_ID);
    DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(raw.location() + "/data/file-0.parquet")
        .withFileSizeInBytes(1024).withRecordCount(1).build();
    raw.newAppend().appendFile(file).commit();

    // Explicit invalidation evicts L2, L1, and authz caches.
    catalog.invalidateTable(TABLE_ID);

    Table after = as("alice", () -> catalog.loadTable(TABLE_ID));
    assertThat(after).isNotSameAs(before);
    assertThat(after.currentSnapshot()).isNotNull();
    assertThat(after.currentSnapshot().snapshotId()).isEqualTo(raw.currentSnapshot().snapshotId());
  }

  @Test
  void testDropTableEvictsCache() throws Exception {
    // Create a table with a snapshot so the cached version has observable state.
    hiveCatalog.createTable(TABLE_ID, SCHEMA);
    Table raw = hiveCatalog.loadTable(TABLE_ID);
    DataFile file = DataFiles.builder(PartitionSpec.unpartitioned())
        .withPath(raw.location() + "/data/file-0.parquet")
        .withFileSizeInBytes(1024).withRecordCount(1).build();
    raw.newAppend().appendFile(file).commit();

    stub.grantTable("alice", NS, TABLE, AccessLevel.READ_WRITE);

    Table cached = as("alice", () -> catalog.loadTable(TABLE_ID));
    assertThat(cached.currentSnapshot()).isNotNull();

    // Drop clears L2, L1, and authz caches via invalidateTable.
    as("alice", () -> { catalog.dropTable(TABLE_ID); return null; });

    // Recreate a fresh empty table (no snapshot) and reload.
    hiveCatalog.createTable(TABLE_ID, SCHEMA);

    Table reloaded = as("alice", () -> catalog.loadTable(TABLE_ID));
    assertThat(reloaded).isNotSameAs(cached);
    assertThat(reloaded.currentSnapshot()).isNull();
  }

  // ---------------------------------------------------------------------------
  // Stub privilege helper
  // ---------------------------------------------------------------------------

  /**
   * Configurable stub for {@link HMSPrivilegeHelper} that records how many times it was queried.
   * Grants are registered with {@link #grantTable} / {@link #grantNamespace}; any unregistered
   * combination returns {@link AccessLevel#NONE}.
   */
  static class StubPrivilegeHelper implements HMSPrivilegeHelper {

    private final Map<String, AccessLevel> tableGrants = new ConcurrentHashMap<>();
    private final Map<String, AccessLevel> namespaceGrants = new ConcurrentHashMap<>();
    private final AtomicInteger callCount = new AtomicInteger();

    void grantTable(String user, String db, String table, AccessLevel level) {
      tableGrants.put(user + "/" + db + "." + table, level);
    }

    void grantNamespace(String user, String db, AccessLevel level) {
      namespaceGrants.put(user + "/" + db, level);
    }

    int getCallCount() {
      return callCount.get();
    }

    @Override
    public boolean isAvailable() {
      return true;
    }

    @Override
    public AccessLevel getAccessLevel(String dbName, String tableName, String userName) {
      callCount.incrementAndGet();
      return tableGrants.getOrDefault(userName + "/" + dbName + "." + tableName, AccessLevel.NONE);
    }

    @Override
    public AccessLevel getNamespaceAccessLevel(String dbName, String userName) {
      callCount.incrementAndGet();
      return namespaceGrants.getOrDefault(userName + "/" + dbName, AccessLevel.NONE);
    }
  }
}
