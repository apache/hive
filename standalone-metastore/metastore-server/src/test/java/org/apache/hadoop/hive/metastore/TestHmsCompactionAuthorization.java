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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hive.metastore;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.CompactionRequest;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.ShowCompactResponseElement;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf.ConfVars;
import org.apache.hadoop.hive.metastore.events.PreEventContext;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.metastore.utils.TestTxnDbUtil;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.security.authorize.ProxyUsers;
import org.apache.thrift.TException;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.function.ThrowingRunnable;

import java.util.List;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

/**
 * Tests the authorization of compact2 and submit_for_cleanup against a remote HMS, where the caller is
 * not a trusted proxy user unless the test configures it to be.
 */
@Category(MetastoreUnitTest.class)
public class TestHmsCompactionAuthorization {

  /** Pre-event listener that rejects every event while {@code deny} is set. */
  public static class DenyingAuthorizationListener extends MetaStorePreEventListener {
    private static volatile boolean deny = false;

    public DenyingAuthorizationListener(Configuration config) {
      super(config);
    }

    @Override
    public void onEvent(PreEventContext context)
        throws MetaException, NoSuchObjectException, InvalidOperationException {
      if (deny) {
        throw new MetaException("Authorization fails");
      }
    }
  }

  private static final String DB = "db_compaction_authz";
  private static final String TAB = "tab_compaction_authz";

  private static Configuration conf;
  private static HiveMetaStoreClient client;

  @BeforeClass
  public static void setUpClass() throws Exception {
    conf = MetastoreConf.newMetastoreConf();
    MetastoreConf.setBoolVar(conf, ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    MetastoreConf.setClass(conf, ConfVars.PRE_EVENT_LISTENERS, DenyingAuthorizationListener.class,
        MetaStorePreEventListener.class);
    MetastoreConf.setLongVar(conf, ConfVars.THRIFT_CONNECTION_RETRIES, 3);
    MetastoreConf.setVar(conf, ConfVars.STORAGE_SCHEMA_READER_IMPL, "no.such.class");
    // hive.in.test must stay off, otherwise the proxy-privilege check is skipped
    MetaStoreTestUtils.setConfForStandloneMode(conf);
    int port = MetaStoreTestUtils.startMetaStoreWithRetry(conf);
    MetastoreConf.setVar(conf, ConfVars.THRIFT_URIS, "thrift://localhost:" + port);
    client = new HiveMetaStoreClient(conf);
    TestTxnDbUtil.cleanDb(conf);
    TestTxnDbUtil.prepDb(conf);
  }

  @AfterClass
  public static void tearDownClass() throws Exception {
    DenyingAuthorizationListener.deny = false;
    setProxyUsers(false);
    if (client != null) {
      client.dropDatabase(DB, true, true, true);
      client.close();
    }
  }

  @Before
  public void setUp() throws Exception {
    DenyingAuthorizationListener.deny = false;
    setProxyUsers(false);
    client.dropDatabase(DB, true, true, true);
    new DatabaseBuilder().setName(DB).setCatalogName(Warehouse.DEFAULT_CATALOG_NAME).create(client, conf);
    new TableBuilder().setDbName(DB).setTableName(TAB).addCol("id", "int").create(client, conf);
    TestTxnDbUtil.cleanDb(conf);
    TestTxnDbUtil.prepDb(conf);
  }

  @After
  public void tearDown() {
    DenyingAuthorizationListener.deny = false;
    setProxyUsers(false);
  }

  /** Makes the (in-process) test user a trusted proxy user, or removes that privilege. */
  private static void setProxyUsers(boolean trusted) {
    try {
      Configuration proxyConf = new Configuration(false);
      if (trusted) {
        String user = UserGroupInformation.getCurrentUser().getShortUserName();
        proxyConf.set("hadoop.proxyuser." + user + ".hosts", "*");
        proxyConf.set("hadoop.proxyuser." + user + ".groups", "*");
      }
      ProxyUsers.refreshSuperUserGroupsConfiguration(proxyConf);
    } catch (Exception e) {
      throw new RuntimeException(e);
    }
  }

  private CompactionRequest compactionRequest(String dbName, String tableName) {
    return new CompactionRequest(dbName, tableName, CompactionType.MAJOR);
  }

  private String tableLocation() throws Exception {
    Table table = client.getTable(DB, TAB);
    return table.getSd().getLocation();
  }

  private List<ShowCompactResponseElement> compactions() throws Exception {
    return client.showCompactions().getCompacts();
  }

  private void assertAuthorizationFailure(ThrowingRunnable action) {
    DenyingAuthorizationListener.deny = true;
    try {
      MetaException ex = assertThrows(MetaException.class, action);
      assertTrue(ex.getMessage(), ex.getMessage().contains("Authorization fails"));
    } finally {
      DenyingAuthorizationListener.deny = false;
    }
  }

  /**
   * compact2 does not declare any Thrift exception, so a server-side failure reaches the client as a generic
   * TApplicationException without the original message: only check that the request is rejected and not queued.
   */
  private void assertCompact2Rejected(CompactionRequest request) throws Exception {
    assertThrows(TException.class, () -> client.compact2(request));
    assertEquals(0, compactions().size());
  }

  @Test
  public void testCompact2IsAuthorized() throws Exception {
    DenyingAuthorizationListener.deny = true;
    try {
      assertCompact2Rejected(compactionRequest(DB, TAB));
    } finally {
      DenyingAuthorizationListener.deny = false;
    }
  }

  @Test
  public void testSubmitForCleanupIsAuthorized() throws Exception {
    assertAuthorizationFailure(() -> client.submitForCleanup(compactionRequest(DB, TAB), 1L, 1L));
    assertEquals(0, compactions().size());
  }

  @Test
  public void testTrustedCallerSkipsAuthorization() throws Exception {
    setProxyUsers(true);
    DenyingAuthorizationListener.deny = true;
    try {
      client.compact2(compactionRequest(DB, TAB));
      assertTrue(client.submitForCleanup(compactionRequest(DB, "does_not_exist"), 1L, 1L));
    } finally {
      DenyingAuthorizationListener.deny = false;
    }
    assertEquals(2, compactions().size());
  }

  @Test
  public void testCompact2MissingTable() throws Exception {
    assertCompact2Rejected(compactionRequest(DB, "no_such_table"));
  }

  @Test
  public void testSubmitForCleanupMissingTableIsIgnored() throws Exception {
    CompactionRequest request = compactionRequest(DB, "ctas_rolled_back");
    request.putToProperties(hive_metastoreConstants.META_TABLE_LOCATION, tableLocation() + "_ctas");
    assertFalse(client.submitForCleanup(request, 1L, 1L));
    assertEquals(0, compactions().size());
  }

  @Test
  public void testSubmitForCleanupMissingPartitionIsIgnored() throws Exception {
    Table partitioned = new TableBuilder().setDbName(DB).setTableName("part_tab").addCol("id", "int")
        .addPartCol("p", "string").create(client, conf);
    CompactionRequest request = compactionRequest(DB, partitioned.getTableName());
    request.setPartitionname("p=missing");
    assertFalse(client.submitForCleanup(request, 1L, 1L));
    assertEquals(0, compactions().size());
  }

  @Test
  public void testSubmitForCleanupLocationMustBeInsideTable() throws Exception {
    String tableLocation = tableLocation();
    String[] outside = {
        "/tmp/some_other_dir",
        tableLocation + "/../other_table",           // escapes the table directory
        tableLocation + "_sibling/dir",              // shares only a string prefix with the table directory
    };
    for (String location : outside) {
      CompactionRequest request = compactionRequest(DB, TAB);
      request.putToProperties(hive_metastoreConstants.META_TABLE_LOCATION, location);
      MetaException ex = assertThrows("location=" + location, MetaException.class,
          () -> client.submitForCleanup(request, 1L, 1L));
      assertTrue(ex.getMessage(), ex.getMessage().contains("is not inside the location"));
    }
    assertEquals(0, compactions().size());

    CompactionRequest inside = compactionRequest(DB, TAB);
    inside.putToProperties(hive_metastoreConstants.META_TABLE_LOCATION, tableLocation + "/delta_0000001_0000001");
    assertTrue(client.submitForCleanup(inside, 1L, 1L));
    assertEquals(1, compactions().size());
  }

  @Test
  public void testRequestIsQueuedWithCanonicalNames() throws Exception {
    client.compact2(compactionRequest(DB.toUpperCase(), TAB.toUpperCase()));
    List<ShowCompactResponseElement> compactions = compactions();
    assertEquals(1, compactions.size());
    assertEquals(DB, compactions.get(0).getDbname());
    assertEquals(TAB, compactions.get(0).getTablename());
  }

  @Test
  public void testNonDefaultCatalogIsRejected() throws Exception {
    String catalogDb = MetaStoreUtils.prependNotNullCatToDbName("other_catalog", DB);
    assertCompact2Rejected(compactionRequest(catalogDb, TAB));
    MetaException ex = assertThrows(MetaException.class,
        () -> client.submitForCleanup(compactionRequest(catalogDb, TAB), 1L, 1L));
    assertTrue(ex.getMessage(), ex.getMessage().contains("default catalog"));
  }

  @Test
  public void testTransactionalTableIsAuthorizedWithItsOldState() throws Exception {
    // The ACID input/output formats are not available here, so the table is stored directly. If the old and the
    // new table of the pre-event were the same object, the transactional validation would take the table for one
    // that is being converted to ACID and reject it as it is not stored in an ACID format.
    String location = client.getDatabase(DB).getLocationUri() + "/acid_tab";
    Table acid = new TableBuilder().setDbName(DB).setTableName("acid_tab").addCol("id", "int")
        .setLocation(location)
        .addTableParam("transactional", "true")
        .addTableParam("transactional_properties", "default")
        .build(conf);
    ObjectStore store = new ObjectStore();
    store.setConf(conf);
    store.createTable(acid);
    Path path = new Path(location);
    path.getFileSystem(conf).mkdirs(path);

    client.compact2(compactionRequest(DB, "acid_tab"));
    assertEquals(1, compactions().size());
  }

  @Test
  public void testPartitionNameIsCanonicalized() throws Exception {
    Table partitioned = new TableBuilder().setDbName(DB).setTableName("part_tab").addCol("id", "int")
        .addPartCol("p", "string").create(client, conf);
    new PartitionBuilder().inTable(partitioned).addValue("a").addToTable(client, conf);

    // the extra key is ignored when the partition is looked up, it must not reach the queue
    CompactionRequest compaction = compactionRequest(DB, "part_tab");
    compaction.setPartitionname("p=a/extra=b");
    client.compact2(compaction);
    CompactionRequest cleanup = compactionRequest(DB, "part_tab");
    cleanup.setPartitionname("p=a/extra=b");
    assertTrue(client.submitForCleanup(cleanup, 1L, 1L));

    List<ShowCompactResponseElement> compactions = compactions();
    assertEquals(2, compactions.size());
    for (ShowCompactResponseElement compaction1 : compactions) {
      assertEquals("p=a", compaction1.getPartitionname());
    }
  }

  @Test
  public void testPartitionOfNonNativeTableIsAuthorizedAgainstTheTable() throws Exception {
    // the partitions of storage handler tables (e.g. Iceberg) do not exist in HMS
    new TableBuilder().setDbName(DB).setTableName("handler_tab").addCol("id", "int")
        .addTableParam("storage_handler", "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler")
        .create(client, conf);
    CompactionRequest request = compactionRequest(DB, "handler_tab");
    request.setPartitionname("p=1");
    client.compact2(request);

    List<ShowCompactResponseElement> compactions = compactions();
    assertEquals(1, compactions.size());
    assertEquals("p=1", compactions.get(0).getPartitionname());

    DenyingAuthorizationListener.deny = true;
    try {
      CompactionRequest denied = compactionRequest(DB, "handler_tab");
      denied.setPartitionname("p=1");
      assertThrows(TException.class, () -> client.compact2(denied));
    } finally {
      DenyingAuthorizationListener.deny = false;
    }
  }
}
