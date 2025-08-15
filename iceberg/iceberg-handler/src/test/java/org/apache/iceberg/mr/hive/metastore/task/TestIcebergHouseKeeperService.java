/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.iceberg.mr.hive.metastore.task;

import java.io.File;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.TableFetcher;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.DataFiles;
import org.apache.iceberg.mr.hive.IcebergTableUtil;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class TestIcebergHouseKeeperService {
  private static final Logger LOG = LoggerFactory.getLogger(TestIcebergHouseKeeperService.class);

  private static final HiveConf conf = new HiveConf(TestIcebergHouseKeeperService.class);
  private static Hive db;

  @BeforeClass
  public static void beforeClass() throws Exception {
    conf.set("hive.security.authorization.enabled", "false");
    conf.set("hive.security.authorization.manager",
        "org.apache.hadoop.hive.ql.security.authorization.plugin.sqlstd.SQLStdConfOnlyAuthorizerFactory");
    conf.set("iceberg.engine.hive.lock-enabled", "false");

    db = Hive.get(conf);
  }

  @AfterClass
  public static void afterClass() {
    db.close(true);
  }

  @Test
  public void testIcebergTableFetched() throws Exception {
    createIcebergTable("iceberg_table");

    TableFetcher tableFetcher = IcebergTableUtil.getTableFetcher(db.getMSC(), null, "default", "*");

    int maxBatchSize = MetastoreConf.getIntVar(conf, MetastoreConf.ConfVars.BATCH_RETRIEVE_MAX);
    List<org.apache.hadoop.hive.metastore.api.Table> tables = tableFetcher.getTables(maxBatchSize);
    Assert.assertEquals("hive", tables.get(0).getCatName());
    Assert.assertEquals("default", tables.get(0).getDbName());
    Assert.assertEquals("iceberg_table", tables.get(0).getTableName());
  }

  @Test
  public void testExpireSnapshotsByServiceRun() throws Exception {
    String tableName = "iceberg_table_snapshot_expiry_e2e_test";
    createIcebergTable(tableName);
    IcebergHouseKeeperService service = getServiceForTable("default", tableName);

    GetTableRequest request = new GetTableRequest("default", tableName);
    org.apache.iceberg.Table icebergTable = IcebergTableUtil.getTable(conf, db.getMSC().getTable(request));

    String metadataDirectory = icebergTable.location().replaceAll("^[a-zA-Z]+:", "") + "/metadata";

    DataFile datafile = DataFiles.builder(icebergTable.spec())
        .withRecordCount(3)
        .withPath("/tmp/file.parquet")
        .withFileSizeInBytes(10)
        .build();

    icebergTable.newAppend().appendFile(datafile).commit();
    assertSnapshotFiles(metadataDirectory, 1);
    icebergTable.newAppend().appendFile(datafile).commit();
    assertSnapshotFiles(metadataDirectory, 2);

    Thread.sleep(1000); // allow snapshots that are 1000ms old to become eligible for snapshot expiry
    service.run();

    assertSnapshotFiles(metadataDirectory, 1);
    db.dropTable("default", "iceberg_table_snapshot_expiry_e2e_test");
  }

  private void createIcebergTable(String name) throws Exception {
    Table table = new Table("default", name);
    List<FieldSchema> columns = Lists.newArrayList();
    columns.add(new FieldSchema("col", "string", "First column"));
    table.setFields(columns); // Set columns

    table.setProperty("EXTERNAL", "TRUE");
    table.setTableType(TableType.EXTERNAL_TABLE);
    table.setProperty("table_type", "ICEBERG");

    table.setProperty("history.expire.max-snapshot-age-ms", "500");

    db.createTable(table);
  }

  /**
   * Creates IcebergHouseKeeperService that's configured to clean up a table by database and table name.
   *
   * @param tableName to be cleaned up
   * @return IcebergHouseKeeperService
   */
  private IcebergHouseKeeperService getServiceForTable(String dbName, String tableName) {
    IcebergHouseKeeperService service = new IcebergHouseKeeperService();
    HiveConf serviceConf = new HiveConf(conf);
    serviceConf.set("hive.metastore.iceberg.table.expiry.database.pattern", dbName);
    serviceConf.set("hive.metastore.iceberg.table.expiry.table.pattern", tableName);
    service.setConf(serviceConf);
    return service;
  }

  private void assertSnapshotFiles(String metadataDirectory, int numberForSnapshotFiles) {
    File[] matchingFiles = new File(metadataDirectory).listFiles((dir, name) -> name.startsWith("snap-"));
    List<File> files = Optional.ofNullable(matchingFiles).map(Arrays::asList).orElse(Collections.emptyList());
    LOG.debug("Snapshot files found in directory({}): {}", metadataDirectory, files);
    Assert.assertEquals(String.format("Unexpected no. of snapshot files in metadata directory: %s",
        metadataDirectory), numberForSnapshotFiles, files.size());
  }
}
