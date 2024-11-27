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

package org.apache.hadoop.hive.metastore.tools.metatool;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.GetTableRequest;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.dbinstall.rules.DatabaseRule;
import org.apache.hadoop.hive.metastore.dbinstall.rules.Derby;
import org.apache.hadoop.hive.metastore.metasummary.MetaSummaryHandler;
import org.apache.hadoop.hive.metastore.metasummary.MetaSummarySchema;
import org.apache.hadoop.hive.metastore.metasummary.MetadataTableSummary;
import org.apache.hadoop.hive.metastore.metasummary.SummaryMapBuilder;
import org.apache.hadoop.hive.metastore.tools.MetaToolObjectStore;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category(MetastoreUnitTest.class)
public class TestMetaTookTaskMetadataSummary {
  private static final MetaToolTaskMetadataSummary TASK = new MetaToolTaskMetadataSummary();
  private static final String STORAGE_HANDLER = "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler";
  private static final String TABLE_TYPE_PARAM = "table_type";
  private static final String CURRENT_SNAPSHOT_TIMESTAMP_MS = "current-snapshot-timestamp-ms";
  private static Configuration conf;
  private static final String DB = "test_metasore_summary_1";
  private static final String TBL_PREFIX = "tbl";
  private static final DatabaseRule RULE = new Derby();

  @BeforeClass
  public static void setup() throws Exception {
    RULE.before();
    RULE.install();
    MetaToolTaskMetadataSummary.addSummaryHandler("iceberg", FakeIcebergSummaryHandler.class.getName());
    conf = MetastoreConf.newMetastoreConf();
    conf.set("hive.metastore.client.capabilities", "HIVEFULLACIDWRITE,HIVEFULLACIDREAD,HIVEMANAGEDINSERTWRITE");
    setMetaStoreConfiguration(conf);
    int metastorePort = MetaStoreTestUtils.startMetaStoreWithRetry(conf, true, false);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, "thrift://localhost:" + metastorePort);
    try (HiveMetaStoreClient msc = new HiveMetaStoreClient(conf)) {
      String tableName1 = TBL_PREFIX + "1";
      new DatabaseBuilder().setName(DB).create(msc, conf);
      new TableBuilder().setDbName(DB).setTableName("tbl1").addCol("a", "string").addPartCol("dt", "string")
          .addTableParam("EXTERNAL", "true")
          .setType("EXTERNAL_TABLE")
          .setSerdeLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
          .setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
          .setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
          .create(msc, conf);

      Map<String, String> properties = new HashMap<>();
      EnvironmentContext context = new EnvironmentContext(properties);
      GetTableRequest request = new GetTableRequest(DB, tableName1);
      Table table1 = msc.getTable(request);
      for (String part : new String[] {"2024-09-29", "2024-10-29", "2024-11-29"}) {
        Partition partition = new PartitionBuilder().inTable(table1).addValue(part)
            .addPartParam(StatsSetupConst.TOTAL_SIZE, "1000")
            .addPartParam(StatsSetupConst.NUM_FILES, "1")
            .addPartParam(StatsSetupConst.ROW_COUNT, "10")
            .addPartParam(StatsSetupConst.RAW_DATA_SIZE, "1000")
            .build(conf);
        msc.add_partition(partition, context);
      }

      new TableBuilder().setDbName(DB).setTableName(TBL_PREFIX + "2").addCol("a", "string")
          .addTableParam(StatsSetupConst.TOTAL_SIZE, "2000")
          .addTableParam(StatsSetupConst.NUM_FILES, "2")
          .addTableParam(StatsSetupConst.ROW_COUNT, "20")
          .addTableParam(StatsSetupConst.RAW_DATA_SIZE, "2000")
          .addTableParam(hive_metastoreConstants.TABLE_TRANSACTIONAL_PROPERTIES, "insert_only")
          .addTableParam(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true")
          .setType("MANAGED_TABLE")
          .setSerdeLib("org.apache.hadoop.hive.ql.io.orc.OrcSerde")
          .setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
          .setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
          .create(msc, conf);

      for (int i = 3; i < 6; i++) {
        new TableBuilder().setDbName(DB).setTableName(TBL_PREFIX + i).addCol("a", "string")
            .addTableParam("EXTERNAL", "true")
            .setType("EXTERNAL_TABLE")
            .addTableParam(StatsSetupConst.TOTAL_SIZE, i + "000")
            .addTableParam(StatsSetupConst.NUM_FILES, i + "")
            .addTableParam(StatsSetupConst.ROW_COUNT, i + "0")
            .addTableParam(StatsSetupConst.RAW_DATA_SIZE, i + "000")
            .addTableParam(TABLE_TYPE_PARAM, "ICEBERG")
            .addTableParam(CURRENT_SNAPSHOT_TIMESTAMP_MS, (System.currentTimeMillis() - (i - 3) * 24 * 3600000L) + "")
            .setSerdeLib(STORAGE_HANDLER)
            .setInputFormat("org.apache.iceberg.mr.hive.HiveIcebergInputFormat")
            .setOutputFormat("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat")
            .create(msc, conf);
        FakeIcebergSummaryHandler.
            addTableSummary(new TableName(MetaStoreUtils.getDefaultCatalog(conf), DB, TBL_PREFIX + i), i, i + 1, i + 2, i + 3);
      }
    }
    MetaToolObjectStore objectStore = new MetaToolObjectStore();
    objectStore.setConf(conf);
    TASK.setObjectStore(objectStore);
  }

  private static void setMetaStoreConfiguration(Configuration conf) {
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECT_URL_KEY, RULE.getJdbcUrl());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_DRIVER, RULE.getJdbcDriver());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.CONNECTION_USER_NAME, RULE.getHiveUser());
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.PWD, RULE.getHivePassword());
    // In this case we can disable auto_create which is enabled by default for every test
    MetastoreConf.setBoolVar(conf, MetastoreConf.ConfVars.AUTO_CREATE_ALL, false);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getIteratorToTest() {
    List<Object[]> params = new ArrayList<>();
    params.add(new Object[]{"-json", "output.json", null, null});
    params.add(new Object[]{"-json", "output.json", 2, null});
    params.add(new Object[]{"-json", "output.json", 3, null});
    params.add(new Object[]{"-json", "output.json", 3, 1});
    params.add(new Object[]{"-csv", "output.csv", null, null});
    params.add(new Object[]{"-console", null, null, null});
    return params;
  }

  private final String[] inputParams;
  public TestMetaTookTaskMetadataSummary(String format, String outputFile,
      Integer lastUpdated, Integer limit) {
    List<String> params = new ArrayList<>();
    params.add(format);
    if (outputFile != null) {
      params.add(outputFile);
    }
    if (lastUpdated != null) {
      params.add("" + lastUpdated);
    }
    if (limit != null) {
      params.add("" + limit);
    }
    this.inputParams = params.toArray(new String[0]);
  }

  @Test
  public void testObtainAndFilterSummary() throws Exception {
    TASK.validateInput(inputParams);
    Assert.assertEquals(TASK.formatJson, "-json".equals(inputParams[0]));
    Pair<MetaSummarySchema, List<MetadataTableSummary>> result =
        TASK.obtainAndFilterSummary();
    MetaSummarySchema schema = result.getLeft();
    if (TASK.formatJson) {
      Assert.assertEquals(Arrays.asList(FakeIcebergSummaryHandler.METADATA, FakeIcebergSummaryHandler.VERSION), schema.getFields());
    } else {
      Assert.assertEquals(Arrays.asList(FakeIcebergSummaryHandler.NUM_BRANCHES, FakeIcebergSummaryHandler.NUM_TAGS,
          FakeIcebergSummaryHandler.NUM_DATA_FILES, FakeIcebergSummaryHandler.NUM_SNAPSHOTS, FakeIcebergSummaryHandler.VERSION),
          schema.getFields());
    }
    List<MetadataTableSummary> summaries = result.getRight();
    int size = inputParams.length > 2 ? 2 + Integer.parseInt(inputParams[2]) : 5;
    int limit = inputParams.length > 3 ? 2 + Integer.parseInt(inputParams[3]) : 5;
    Assert.assertEquals(Math.min(size, limit), summaries.size());

    Map<TableName, MetadataTableSummary> summaryMap = new HashMap<>();
    String catalog = MetaStoreUtils.getDefaultCatalog(conf);
    summaries.forEach(summary -> summaryMap.put(new TableName(catalog,
        summary.getDbName(), summary.getTblName()), summary));
    MetadataTableSummary summary = summaryMap.get(new TableName(catalog, DB, TBL_PREFIX + "1"));
    Assert.assertEquals(3, summary.getPartitionCount());
    Assert.assertEquals(3, summary.getNumFiles());
    Assert.assertEquals(30, summary.getNumRows());
    Assert.assertEquals(3000, summary.getTotalSize());
    Assert.assertEquals("parquet", summary.getFileFormat());
    Assert.assertEquals("HIVE_EXTERNAL", summary.getTableType());

    summary = summaryMap.get(new TableName(catalog, DB, TBL_PREFIX + "2"));
    Assert.assertEquals(0, summary.getPartitionCount());
    Assert.assertEquals(2, summary.getNumFiles());
    Assert.assertEquals(20, summary.getNumRows());
    Assert.assertEquals(2000, summary.getTotalSize());
    Assert.assertEquals("orc", summary.getFileFormat());
    Assert.assertEquals("HIVE_ACID_INSERT_ONLY", summary.getTableType());

    for (int i = 3, j = 0; j < summaries.size() - 2; i++, j++) {
      TableName tableName = new TableName(catalog, DB, TBL_PREFIX + i);
      summary = summaryMap.get(tableName);
      Assert.assertEquals(0, summary.getPartitionCount());
      Assert.assertEquals(i, summary.getNumFiles());
      Assert.assertEquals(i * 10, summary.getNumRows());
      Assert.assertEquals(i * 1000, summary.getTotalSize());
      Assert.assertEquals("ICEBERG", summary.getTableType());
      Assert.assertEquals("v2", summary.getExtraSummary().remove(FakeIcebergSummaryHandler.VERSION));
      Map<String, Object> icebergSummary = FakeIcebergSummaryHandler.TABLE_TO_SUMMARY.get(tableName);
      Assert.assertFalse(icebergSummary.isEmpty());
      if (TASK.formatJson) {
        Assert.assertEquals(icebergSummary,
            summary.getExtraSummary().get(FakeIcebergSummaryHandler.METADATA));
      } else {
        Assert.assertEquals(icebergSummary, summary.getExtraSummary());
      }
    }
  }

  public static class FakeIcebergSummaryHandler implements MetaSummaryHandler {
    private static final String NUM_BRANCHES = "numBranches";
    private static final String NUM_TAGS = "numTags";
    private static final String NUM_DATA_FILES = "numDataFiles";
    private static final String NUM_SNAPSHOTS = "numSnapshots";
    private static final String METADATA = "metadata";
    private static final String VERSION = "format-version";
    private static final Map<TableName, Map<String, Object>> TABLE_TO_SUMMARY = new HashMap<>();
    private Configuration configuration;
    private boolean formatJson;

    @Override
    public void initialize(String catalog,
        boolean formatJson, MetaSummarySchema schema) {
      this.formatJson = formatJson;
      if (formatJson) {
        schema.addFields(METADATA, VERSION);
      } else {
        schema.addFields(NUM_BRANCHES, NUM_TAGS,
            NUM_DATA_FILES, NUM_SNAPSHOTS, VERSION);
      }
    }

    @Override
    public void appendSummary(TableName tableName, MetadataTableSummary tableSummary) {
      SummaryMapBuilder builder = new SummaryMapBuilder();
      Map<String, Object> summary = TABLE_TO_SUMMARY.getOrDefault(tableName, new HashMap<>());
      builder.add(VERSION, "v2");
      Map<String, Object> metadata = new HashMap<>();
      metadata.put(NUM_DATA_FILES, summary.getOrDefault(NUM_DATA_FILES, 0));
      metadata.put(NUM_SNAPSHOTS, summary.getOrDefault(NUM_SNAPSHOTS, 0));
      metadata.put(NUM_BRANCHES, summary.getOrDefault(NUM_BRANCHES, 0));
      metadata.put(NUM_TAGS, summary.getOrDefault(NUM_TAGS, 0));
      if (formatJson) {
        builder.add(METADATA, metadata);
        tableSummary.addExtra(builder);
      } else {
        metadata.forEach(builder::add);
        tableSummary.addExtra(builder);
      }
    }

    public static void addTableSummary(TableName tableName,
        int numBranches, int numTags, int numSnapshots, int numDataFiles) {
      SummaryMapBuilder builder = new SummaryMapBuilder()
          .add(NUM_BRANCHES, numBranches)
          .add(NUM_TAGS, numTags)
          .add(NUM_SNAPSHOTS, numSnapshots)
          .add(NUM_DATA_FILES, numDataFiles);
      TABLE_TO_SUMMARY.put(tableName, builder.build());
    }

    @Override
    public void close() throws Exception {
      // no op
    }

    @Override
    public void setConf(Configuration conf) {
      this.configuration = conf;
    }

    @Override
    public Configuration getConf() {
      return configuration;
    }
  }

  @AfterClass
  public static void destroy() throws Exception {
    try (HiveMetaStoreClient msc = new HiveMetaStoreClient(conf)) {
      Database database = msc.getDatabase(DB);
      if (database != null) {
        cleanDirectory(database.getLocationUri(), database.getManagedLocationUri());
      }
      msc.dropDatabase(DB, true, true, true);
    } finally {
      RULE.after();
    }
  }


  private static void cleanDirectory(String... location) {
    try {
      if (location != null) {
        for (String loc : location) {
          if (loc != null) {
            Path path = new Path(loc);
            path.getFileSystem(conf).delete(path, true);
          }
        }
      }
    } catch (Exception e) {
      // ignore the clean
    }
  }
}
