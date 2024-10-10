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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.HiveMetaStoreClient;
import org.apache.hadoop.hive.metastore.MetaStoreTestUtils;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.client.builder.DatabaseBuilder;
import org.apache.hadoop.hive.metastore.client.builder.PartitionBuilder;
import org.apache.hadoop.hive.metastore.client.builder.TableBuilder;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.metasummary.MetaSummaryHandler;
import org.apache.hadoop.hive.metastore.metasummary.MetaSummarySchema;
import org.apache.hadoop.hive.metastore.metasummary.MetadataTableSummary;
import org.apache.hadoop.hive.metastore.metasummary.SummaryMapBuilder;
import org.apache.hadoop.hive.metastore.security.HadoopThriftAuthBridge;
import org.apache.hadoop.hive.metastore.tools.MetaToolObjectStore;
import org.junit.BeforeClass;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

@RunWith(Parameterized.class)
@Category(MetastoreUnitTest.class)
public class TestMetaTookTaskMetadataSummary {
  private static final MetaToolTaskMetadataSummary SUMMARY_TASK = new MetaToolTaskMetadataSummary();
  private static final String STORAGE_HANDLER = "org.apache.iceberg.mr.hive.HiveIcebergStorageHandler";
  private static final String TABLE_TYPE_PARAM = "table_type";
  private static final String CURRENT_SNAPSHOT_TIMESTAMP_MS = "current-snapshot-timestamp-ms";
  private static Configuration conf;

  @BeforeClass
  public static void setup() throws Exception {
    MetaToolTaskMetadataSummary.addSummaryHandler("iceberg", FakeIcebergSummaryHandler.class.getName());
    conf = MetastoreConf.newMetastoreConf();
    int metastorePort = MetaStoreTestUtils.startMetaStoreWithRetry(HadoopThriftAuthBridge.getBridge(), conf);
    MetastoreConf.setVar(conf, MetastoreConf.ConfVars.THRIFT_URIS, "thrift://localhost:" + metastorePort);
    try (HiveMetaStoreClient msc = new HiveMetaStoreClient(conf)) {
      String dbName = "test_metasore_summary_1";
      String tableName1 = "tbl1";
      new DatabaseBuilder().setName(dbName).create(msc, conf);
      new TableBuilder().setDbName(dbName).setTableName("tbl1").addCol("a", "string").addPartCol("dt", "string")
          .addTableParam("EXTERNAL", "true")
          .setType("EXTERNAL_TABLE")
          .setSerdeLib("org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe")
          .setInputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetInputFormat")
          .setOutputFormat("org.apache.hadoop.hive.ql.io.parquet.MapredParquetOutputFormat")
          .create(msc, conf);
      Table table1 = msc.getTable(dbName, tableName1);
      new PartitionBuilder().inTable(table1).addValue("2024-09-29")
          .addPartParam(StatsSetupConst.TOTAL_SIZE, "1000")
          .addPartParam(StatsSetupConst.NUM_FILES, "1")
          .addPartParam(StatsSetupConst.ROW_COUNT, "10")
          .addPartParam(StatsSetupConst.RAW_DATA_SIZE, "1000")
          .addToTable(msc, conf);

      new TableBuilder().setDbName(dbName).setTableName("tbl2").addCol("a", "string")
          .addTableParam(StatsSetupConst.TOTAL_SIZE, "2000")
          .addTableParam(StatsSetupConst.NUM_FILES, "2")
          .addTableParam(StatsSetupConst.ROW_COUNT, "20")
          .addTableParam(StatsSetupConst.RAW_DATA_SIZE, "2000")
          .addTableParam(hive_metastoreConstants.TABLE_IS_TRANSACTIONAL, "true")
          .setType("MANAGED_TABLE")
          .setSerdeLib("org.apache.hadoop.hive.ql.io.orc.OrcSerde")
          .setInputFormat("org.apache.hadoop.hive.ql.io.orc.OrcInputFormat")
          .setOutputFormat("org.apache.hadoop.hive.ql.io.orc.OrcOutputFormat")
          .create(msc, conf);

      for (int i = 3; i < 6; i++) {
        new TableBuilder().setDbName(dbName).setTableName("tbl" + i).addCol("a", "string")
            .addTableParam("EXTERNAL", "true")
            .setType("EXTERNAL_TABLE")
            .addTableParam(StatsSetupConst.TOTAL_SIZE, i + "000")
            .addTableParam(StatsSetupConst.NUM_FILES, i + "")
            .addTableParam(StatsSetupConst.ROW_COUNT, i + "0")
            .addTableParam(StatsSetupConst.RAW_DATA_SIZE, i + "000")
            .addTableParam(TABLE_TYPE_PARAM, "ICEBERG")
            .addTableParam(CURRENT_SNAPSHOT_TIMESTAMP_MS, (System.currentTimeMillis() - (i - 2) * 24 * 3600000L) + "")
            .setSerdeLib(STORAGE_HANDLER)
            .setInputFormat("org.apache.iceberg.mr.hive.HiveIcebergInputFormat")
            .setOutputFormat("org.apache.iceberg.mr.hive.HiveIcebergOutputFormat")
            .create(msc, conf);
      }
    }
    MetaToolObjectStore objectStore = new MetaToolObjectStore();
    objectStore.setConf(conf);
    SUMMARY_TASK.setObjectStore(objectStore);
  }

  @Parameterized.Parameters
  public static Collection<Object[]> getIteratorToTest() {
    return null;
  }

  public TestMetaTookTaskMetadataSummary() {
  }

  public static class FakeIcebergSummaryHandler implements MetaSummaryHandler {
    private static final String NUM_BRANCHES = "numBranches";
    private static final String NUM_TAGS = "numTags";
    private static final String NUM_DATA_FILES = "numDataFiles";
    private static final String NUM_SNAPSHOTS = "numSnapshots";
    private static final String NUM_MANIFESTS = "numManifests";
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
        schema.addFields(NUM_BRANCHES, NUM_TAGS, METADATA, VERSION);
      } else {
        schema.addFields(NUM_BRANCHES, NUM_TAGS,
            NUM_DATA_FILES, NUM_SNAPSHOTS, NUM_MANIFESTS, VERSION);
      }
    }

    @Override
    public void appendSummary(TableName tableName, MetadataTableSummary tableSummary) {
      SummaryMapBuilder builder = new SummaryMapBuilder();
      Map<String, Object> summary = TABLE_TO_SUMMARY.getOrDefault(tableName, new HashMap<>());
      builder
          .add(NUM_BRANCHES, summary.getOrDefault(NUM_BRANCHES, 0))
          .add(NUM_TAGS, summary.getOrDefault(NUM_TAGS, 0))
          .add(VERSION, "v2");
      Map<String, Object> metadata = new HashMap<>();
      metadata.put(NUM_DATA_FILES, summary.getOrDefault(NUM_DATA_FILES, 0));
      metadata.put(NUM_SNAPSHOTS, summary.getOrDefault(NUM_SNAPSHOTS, 0));
      metadata.put(NUM_MANIFESTS, summary.getOrDefault(NUM_MANIFESTS, 0));
      if (formatJson) {
        builder.add(METADATA, metadata);
        tableSummary.addExtra(builder);
      } else {
        tableSummary.addExtra(builder);
        metadata.forEach(tableSummary::addExtra);
      }
    }

    public static void addTableSummary(TableName tableName, Map<String, Object> summary) {
      TABLE_TO_SUMMARY.put(tableName, summary);
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

}
