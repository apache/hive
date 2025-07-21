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

package org.apache.iceberg.hive;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Maps;
import org.apache.thrift.TException;

public class MetastoreUtil {

  public static final String DEFAULT_INPUT_FORMAT_CLASS = "org.apache.iceberg.mr.hive.HiveIcebergInputFormat";
  public static final String DEFAULT_OUTPUT_FORMAT_CLASS = "org.apache.iceberg.mr.hive.HiveIcebergOutputFormat";
  public static final String DEFAULT_SERDE_CLASS = "org.apache.iceberg.mr.hive.HiveIcebergSerDe";

  private static final DynMethods.UnboundMethod ALTER_TABLE =
      DynMethods.builder("alter_table")
          .impl(
              IMetaStoreClient.class,
              "alter_table_with_environmentContext",
              String.class,
              String.class,
              Table.class,
              EnvironmentContext.class)
          .impl(
              IMetaStoreClient.class,
              "alter_table",
              String.class,
              String.class,
              Table.class,
              EnvironmentContext.class)
          .impl(IMetaStoreClient.class, "alter_table", String.class, String.class, Table.class)
      .build();

  private MetastoreUtil() {
  }

  /**
   * Calls alter_table method using the metastore client. If the HMS supports it, environmental
   * context will be set in a way that turns off stats updates to avoid recursive file listing.
   */
  public static void alterTable(
      IMetaStoreClient client, String databaseName, String tblName, Table table) throws TException {
    alterTable(client, databaseName, tblName, table, ImmutableMap.of());
  }

  /**
   * Calls alter_table method using the metastore client. If the HMS supports it, environmental
   * context will be set in a way that turns off stats updates to avoid recursive file listing.
   */
  public static void alterTable(
      IMetaStoreClient client,
      String databaseName,
      String tblName,
      Table table,
      Map<String, String> extraEnv)
      throws TException {
    Map<String, String> env = Maps.newHashMapWithExpectedSize(extraEnv.size() + 1);
    env.putAll(extraEnv);
    env.put(StatsSetupConst.DO_NOT_UPDATE_STATS, StatsSetupConst.TRUE);

    try {
      ALTER_TABLE.invoke(client, databaseName, tblName, table, new EnvironmentContext(env));
    } catch (RuntimeException e) {
      // TException would be wrapped into RuntimeException during reflection
      if (e.getCause() instanceof TException) {
        throw (TException) e.getCause();
      } else {
        throw e;
      }
    }
  }

  public static Table convertIcebergTableToHiveTable(org.apache.iceberg.Table icebergTable, Configuration conf) {
    Table hiveTable = new Table();
    TableMetadata metadata = ((BaseTable) icebergTable).operations().current();
    long maxHiveTablePropertySize = conf.getLong(HiveOperationsBase.HIVE_TABLE_PROPERTY_MAX_SIZE,
        HiveOperationsBase.HIVE_TABLE_PROPERTY_MAX_SIZE_DEFAULT);
    HMSTablePropertyHelper.updateHmsTableForIcebergTable(metadata.metadataFileLocation(), hiveTable, metadata,
        null, true, maxHiveTablePropertySize, null);
    hiveTable.getParameters().put(CatalogUtils.ICEBERG_CATALOG_TYPE, CatalogUtil.ICEBERG_CATALOG_TYPE_REST);
    TableName tableName = TableName.fromString(icebergTable.name(), null, null);
    hiveTable.setTableName(tableName.getTable());
    hiveTable.setDbName(tableName.getDb());
    StorageDescriptor storageDescriptor = new StorageDescriptor();
    hiveTable.setSd(storageDescriptor);
    hiveTable.setTableType("EXTERNAL_TABLE");
    hiveTable.setPartitionKeys(new LinkedList<>());
    List<FieldSchema> cols = new LinkedList<>();
    storageDescriptor.setCols(cols);
    storageDescriptor.setLocation(icebergTable.location());
    storageDescriptor.setInputFormat(DEFAULT_INPUT_FORMAT_CLASS);
    storageDescriptor.setOutputFormat(DEFAULT_OUTPUT_FORMAT_CLASS);
    storageDescriptor.setBucketCols(new LinkedList<>());
    storageDescriptor.setSortCols(new LinkedList<>());
    storageDescriptor.setParameters(Maps.newHashMap());
    SerDeInfo serDeInfo = new SerDeInfo("icebergSerde", DEFAULT_SERDE_CLASS, Maps.newHashMap());
    serDeInfo.getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1"); // Default serialization format.
    storageDescriptor.setSerdeInfo(serDeInfo);
    icebergTable.schema().columns().forEach(icebergColumn -> {
      FieldSchema fieldSchema = new FieldSchema();
      fieldSchema.setName(icebergColumn.name());
      fieldSchema.setType(icebergColumn.type().toString());
      cols.add(fieldSchema);
    });
    return hiveTable;
  }
}
