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

package org.apache.iceberg.hive;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.EnvironmentContext;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.utils.MetaStoreUtils;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.iceberg.BaseTable;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.Schema;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.common.DynMethods;
import org.apache.iceberg.relocated.com.google.common.collect.ImmutableMap;
import org.apache.iceberg.relocated.com.google.common.collect.Lists;
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

  public static List<FieldSchema> getPartitionKeys(org.apache.iceberg.Table table, int specId) {
    Schema schema = table.specs().get(specId).schema();
    List<FieldSchema> hiveSchema = HiveSchemaUtil.convert(schema);
    Map<String, String> colNameToColType = hiveSchema.stream()
        .collect(Collectors.toMap(FieldSchema::getName, FieldSchema::getType));
    return table.specs().get(specId).fields().stream()
        .map(partField -> new FieldSchema(
            schema.findColumnName(partField.sourceId()),
            colNameToColType.get(schema.findColumnName(partField.sourceId())),
            String.format("Transform: %s", partField.transform().toString()))
        )
        .toList();
  }

  public static Table toHiveTable(org.apache.iceberg.Table table, Configuration conf) {
    var result = new Table();
    TableName tableName = TableName.fromString(table.name(), MetaStoreUtils.getDefaultCatalog(conf),
        Warehouse.DEFAULT_DATABASE_NAME);
    result.setCatName(tableName.getCat());
    result.setDbName(tableName.getDb());
    result.setTableName(tableName.getTable());
    result.setTableType(TableType.EXTERNAL_TABLE.toString());
    result.setPartitionKeys(getPartitionKeys(table, table.spec().specId()));
    TableMetadata metadata = ((BaseTable) table).operations().current();
    long maxHiveTablePropertySize = conf.getLong(HiveOperationsBase.HIVE_TABLE_PROPERTY_MAX_SIZE,
        HiveOperationsBase.HIVE_TABLE_PROPERTY_MAX_SIZE_DEFAULT);
    HMSTablePropertyHelper.updateHmsTableForIcebergTable(metadata.metadataFileLocation(), result, metadata,
        null, true, maxHiveTablePropertySize, null);
    String catalogType = IcebergCatalogProperties.getCatalogType(conf);
    if (!StringUtils.isEmpty(catalogType) && !IcebergCatalogProperties.NO_CATALOG_TYPE.equals(catalogType)) {
      result.getParameters().put(CatalogUtil.ICEBERG_CATALOG_TYPE, IcebergCatalogProperties.getCatalogType(conf));
    }
    result.setSd(getHiveStorageDescriptor(table));
    return result;
  }

  private static StorageDescriptor getHiveStorageDescriptor(org.apache.iceberg.Table table) {
    var result = new StorageDescriptor();
    result.setCols(HiveSchemaUtil.convert(table.schema()));
    result.setBucketCols(Lists.newArrayList());
    result.setNumBuckets(-1);
    result.setSortCols(Lists.newArrayList());
    result.setInputFormat(DEFAULT_INPUT_FORMAT_CLASS);
    result.setOutputFormat(DEFAULT_OUTPUT_FORMAT_CLASS);
    result.setSerdeInfo(getHiveSerdeInfo());
    result.setLocation(table.location());
    result.setParameters(Maps.newHashMap());
    result.setSkewedInfo(new SkewedInfo(Collections.emptyList(), Collections.emptyList(), Collections.emptyMap()));
    return result;
  }

  private static SerDeInfo getHiveSerdeInfo() {
    var result = new SerDeInfo("icebergSerde", DEFAULT_SERDE_CLASS, Maps.newHashMap());
    result.getParameters().put(serdeConstants.SERIALIZATION_FORMAT, "1"); // Default serialization format.
    return result;
  }
}
