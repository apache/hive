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
package org.apache.hadoop.hive.ql.txn.compactor;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.DDLPlanUtils;
import org.apache.hadoop.hive.ql.util.DirectionUtils;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Builds query strings that help with query-based compaction of insert-only tables.
 */
class CompactionQueryBuilderForInsertOnly extends CompactionQueryBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionQueryBuilderForInsertOnly.class.getName());

  private StorageDescriptor storageDescriptor; // for Create in insert-only

  /**
   * Construct a CompactionQueryBuilderForInsertOnly with required params.
   *
   * @param compactionType major or minor or rebalance, e.g. CompactionType.MAJOR.
   *                       Cannot be null.
   * @param operation      query's Operation e.g. Operation.CREATE.
   * @throws IllegalArgumentException if compactionType is null or the compaction type is REBALANCE
   */
  CompactionQueryBuilderForInsertOnly(CompactionType compactionType, Operation operation, String resultTableName) {
    super(compactionType, operation, true, resultTableName);
    if (CompactionType.REBALANCE.equals(compactionType)) {
      throw new IllegalArgumentException("Rebalance compaction is supported only on full ACID tables!");
    }
  }

  /**
   * Set the StorageDescriptor of the table or partition to compact.
   * Required for Create operations in insert-only compaction.
   *
   * @param storageDescriptor StorageDescriptor of the table or partition to compact, not null
   */
  CompactionQueryBuilder setStorageDescriptor(StorageDescriptor storageDescriptor) {
    this.storageDescriptor = storageDescriptor;
    return this;
  }

  protected void buildSelectClauseForInsert(StringBuilder query) {
    // Need list of columns for major crud, mmmajor partitioned, mmminor
    List<FieldSchema> cols;
    if (CompactionType.MAJOR.equals(compactionType) && sourcePartition != null || CompactionType.MINOR.equals(
        compactionType)) {
      if (sourceTab == null) {
        return; // avoid NPEs, don't throw an exception but skip this part of the query
      }
      cols = sourceTab.getSd().getCols();
    } else {
      cols = null;
    }
    switch (compactionType) {
    case MAJOR: {
      if (sourcePartition != null) { //mmmajor and partitioned
        appendColumns(query, cols, false);
      } else { // mmmajor and unpartitioned
        query.append("*");
      }
      break;
    }
    case MINOR: {
      appendColumns(query, cols, false);
    }
    }
  }

  protected void getSourceForInsert(StringBuilder query) {
    if (sourceTabForInsert != null) {
      query.append(sourceTabForInsert);
    } else {
      query.append(sourceTab.getDbName()).append(".").append(sourceTab.getTableName());
    }
    query.append(" ");
    if (CompactionType.MAJOR.equals(compactionType) && StringUtils.isNotBlank(orderByClause)) {
      query.append(orderByClause);
    }
  }

  protected void buildWhereClauseForInsert(StringBuilder query) {
    if (CompactionType.MAJOR.equals(compactionType) && sourcePartition != null && sourceTab != null) {
      List<String> vals = sourcePartition.getValues();
      List<FieldSchema> keys = sourceTab.getPartitionKeys();
      if (keys.size() != vals.size()) {
        throw new IllegalStateException("source partition values (" + Arrays.toString(
            vals.toArray()) + ") do not match source table values (" + Arrays.toString(
            keys.toArray()) + "). Failing compaction.");
      }

      query.append(" where ");
      for (int i = 0; i < keys.size(); ++i) {
        FieldSchema keySchema = keys.get(i);
        query.append(i == 0 ? "`" : " and `").append(keySchema.getName()).append("`=");
        if (!keySchema.getType().equalsIgnoreCase(ColumnType.BOOLEAN_TYPE_NAME)) {
          query.append("'").append(vals.get(i)).append("'");
        } else {
          query.append(vals.get(i));
        }
      }
    }
  }

  protected void getDdlForCreate(StringBuilder query) {
    defineColumns(query);

    // PARTITIONED BY. Used for parts of minor compaction.
    if (isPartitioned) {
      query.append(" PARTITIONED BY (`file_name` STRING) ");
    }

    // CLUSTERED BY. (bucketing)
    getMmBucketing(query);

    // SKEWED BY
    getSkewedByClause(query);

    // STORED AS / ROW FORMAT SERDE + INPUTFORMAT + OUTPUTFORMAT
    copySerdeFromSourceTable(query);

    // LOCATION
    if (location != null) {
      query.append(" LOCATION '").append(HiveStringUtils.escapeHiveCommand(location)).append("'");
    }

    // TBLPROPERTIES
    addTblProperties(query);
  }

  /**
   * Define columns of the create query.
   */
  private void defineColumns(StringBuilder query) {
    if (sourceTab == null) {
      return; // avoid NPEs, don't throw an exception but skip this part of the query
    }
    query.append("(");
    List<String> columnDescs = getColumnDescs();
    query.append(StringUtils.join(columnDescs, ','));
    query.append(") ");
  }

  /**
   * Part of Create operation. Copy source table bucketing for insert-only compaction.
   */
  private void getMmBucketing(StringBuilder query) {
    if (sourceTab == null) {
      return; // avoid NPEs, don't throw an exception but skip this part of the query
    }
    boolean isFirst;
    List<String> buckCols = sourceTab.getSd().getBucketCols();
    if (buckCols.size() > 0) {
      query.append("CLUSTERED BY (").append(StringUtils.join(buckCols, ",")).append(") ");
      List<Order> sortCols = sourceTab.getSd().getSortCols();
      if (sortCols.size() > 0) {
        query.append("SORTED BY (");
        isFirst = true;
        for (Order sortCol : sortCols) {
          if (!isFirst) {
            query.append(", ");
          }
          isFirst = false;
          query.append(sortCol.getCol()).append(" ").append(DirectionUtils.codeToText(sortCol.getOrder()));
        }
        query.append(") ");
      }
      query.append("INTO ").append(sourceTab.getSd().getNumBuckets()).append(" BUCKETS");
    }
  }

  /**
   * Part of Create operation. Insert-only compaction tables copy source tables.
   */
  private void getSkewedByClause(StringBuilder query) {
    if (sourceTab == null) {
      return; // avoid NPEs, don't throw an exception but skip this part of the query
    }
    boolean isFirst; // Stored as directories. We don't care about the skew otherwise.
    if (sourceTab.getSd().isStoredAsSubDirectories()) {
      SkewedInfo skewedInfo = sourceTab.getSd().getSkewedInfo();
      if (skewedInfo != null && !skewedInfo.getSkewedColNames().isEmpty()) {
        query.append(" SKEWED BY (").append(StringUtils.join(skewedInfo.getSkewedColNames(), ", ")).append(") ON ");
        isFirst = true;
        for (List<String> colValues : skewedInfo.getSkewedColValues()) {
          if (!isFirst) {
            query.append(", ");
          }
          isFirst = false;
          query.append("('").append(StringUtils.join(colValues, "','")).append("')");
        }
        query.append(") STORED AS DIRECTORIES");
      }
    }
  }

  private static Set<String> getHiveMetastoreConstants() {
    Set<String> result = new HashSet<>();
    for (Field f : hive_metastoreConstants.class.getDeclaredFields()) {
      if (!Modifier.isFinal(f.getModifiers())) {
        continue;
      }
      if (!String.class.equals(f.getType())) {
        continue;
      }
      f.setAccessible(true);
      try {
        result.add((String) f.get(null));
      } catch (IllegalAccessException e) {
        throw new RuntimeException(e);
      }
    }
    return result;
  }

  /**
   * Part of Create operation. All tmp tables are not transactional and are marked as
   * compaction tables. Additionally...
   * - Insert-only compaction tables copy source tables' tblproperties, except metastore/statistics
   * properties.
   */
  private void addTblProperties(StringBuilder query) {
    Map<String, String> tblProperties = new HashMap<>();
    tblProperties.put("transactional", "false");
    if (sourceTab != null) { // to avoid NPEs, skip this part if sourceTab is null
      // Exclude all standard table properties.
      Set<String> excludes = getHiveMetastoreConstants();
      excludes.addAll(StatsSetupConst.TABLE_PARAMS_STATS_KEYS);
      for (Map.Entry<String, String> e : sourceTab.getParameters().entrySet()) {
        if (e.getValue() == null) {
          continue;
        }
        if (excludes.contains(e.getKey())) {
          continue;
        }
        tblProperties.put(e.getKey(), HiveStringUtils.escapeHiveCommand(e.getValue()));
      }
    }
    super.addTblProperties(query, tblProperties);
  }

  /**
   * Part of Create operation. Insert-only compaction tables copy source tables' serde.
   */
  protected void copySerdeFromSourceTable(StringBuilder query) {
    if (storageDescriptor == null) {
      return; // avoid NPEs, don't throw an exception but skip this part of the query
    }
    ensureTableToCompactIsNative();
    SerDeInfo serdeInfo = storageDescriptor.getSerdeInfo();
    Map<String, String> serdeParams = serdeInfo.getParameters();
    query.append(" ROW FORMAT SERDE '").append(HiveStringUtils.escapeHiveCommand(serdeInfo.getSerializationLib()))
        .append("'");
    // WITH SERDEPROPERTIES
    if (!serdeParams.isEmpty()) {
      DDLPlanUtils.appendSerdeParams(query, serdeParams);
    }
    query.append("STORED AS INPUTFORMAT '")
        .append(HiveStringUtils.escapeHiveCommand(storageDescriptor.getInputFormat())).append("'")
        .append(" OUTPUTFORMAT '").append(HiveStringUtils.escapeHiveCommand(storageDescriptor.getOutputFormat()))
        .append("'");
  }

  private void ensureTableToCompactIsNative() {
    if (sourceTab == null) {
      return;
    }
    String storageHandler = sourceTab.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE);
    if (storageHandler != null) {
      String message =
          "Table " + sourceTab.getTableName() + "has a storage handler (" + storageHandler + "). Failing compaction for this non-native table.";
      LOG.error(message);
      throw new RuntimeException(message);
    }
  }
}
