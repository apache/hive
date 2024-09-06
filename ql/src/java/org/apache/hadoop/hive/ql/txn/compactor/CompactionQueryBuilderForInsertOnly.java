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
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.DDLPlanUtils;
import org.apache.hadoop.hive.ql.util.DirectionUtils;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Field;
import java.lang.reflect.Modifier;
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

  /**
   * Construct a CompactionQueryBuilderForInsertOnly with required params.
   *
   * @param compactionType major or minor or rebalance, e.g. CompactionType.MAJOR.
   *                       Cannot be null.
   *                       Rebalance compaction is not supported for insert-only tables.
   * @throws IllegalArgumentException if compactionType is null or the compaction type is REBALANCE
   */
  CompactionQueryBuilderForInsertOnly(CompactionType compactionType) {
    super(compactionType, true);
    if (CompactionType.REBALANCE.equals(compactionType)) {
      throw new IllegalArgumentException("Rebalance compaction can only be supported on full ACID tables.");
    }
  }

  @Override
  protected void buildSelectClauseForInsert(StringBuilder query) {
    switch (compactionType) {
    case MAJOR: {
      if (sourcePartition != null) { //mmmajor and partitioned
        if (sourceTab != null) {
          List<FieldSchema> cols = sourceTab.getSd().getCols();
          appendColumns(query, cols, false);
          // If the source table is null, just skip to add this part to the query
        }
      } else { // mmmajor and unpartitioned
        query.append("*");
      }
      break;
    }
    case MINOR: {
      if (sourceTab != null) {
        List<FieldSchema> cols = sourceTab.getSd().getCols();
        appendColumns(query, cols, false);
        // If the source table is null, just skip to add this part to the query
      }
    }
    }
  }

  @Override
  protected void getSourceForInsert(StringBuilder query) {
    super.getSourceForInsert(query);
    if (CompactionType.MAJOR.equals(compactionType) && StringUtils.isNotBlank(orderByClause)) {
      query.append(orderByClause);
    }
  }

  @Override
  protected void buildWhereClauseForInsert(StringBuilder query) {
    if (CompactionType.MAJOR.equals(compactionType)) {
      super.buildWhereClauseForInsert(query);
    }
  }

  @Override
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
  @Override
  protected void defineColumns(StringBuilder query) {
    if (sourceTab != null) {
      query.append("(");
      List<String> columnDescs = getColumnDescs();
      query.append(StringUtils.join(columnDescs, ','));
      query.append(") ");
    }
  }

  /**
   * Part of Create operation. Copy source table bucketing for insert-only compaction.
   */
  private void getMmBucketing(StringBuilder query) {
    if (sourceTab != null) {
      boolean isFirst;
      List<String> buckCols = sourceTab.getSd().getBucketCols();
      if (!buckCols.isEmpty()) {
        query.append("CLUSTERED BY (").append(StringUtils.join(buckCols, ",")).append(") ");
        List<Order> sortCols = sourceTab.getSd().getSortCols();
        if (!sortCols.isEmpty()) {
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
  }

  /**
   * Part of Create operation. Insert-only compaction tables copy source tables.
   */
  private void getSkewedByClause(StringBuilder query) {
    if (sourceTab != null) {
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
  protected void addTblProperties(StringBuilder query) {
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
    if (storageDescriptor != null) {
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

  }

  private void ensureTableToCompactIsNative() {
    if (sourceTab != null) {
      String storageHandler = sourceTab.getParameters().get(hive_metastoreConstants.META_TABLE_STORAGE);
      if (storageHandler != null) {
        String message =
            "Table " + sourceTab.getTableName() + " has a storage handler (" + storageHandler + "). Failing compaction for this non-native table.";
        LOG.error(message);
        throw new RuntimeException(message);
      }
    }
  }

}
