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
import org.apache.hadoop.hive.metastore.ColumnType;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds query strings that help with query-based MAJOR compaction of CRUD.
 */
class CompactionQueryBuilderForMajor extends CompactionQueryBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionQueryBuilderForMajor.class.getName());

  /**
   * Construct a CompactionQueryBuilderForMajor with required params.
   *
   * @param compactionType major or minor or rebalance, e.g. CompactionType.MAJOR.
   *                       Cannot be null.
   * @param operation query's Operation e.g. Operation.CREATE.
   * @throws IllegalArgumentException if compactionType is null
   */
  CompactionQueryBuilderForMajor(CompactionType compactionType, Operation operation, String resultTableName) {
    super(compactionType, operation, false, resultTableName);
  }

  protected void buildSelectClauseForInsert(StringBuilder query) {
    // Need list of columns for major crud, mmmajor partitioned, mmminor
    if (sourceTab == null) {
      return; // avoid NPEs, don't throw an exception but skip this part of the query
    }
    List<FieldSchema> cols = sourceTab.getSd().getCols();

    query.append(
        "validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId), " + "ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, " + "NAMED_STRUCT(");
    appendColumns(query, cols, true);
    query.append(") ");
  }

  protected void getSourceForInsert(StringBuilder query) {
    if (sourceTabForInsert != null) {
      query.append(sourceTabForInsert);
    } else {
      query.append(sourceTab.getDbName()).append(".").append(sourceTab.getTableName());
    }
    query.append(" ");
  }

  protected void buildWhereClauseForInsert(StringBuilder query) {
    if (sourcePartition != null && sourceTab != null) {
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

    // STORED AS / ROW FORMAT SERDE + INPUTFORMAT + OUTPUTFORMAT
    query.append(" stored as orc");

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
    query.append(
        "`operation` int, `originalTransaction` bigint, `bucket` int, `rowId` bigint, " + "`currentTransaction` bigint, `row` struct<");
    List<String> columnDescs = getColumnDescs();
    query.append(StringUtils.join(columnDescs, ','));
    query.append(">) ");
  }

  /**
   * Part of Create operation. All tmp tables are not transactional and are marked as
   * compaction tables. Additionally...
   * - Crud compaction temp tables need tblproperty, "compactiontable."
   */
  private void addTblProperties(StringBuilder query) {
    Map<String, String> tblProperties = new HashMap<>();
    tblProperties.put("transactional", "false");
    tblProperties.put(AcidUtils.COMPACTOR_TABLE_PROPERTY, compactionType.name());
    if (sourceTab != null) { // to avoid NPEs, skip this part if sourceTab is null
      for (Map.Entry<String, String> e : sourceTab.getParameters().entrySet()) {
        if (e.getKey().startsWith("orc.")) {
          tblProperties.put(e.getKey(), HiveStringUtils.escapeHiveCommand(e.getValue()));
        }
      }
    }
    super.addTblProperties(query, tblProperties);
  }
}
