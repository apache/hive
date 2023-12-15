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
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.*;

/**
 * Builds query strings that help with REBALANCE compaction of CRUD.
 */
class CompactionQueryBuilderForRebalance extends CompactionQueryBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionQueryBuilderForRebalance.class.getName());

  private int numberOfBuckets;

  /**
   * Construct a CompactionQueryBuilderForRebalance with required params.
   *
   * @param compactionType major or minor or rebalance, e.g. CompactionType.MAJOR.
   *                       Cannot be null.
   * @param operation query's Operation e.g. Operation.CREATE.
   * @throws IllegalArgumentException if compactionType is null
   */
  CompactionQueryBuilderForRebalance(CompactionType compactionType, Operation operation, String resultTableName) {
    super(compactionType, operation, false, resultTableName);
  }

  protected void buildSelectClauseForInsert(StringBuilder query) {
    // Need list of columns for major crud, mmmajor partitioned, mmminor
    if (sourceTab == null) {
      return; // avoid NPEs, don't throw an exception but skip this part of the query
    }
    List<FieldSchema> cols = sourceTab.getSd().getCols();
    query.append("0, t2.writeId, t2.rowId DIV CEIL(numRows / ").append(numberOfBuckets)
        .append("), t2.rowId, t2.writeId, t2.data from (select ").append("count(ROW__ID.writeId) over() as numRows, ");
    if (StringUtils.isNotBlank(orderByClause)) {
      // in case of reordering the data the writeids cannot be kept.
      query.append("MAX(ROW__ID.writeId) over() as writeId, row_number() OVER (").append(orderByClause);
    } else {
      query.append(
          "ROW__ID.writeId as writeId, row_number() OVER (order by ROW__ID.writeId ASC, ROW__ID.bucketId ASC, ROW__ID.rowId ASC");
    }
    query.append(") - 1 AS rowId, NAMED_STRUCT(");
    for (int i = 0; i < cols.size(); ++i) {
      query.append(i == 0 ? "'" : ", '").append(cols.get(i).getName()).append("', `").append(cols.get(i).getName())
          .append("`");
    }
    query.append(") as data");
  }

  protected void getSourceForInsert(StringBuilder query) {
    if (sourceTabForInsert != null) {
      query.append(sourceTabForInsert);
    } else {
      query.append(sourceTab.getDbName()).append(".").append(sourceTab.getTableName());
    }
    query.append(" ");
    if (StringUtils.isNotBlank(orderByClause)) {
      query.append(orderByClause);
    } else {
      query.append("order by ROW__ID.writeId ASC, ROW__ID.bucketId ASC, ROW__ID.rowId ASC");
    }
    query.append(") t2");
  }

  protected void buildWhereClauseForInsert(StringBuilder query) {
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
   * compaction tables.
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

  /**
   * Sets the target number of implicit buckets for a rebalancing compaction
   * @param numberOfBuckets The target number of buckets
   */
  public CompactionQueryBuilderForRebalance setNumberOfBuckets(int numberOfBuckets) {
    this.numberOfBuckets = numberOfBuckets;
    return this;
  }

}
