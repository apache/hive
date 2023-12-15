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

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Builds query strings that help with query-based MINOR compaction of CRUD.
 */
class CompactionQueryBuilderForMinor extends CompactionQueryBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionQueryBuilderForMinor.class.getName());

  /**
   * Construct a CompactionQueryBuilderMinor with required params.
   *
   * @param compactionType major or minor or rebalance, e.g. CompactionType.MAJOR.
   *                       Cannot be null.
   * @param operation query's Operation e.g. Operation.CREATE.
   * @throws IllegalArgumentException if compactionType is null
   */
  CompactionQueryBuilderForMinor(CompactionType compactionType, Operation operation, String resultTableName) {
    super(compactionType, operation, false, resultTableName);
  }

  protected void buildSelectClauseForInsert(StringBuilder query) {
    // Need list of columns for major crud, mmmajor partitioned, mmminor
    query.append("`operation`, `originalTransaction`, `bucket`, `rowId`, `currentTransaction`, `row`");
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
    if (validWriteIdList != null) {
      if (validWriteIdList != null) {
        long[] invalidWriteIds = validWriteIdList.getInvalidWriteIds();
        if (invalidWriteIds.length > 0) {
          query.append(" where `originalTransaction` not in (")
              .append(StringUtils.join(ArrayUtils.toObject(invalidWriteIds), ",")).append(")");
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
    int bucketingVersion = 0;
    if (CompactionType.MINOR.equals(compactionType)) {
      bucketingVersion = getMinorCrudBucketing(query, bucketingVersion);
    }

    // STORED AS / ROW FORMAT SERDE + INPUTFORMAT + OUTPUTFORMAT
    query.append(" stored as orc");

    // LOCATION
    if (location != null) {
      query.append(" LOCATION '").append(HiveStringUtils.escapeHiveCommand(location)).append("'");
    }

    // TBLPROPERTIES
    addTblProperties(query, bucketingVersion);
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
    List<FieldSchema> cols = sourceTab.getSd().getCols();
    List<String> columnDescs = getColumnDescs();
    query.append(StringUtils.join(columnDescs, ','));
    query.append(">)");
  }

  /**
   * Part of Create operation. Minor crud compaction uses its own bucketing system.
   */
  private int getMinorCrudBucketing(StringBuilder query, int bucketingVersion) {
    if (isBucketed && sourceTab != null) { // skip if sourceTab is null to avoid NPEs
      int numBuckets = 1;
      try {
        org.apache.hadoop.hive.ql.metadata.Table t =
            Hive.get().getTable(sourceTab.getDbName(), sourceTab.getTableName());
        numBuckets = Math.max(t.getNumBuckets(), numBuckets);
        bucketingVersion = t.getBucketingVersion();
      } catch (HiveException e) {
        LOG.info("Error finding table {}. Minor compaction result will use 0 buckets.", sourceTab.getTableName());
      } finally {
        query.append(" clustered by (`bucket`)").append(" sorted by (`originalTransaction`, `bucket`, `rowId`)")
            .append(" into ").append(numBuckets).append(" buckets");
      }
    }
    return bucketingVersion;
  }

  /**
   * Part of Create operation. All tmp tables are not transactional and are marked as
   * compaction tables. Additionally...
   * - Crud compaction temp tables need tblproperty, "compactiontable."
   * - Minor crud compaction temp tables need bucketing version tblproperty, if table is bucketed.
   */
  private void addTblProperties(StringBuilder query, int bucketingVersion) {
    Map<String, String> tblProperties = new HashMap<>();
    tblProperties.put("transactional", "false");
    tblProperties.put(AcidUtils.COMPACTOR_TABLE_PROPERTY, compactionType.name());
    if (isBucketed) {
      tblProperties.put("bucketing_version", String.valueOf(bucketingVersion));
    }
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
