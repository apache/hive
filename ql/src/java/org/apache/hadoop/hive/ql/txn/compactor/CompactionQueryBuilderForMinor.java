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
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hive.common.util.HiveStringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Builds query strings that help with query-based MINOR compaction of CRUD.
 */
class CompactionQueryBuilderForMinor extends CompactionQueryBuilder {

  private static final Logger LOG = LoggerFactory.getLogger(CompactionQueryBuilderForMinor.class.getName());

  CompactionQueryBuilderForMinor() {
    super(CompactionType.MINOR, false);
  }

  @Override
  protected void buildSelectClauseForInsert(StringBuilder query) {
    query.append("`operation`, `originalTransaction`, `bucket`, `rowId`, `currentTransaction`, `row`");
  }

  @Override
  protected void buildWhereClauseForInsert(StringBuilder query) {
    if (validWriteIdList != null) {
      long[] invalidWriteIds = validWriteIdList.getInvalidWriteIds();
      if (invalidWriteIds.length > 0) {
        query.append(" where `originalTransaction` not in (")
            .append(StringUtils.join(ArrayUtils.toObject(invalidWriteIds), ",")).append(")");
      }
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
    int bucketingVersion = getMinorCrudBucketing(query);

    // STORED AS / ROW FORMAT SERDE + INPUTFORMAT + OUTPUTFORMAT
    query.append(" stored as orc");

    // LOCATION
    if (location != null) {
      query.append(" LOCATION '").append(HiveStringUtils.escapeHiveCommand(location)).append("'");
    }

    // TBLPROPERTIES
    addTblProperties(query, isBucketed, bucketingVersion);
  }

  /**
   * Part of Create operation. Minor crud compaction uses its own bucketing system.
   */
  private int getMinorCrudBucketing(StringBuilder query) {
    int bucketingVersion = 0;
    if (isBucketed && sourceTab != null) { // skip if sourceTab is null to avoid NPEs
      int numBuckets = 1;
      try {
        org.apache.hadoop.hive.ql.metadata.Table t = getTable();
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

  protected org.apache.hadoop.hive.ql.metadata.Table getTable() throws HiveException {
    return Hive.get().getTable(sourceTab.getDbName(), sourceTab.getTableName());
  }

}
