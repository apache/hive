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

import java.util.List;

/**
 * Builds query strings that help with REBALANCE compaction of CRUD.
 */
class CompactionQueryBuilderForRebalance extends CompactionQueryBuilder {

  CompactionQueryBuilderForRebalance() {
    super(CompactionType.REBALANCE, false);
  }

  @Override
  protected void buildSelectClauseForInsert(StringBuilder query) {
    // Need list of columns. If the source table is null, don't throw
    // an exception, but skip this part of the query
    if (sourceTab != null) {
      List<FieldSchema> cols = sourceTab.getSd().getCols();
      query.append("0, t2.writeId, t2.rowId DIV CEIL(numRows / ").append(numberOfBuckets)
          .append("), t2.rowId, t2.writeId, t2.data from (select ")
          .append("count(ROW__ID.writeId) over() as numRows, ");
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
  }

  @Override
  protected void getSourceForInsert(StringBuilder query) {
    super.getSourceForInsert(query);
    if (StringUtils.isNotBlank(orderByClause)) {
      query.append(orderByClause);
    } else {
      query.append("order by ROW__ID.writeId ASC, ROW__ID.bucketId ASC, ROW__ID.rowId ASC");
    }
    query.append(") t2");
  }

  @Override
  protected void buildWhereClauseForInsert(StringBuilder query) {
    // No where clause in the insert query of rebalance compaction
  }

}
