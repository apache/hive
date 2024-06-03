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

import org.apache.hadoop.hive.metastore.api.CompactionType;
import org.apache.hadoop.hive.metastore.api.FieldSchema;

import java.util.List;

/**
 * Builds query strings that help with query-based MAJOR compaction of CRUD.
 */
class CompactionQueryBuilderForMajor extends CompactionQueryBuilder {

  CompactionQueryBuilderForMajor() {
    super(CompactionType.MAJOR, false);
  }

  @Override
  protected void buildSelectClauseForInsert(StringBuilder query) {
    // Need list of columns fir major crud compaction.
    // If the source table is null, don't throw an exception, but skip this part of the query
    if (sourceTab != null) {
      List<FieldSchema> cols = sourceTab.getSd().getCols();

      query.append(
          "validate_acid_sort_order(ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId), ROW__ID.writeId, ROW__ID.bucketId, ROW__ID.rowId, ROW__ID.writeId, NAMED_STRUCT(");
      appendColumns(query, cols, true);
      query.append(") ");
    }
  }

  @Override
  protected void buildWhereClauseForInsert(StringBuilder query) {
    super.buildWhereClauseForInsert(query);
  }
}
