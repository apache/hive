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

package org.apache.hadoop.hive.ql.metadata;

import java.util.ArrayList;
import java.util.EnumMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.vector.LongColumnVector;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.parse.rewrite.MergeStatement;
import org.apache.hadoop.hive.ql.parse.rewrite.sql.MultiInsertSqlGenerator;
import org.apache.hadoop.hive.ql.session.SessionStateUtil;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.Type;

public class RowLineageUtils {

  public static final Map<VirtualColumn, String> ROW_LINEAGE_COLUMNS_TO_FILE_NAME = new EnumMap<>(VirtualColumn.class);

  static {
    ROW_LINEAGE_COLUMNS_TO_FILE_NAME.put(VirtualColumn.ROW_LINEAGE_ID, "_row_id");
    ROW_LINEAGE_COLUMNS_TO_FILE_NAME.put(VirtualColumn.LAST_UPDATED_SEQUENCE_NUMBER, "_last_updated_sequence_number");
  }

  public static boolean isRowLineageColumnPresent(VectorizedRowBatchCtx rbCtx, MessageType fileSchema,
      VirtualColumn rowLineageColumn) {
    return rbCtx.isVirtualColumnNeeded(rowLineageColumn.getName()) && fileSchema.containsField(
        ROW_LINEAGE_COLUMNS_TO_FILE_NAME.get(rowLineageColumn));
  }

  public static void addRowLineageColumnsForUpdate(Table table, MultiInsertSqlGenerator sqlGenerator,
      List<String> insertValues, Configuration conf) {
    if (supportsRowLineage(table)) {
      SessionStateUtil.addResource(conf, SessionStateUtil.ROW_LINEAGE, true);
      // copy ROW_ID
      sqlGenerator.append(",");
      sqlGenerator.append(HiveUtils.unparseIdentifier(VirtualColumn.ROW_LINEAGE_ID.getName(), conf));

      // reset LAST_UPDATED_SEQUENCE_NUMBER
      sqlGenerator.append(", NULL AS ");
      sqlGenerator.append(HiveUtils.unparseIdentifier(VirtualColumn.LAST_UPDATED_SEQUENCE_NUMBER.getName(), conf));

      insertValues.add(sqlGenerator.qualify(VirtualColumn.ROW_LINEAGE_ID.getName()));
      insertValues.add(sqlGenerator.qualify(VirtualColumn.LAST_UPDATED_SEQUENCE_NUMBER.getName()));
    }
  }

  public static boolean supportsRowLineage(Table table) {
    if (table.getStorageHandler() == null) {
      return false;
    }
    return table.getStorageHandler().supportsRowLineage(table.getParameters());
  }

  public static boolean shouldAddRowLineageColumnsForMerge(MergeStatement mergeStatement, Configuration conf) {
    boolean shouldAddRowLineageColumns =
        supportsRowLineage(mergeStatement.getTargetTable()) && mergeStatement.hasWhenMatchedUpdateClause();
    SessionStateUtil.addResource(conf, SessionStateUtil.ROW_LINEAGE, shouldAddRowLineageColumns);
    return shouldAddRowLineageColumns;
  }

  public static void addSourceColumnsForRowLineage(boolean isRowLineageSupported, MultiInsertSqlGenerator sqlGenerator,
      Configuration conf) {
    if (isRowLineageSupported) {
      SessionStateUtil.addResource(conf, SessionStateUtil.ROW_LINEAGE, true);
      sqlGenerator.append(", ");
      sqlGenerator.append(HiveUtils.unparseIdentifier(VirtualColumn.ROW_LINEAGE_ID.getName(), conf));
      sqlGenerator.append(", ");
      sqlGenerator.append(HiveUtils.unparseIdentifier(VirtualColumn.LAST_UPDATED_SEQUENCE_NUMBER.getName(), conf));
    }
  }

  public static void addRowLineageColumnsForWhenMatchedUpdateClause(boolean isRowLineageSupported, List<String> values,
      String targetAlias, Configuration conf) {
    if (isRowLineageSupported) {
      values.add(targetAlias + "." + HiveUtils.unparseIdentifier(VirtualColumn.ROW_LINEAGE_ID.getName(), conf));
      values.add("NULL AS " + HiveUtils.unparseIdentifier(VirtualColumn.LAST_UPDATED_SEQUENCE_NUMBER.getName(), conf));
    }
  }

  public static List<String> addRowLineageValuesForAppendWhenNotMatchedClause(boolean isRowLineageSupported,
      List<String> insertClauseValues) {
    if (isRowLineageSupported) {
      List<String> selectValues = new ArrayList<>(insertClauseValues);
      // Pad for Iceberg writer schema
      selectValues.add("NULL"); // ROW_ID
      selectValues.add("NULL"); // LAST_UPDATED_SEQUENCE_NUMBER
      return selectValues;
    }
    return insertClauseValues;
  }

  public static void initializeRowLineageColumns(VectorizedRowBatchCtx vrbCtx, VectorizedRowBatch batch) {
    int rowIdIdx = vrbCtx.findVirtualColumnNum(VirtualColumn.ROW_LINEAGE_ID);
    if (rowIdIdx != -1) {
      LongColumnVector rowIdLcv = (LongColumnVector) batch.cols[rowIdIdx];
      rowIdLcv.vector[0] = -1L;
    }
    int lusnIdx = vrbCtx.findVirtualColumnNum(VirtualColumn.LAST_UPDATED_SEQUENCE_NUMBER);
    if (lusnIdx != -1) {
      LongColumnVector lusnLcv = (LongColumnVector) batch.cols[lusnIdx];
      lusnLcv.vector[0] = -1L;
    }
  }

  public static boolean isRowLineageInsert(Configuration conf) {
    // Returns true only if the resource exists and is set to true
    return SessionStateUtil.getResource(conf, SessionStateUtil.ROW_LINEAGE).map(Boolean.class::cast).orElse(false);
  }

  public static MessageType getRequestedSchemaWithRowLineageColumns(VectorizedRowBatchCtx rbCtx,
      MessageType requestedSchema, MessageType fileSchema, List<Integer> colsToInclude) {
    List<Type> newFields = new ArrayList<>(requestedSchema.getFields());
    if (isRowLineageColumnPresent(rbCtx, fileSchema, VirtualColumn.ROW_LINEAGE_ID)) {
      colsToInclude.add(rbCtx.findVirtualColumnNum(VirtualColumn.ROW_LINEAGE_ID));
      newFields.add(fileSchema.getType(ROW_LINEAGE_COLUMNS_TO_FILE_NAME.get(VirtualColumn.ROW_LINEAGE_ID)));
    }
    if (isRowLineageColumnPresent(rbCtx, fileSchema, VirtualColumn.LAST_UPDATED_SEQUENCE_NUMBER)) {
      colsToInclude.add(rbCtx.findVirtualColumnNum(VirtualColumn.LAST_UPDATED_SEQUENCE_NUMBER));
      newFields.add(
          fileSchema.getType(ROW_LINEAGE_COLUMNS_TO_FILE_NAME.get(VirtualColumn.LAST_UPDATED_SEQUENCE_NUMBER)));
    }
    if (newFields.size() != requestedSchema.getFields().size()) {
      return new MessageType(requestedSchema.getName(), newFields);
    }
    return requestedSchema;
  }
}


