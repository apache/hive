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
package org.apache.hadoop.hive.ql.parse.rewrite.sql;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class NonNativeAcidMultiInsertSqlGenerator extends MultiInsertSqlGenerator {
  private final String deletePrefix;

  public NonNativeAcidMultiInsertSqlGenerator(
      Table table, String targetTableFullName, HiveConf conf, String subQueryAlias, String deletePrefix) {
    super(table, targetTableFullName, conf, subQueryAlias);
    this.deletePrefix = deletePrefix;
  }

  @Override
  public void appendAcidSelectColumns(Context.Operation operation) {
    appendAcidSelectColumns(operation, false, false);
  }

  @Override
  public void appendAcidSelectColumnsForDeletedRecords(Context.Operation operation, boolean skipPrefix) {
    appendAcidSelectColumns(operation, true, skipPrefix);
  }

  private void appendAcidSelectColumns(Context.Operation operation, boolean markRowIdAsDeleted, boolean skipPrefix) {
    List<FieldSchema> acidSelectColumns = targetTable.getStorageHandler().acidSelectColumns(targetTable, operation);
    for (FieldSchema fieldSchema : acidSelectColumns) {
      boolean deletedRowId = markRowIdAsDeleted && fieldSchema.equals(targetTable.getStorageHandler().getRowId());
      String identifier = deletedRowId ?
          "-1" : HiveUtils.unparseIdentifier(fieldSchema.getName(), this.conf);
      if (!markRowIdAsDeleted || skipPrefix) {
        queryStr.append(identifier);
      }

      if (StringUtils.isNotEmpty(deletePrefix) && (!markRowIdAsDeleted || !skipPrefix)) {
        if (!markRowIdAsDeleted) {
          queryStr.append(" AS ");
        }
        String prefixedIdentifier = deletedRowId ?
            "-1" : HiveUtils.unparseIdentifier(deletePrefix + fieldSchema.getName(), this.conf);
        queryStr.append(prefixedIdentifier);
      }
      queryStr.append(",");
    }
  }

  @Override
  public List<String> getDeleteValues(Context.Operation operation) {
    List<FieldSchema> acidSelectColumns = targetTable.getStorageHandler().acidSelectColumns(targetTable, operation);
    List<String> deleteValues = new ArrayList<>(acidSelectColumns.size());
    for (FieldSchema fieldSchema : acidSelectColumns) {
      String prefixedIdentifier = HiveUtils.unparseIdentifier(deletePrefix + fieldSchema.getName(), this.conf);
      deleteValues.add(qualify(prefixedIdentifier));
    }
    return deleteValues;
  }

  @Override
  public List<String> getSortKeys() {
    return targetTable.getStorageHandler().acidSortColumns(targetTable, Context.Operation.DELETE).stream()
        .map(fieldSchema -> qualify(
            HiveUtils.unparseIdentifier(deletePrefix + fieldSchema.getName(), this.conf)))
        .collect(Collectors.toList());
  }
}
