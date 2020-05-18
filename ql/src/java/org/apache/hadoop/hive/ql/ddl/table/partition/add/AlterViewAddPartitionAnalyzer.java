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

package org.apache.hadoop.hive.ql.ddl.table.partition.add;

import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for add partition commands for views.
 */
@DDLType(types = HiveParser.TOK_ALTERVIEW_ADDPARTS)
public class AlterViewAddPartitionAnalyzer extends AbstractAddPartitionAnalyzer {
  public AlterViewAddPartitionAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected boolean expectView() {
    return true;
  }

  @Override
  protected boolean allowLocation() {
    return false;
  }

  private static final String VIEW_VALIDATE_QUERY =
      "SELECT *%n" +
      "  FROM %s%n" +
      " WHERE %s";

  @Override
  protected void postProcess(TableName tableName, Table table, AlterTableAddPartitionDesc desc, Task<DDLWork> ddlTask)
      throws SemanticException {
    // Compile internal query to capture underlying table partition dependencies
    String dbTable = HiveUtils.unparseIdentifier(tableName.getDb(), conf) + "." +
        HiveUtils.unparseIdentifier(tableName.getTable(), conf);

    StringBuilder where = new StringBuilder();
    boolean firstOr = true;
    for (AlterTableAddPartitionDesc.PartitionDesc partitionDesc : desc.getPartitions()) {
      if (firstOr) {
        firstOr = false;
      } else {
        where.append(" OR ");
      }
      boolean firstAnd = true;
      where.append("(");
      for (Map.Entry<String, String> entry : partitionDesc.getPartSpec().entrySet()) {
        if (firstAnd) {
          firstAnd = false;
        } else {
          where.append(" AND ");
        }
        where.append(HiveUtils.unparseIdentifier(entry.getKey(), conf));
        where.append(" = '");
        where.append(HiveUtils.escapeString(entry.getValue()));
        where.append("'");
      }
      where.append(")");
    }

    String query = String.format(VIEW_VALIDATE_QUERY, dbTable, where.toString());
    // FIXME: is it ok to have a completely new querystate?
    try (Driver driver = new Driver(QueryState.getNewQueryState(conf, queryState.getLineageState()))) {
      int rc = driver.compile(query, false);
      if (rc != 0) {
        throw new SemanticException(ErrorMsg.NO_VALID_PARTN.getMsg());
      }
      inputs.addAll(driver.getPlan().getInputs());
    }
  }
}
