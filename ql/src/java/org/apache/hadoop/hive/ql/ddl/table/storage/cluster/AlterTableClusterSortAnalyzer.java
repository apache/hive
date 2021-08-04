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

package org.apache.hadoop.hive.ql.ddl.table.storage.cluster;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.AbstractAlterTableDesc;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for table cluster sort commands.
 */
@DDLType(types = HiveParser.TOK_ALTERTABLE_CLUSTER_SORT)
public class AlterTableClusterSortAnalyzer extends AbstractAlterTableAnalyzer {
  public AlterTableClusterSortAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  protected void analyzeCommand(TableName tableName, Map<String, String> partitionSpec, ASTNode command)
      throws SemanticException {
    AbstractAlterTableDesc desc;
    switch (command.getChild(0).getType()) {
    case HiveParser.TOK_NOT_CLUSTERED:
      desc = new AlterTableNotClusteredDesc(tableName, partitionSpec);
      break;
    case HiveParser.TOK_NOT_SORTED:
      desc = new AlterTableNotSortedDesc(tableName, partitionSpec);
      break;
    case HiveParser.TOK_ALTERTABLE_BUCKETS:
      ASTNode buckets = (ASTNode) command.getChild(0);
      List<String> bucketCols = getColumnNames((ASTNode) buckets.getChild(0));
      List<Order> sortCols = new ArrayList<Order>();
      int numBuckets = -1;
      if (buckets.getChildCount() == 2) {
        numBuckets = Integer.parseInt(buckets.getChild(1).getText());
      } else {
        sortCols = getColumnNamesOrder((ASTNode) buckets.getChild(1));
        numBuckets = Integer.parseInt(buckets.getChild(2).getText());
      }
      if (numBuckets <= 0) {
        throw new SemanticException(ErrorMsg.INVALID_BUCKET_NUMBER.getMsg());
      }

      desc = new AlterTableClusteredByDesc(tableName, partitionSpec, numBuckets, bucketCols, sortCols);
      break;
    default:
      throw new SemanticException("Invalid operation " + command.getChild(0).getType());
    }

    addInputsOutputsAlterTable(tableName, partitionSpec, desc, desc.getType(), false);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));

    setAcidDdlDesc(getTable(tableName), desc);
  }
}
