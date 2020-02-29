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

package org.apache.hadoop.hive.ql.ddl.misc.metadata;

import java.util.Map;

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.function.AbstractFunctionAnalyzer;
import org.apache.hadoop.hive.ql.ddl.table.partition.PartitionUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AnalyzeCommandUtils;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for cache metadata commands.
 */
@DDLType(types = HiveParser.TOK_CACHE_METADATA)
public class CacheMetadataAnalyzer extends AbstractFunctionAnalyzer {
  public CacheMetadataAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    Table table = AnalyzeCommandUtils.getTable(root, this);

    CacheMetadataDesc desc;
    // In 2 cases out of 3, we could pass the path and type directly to metastore...
    if (AnalyzeCommandUtils.isPartitionLevelStats(root)) {
      Map<String, String> partSpec = AnalyzeCommandUtils.getPartKeyValuePairsFromAST(table, root, conf);
      Partition part = PartitionUtils.getPartition(db, table, partSpec, true);
      desc = new CacheMetadataDesc(table.getDbName(), table.getTableName(), part.getName());
      inputs.add(new ReadEntity(part));
    } else {
      // Should we get all partitions for a partitioned table?
      desc = new CacheMetadataDesc(table.getDbName(), table.getTableName(), table.isPartitioned());
      inputs.add(new ReadEntity(table));
    }

    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }
}
