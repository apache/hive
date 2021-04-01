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
package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.Map;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.plan.HiveOperation;

/**
 * Analyzer for stats dropping commands.
 */
public class DropStatsSemanticAnalyzer extends SemanticAnalyzer {
  public DropStatsSemanticAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    LOG.debug("Drop statistics semantic analyzer");
    assert root.getType() == HiveParser.TOK_DROP_STATS;
    if(!isImpalaPlan(conf)) {
      throw new SemanticException("DROP STATISTICS command only works for Impala execution engine");
    }
    ASTNode tableNode = (ASTNode) root.getChild(0);
    Table tbl = getTable(getUnescapedName((ASTNode) tableNode.getChild(0)), true);
    if (tbl.isView()) {
      throw new SemanticException("DROP STATISTICS cannot be executed for views");
    }
    Map<String, String> partitionSpec = getPartSpec((ASTNode) tableNode.getChild(1));
    boolean incrementalStats = AnalyzeCommandUtils.isIncrementalStats(root);
    if (incrementalStats && partitionSpec == null) {
      throw new SemanticException("Partition needs to be statically specified in DROP INCREMENTAL STATISTICS");
    }
    if(partitionSpec != null) {
      if (!incrementalStats) {
        throw new SemanticException(
            "Partitions cannot be statically specified in DROP STATISTICS in Impala without INCREMENTAL");
      }
      // if partitionSpec is specified than validate that all partitions are specified
      validatePartSpec(tbl, partitionSpec, tableNode, conf, true);
    }
    LOG.debug("Drop statistics analysis completed");
    super.analyzeInternal(root);
    queryState.setCommandType(HiveOperation.DROP_STATS);
    // The query result will contain a single row with a 'summary' string column.
    this.resultSchema = new ArrayList<>();
    this.resultSchema.add(new FieldSchema("summary", "string", ""));
  }

}
