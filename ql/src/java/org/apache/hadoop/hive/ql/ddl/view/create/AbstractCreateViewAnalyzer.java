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

package org.apache.hadoop.hive.ql.ddl.view.create;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzerFactory;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

/**
 * Abstract ancestor of analyzers that can create a view.
 */
public abstract class AbstractCreateViewAnalyzer extends BaseSemanticAnalyzer {
  AbstractCreateViewAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  protected List<FieldSchema> schema;

  @Override
  public List<FieldSchema> getResultSchema() {
    return schema;
  }

  protected SemanticAnalyzer analyzeQuery(ASTNode select, String fqViewName) throws SemanticException {
    QueryState innerQueryState = new QueryState.Builder().withHiveConf(conf)
      .withValidTxnList(queryState::getValidTxnList)
      .build();
    innerQueryState.getConf().setBoolVar(HiveConf.ConfVars.HIVE_RESULTSET_USE_UNIQUE_COLUMN_NAMES, false);

    SemanticAnalyzer analyzer = (SemanticAnalyzer) SemanticAnalyzerFactory.get(innerQueryState, select);
    ctx.setEnableUnparse(true);
    analyzer.forViewCreation(fqViewName);
    analyzer.analyze(select, ctx);
    analyzer.executeUnParseTranslations();

    queryState.setLineageState(innerQueryState.getLineageState());
    queryState.getLineageState().mapDirToOp(new Path(fqViewName), analyzer.getSinkOp());

    addInputs(analyzer);

    return analyzer;
  }

  private void addInputs(SemanticAnalyzer analyzer) {
    inputs.addAll(analyzer.getInputs());
    for (Map.Entry<String, TableScanOperator> entry : analyzer.getTopOps().entrySet()) {
      String alias = entry.getKey();
      TableScanOperator topOp = entry.getValue();
      ReadEntity parentViewInfo = PlanUtils.getParentViewInfo(alias, analyzer.getViewAliasToInput());

      // Adds tables only for create view (PPD filter can be appended by outer query)
      Table table = topOp.getConf().getTableMetadata();
      PlanUtils.addInput(inputs, new ReadEntity(table, parentViewInfo));
    }
  }

  public static void validateTablesUsed(SemanticAnalyzer analyzer) throws SemanticException {
    // Do not allow view to be defined on temp table or other materialized view
    for (TableScanOperator ts : analyzer.getTopOps().values()) {
      Table table = ts.getConf().getTableMetadata();
      if (SemanticAnalyzer.DUMMY_TABLE.equals(table.getTableName())) {
        continue;
      }

      if (table.isTemporary()) {
        throw new SemanticException("View definition references temporary table " + table.getCompleteName());
      }

      if (table.isMaterializedView()) {
        throw new SemanticException("View definition references materialized view " + table.getCompleteName());
      }
    }
  }

  protected void validateReplaceWithPartitions(String viewName, Table oldView, List<FieldSchema> partitionColumns)
      throws SemanticException {
    if (oldView.getPartCols().isEmpty() || oldView.getPartCols().equals(partitionColumns)) {
      return;
    }

    String partitionViewErrorMsg = "The following view has partition, it could not be replaced: " + viewName;
    List<Partition> partitions = null;
    try {
      partitions = db.getPartitions(oldView);
    } catch (HiveException e) {
      throw new SemanticException(ErrorMsg.REPLACE_VIEW_WITH_PARTITION.getMsg(partitionViewErrorMsg));
    }

    if (!partitions.isEmpty()) {
      throw new SemanticException(ErrorMsg.REPLACE_VIEW_WITH_PARTITION.getMsg(partitionViewErrorMsg));
    }
  }
}
