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

package org.apache.hadoop.hive.ql.ddl.view.materialized.drop;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.hooks.WriteEntity.WriteType;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for drop materialized view commands.
 */
@DDLType(types = HiveParser.TOK_DROP_MATERIALIZED_VIEW)
public class DropMaterializedViewAnalyzer extends BaseSemanticAnalyzer {
  public DropMaterializedViewAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    String viewName = getUnescapedName((ASTNode) root.getChild(0));
    boolean ifExists = (root.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null);

    boolean throwException = !ifExists && !HiveConf.getBoolVar(conf, ConfVars.DROP_IGNORES_NON_EXISTENT);
    Table materializedView = getTable(viewName, throwException);
    if (materializedView != null) {
      inputs.add(new ReadEntity(materializedView));

      boolean tableWithSuffix = AcidUtils.isTableSoftDeleteEnabled(materializedView, conf);
      outputs.add(new WriteEntity(materializedView,
        tableWithSuffix ? WriteType.DDL_EXCL_WRITE : WriteType.DDL_EXCLUSIVE));
    }

    DropMaterializedViewDesc desc = new DropMaterializedViewDesc(viewName, ifExists);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));
  }
}
