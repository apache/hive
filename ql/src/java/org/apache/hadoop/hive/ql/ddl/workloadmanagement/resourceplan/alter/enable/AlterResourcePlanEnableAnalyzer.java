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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.resourceplan.alter.enable;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for enable resource plan commands.
 */
@DDLType(types = HiveParser.TOK_ALTER_RP_ENABLE)
public class AlterResourcePlanEnableAnalyzer extends BaseSemanticAnalyzer {
  public AlterResourcePlanEnableAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    if (root.getChildCount() == 0) {
      console.printError("Activate a resource plan to enable workload management!");
      return;
    }

    String resourcePlanName = unescapeIdentifier(root.getChild(0).getText());

    boolean enable = false;
    boolean activate = false;
    boolean replace = false;

    for (int i = 1; i < root.getChildCount(); ++i) {
      Tree child = root.getChild(i);
      switch (child.getType()) {
      case HiveParser.TOK_ACTIVATE:
        activate = true;
        if (child.getChildCount() > 1) {
          throw new SemanticException("Expected 0 or 1 arguments " + root.toStringTree());
        } else if (child.getChildCount() == 1) {
          if (child.getChild(0).getType() != HiveParser.TOK_REPLACE) {
            throw new SemanticException("Incorrect syntax " + root.toStringTree());
          }
          replace = true;
        }
        break;
      case HiveParser.TOK_ENABLE:
        enable = true;
        break;
      case HiveParser.TOK_REPLACE:
        replace = true;
        break;
      default:
        throw new SemanticException("Unexpected token in alter resource plan statement: " + child.getType());
      }
    }

    AlterResourcePlanEnableDesc desc = new AlterResourcePlanEnableDesc(resourcePlanName, enable, activate, replace);
    Task<DDLWork> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);

    DDLUtils.addServiceOutput(conf, getOutputs());
  }
}
