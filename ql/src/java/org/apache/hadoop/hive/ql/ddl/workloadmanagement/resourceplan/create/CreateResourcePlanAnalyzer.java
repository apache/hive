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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.resourceplan.create;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for create resource plan commands.
 */
@DDLType(types = HiveParser.TOK_CREATE_RP)
public class CreateResourcePlanAnalyzer extends BaseSemanticAnalyzer {
  public CreateResourcePlanAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    if (root.getChildCount() == 0) {
      throw new SemanticException("Expected name in CREATE RESOURCE PLAN statement");
    }

    String resourcePlanName = unescapeIdentifier(root.getChild(0).getText());
    Integer queryParallelism = null;
    String likeName = null;
    boolean ifNotExists = false;

    for (int i = 1; i < root.getChildCount(); ++i) {
      Tree child = root.getChild(i);
      switch (child.getType()) {
      case HiveParser.TOK_QUERY_PARALLELISM:
        // Note: later we may be able to set multiple things together (except LIKE).
        if (queryParallelism == null && likeName == null) {
          queryParallelism = Integer.parseInt(child.getChild(0).getText());
        } else {
          throw new SemanticException("Conflicting create arguments " + root.toStringTree());
        }
        break;
      case HiveParser.TOK_LIKERP:
        if (queryParallelism == null && likeName == null) {
          likeName = unescapeIdentifier(child.getChild(0).getText());
        } else {
          throw new SemanticException("Conflicting create arguments " + root.toStringTree());
        }
        break;
      case HiveParser.TOK_IFNOTEXISTS:
        ifNotExists = true;
        break;
      default:
        throw new SemanticException("Invalid create arguments " + root.toStringTree());
      }
    }

    CreateResourcePlanDesc desc = new CreateResourcePlanDesc(resourcePlanName, queryParallelism, likeName, ifNotExists);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));

    DDLUtils.addServiceOutput(conf, getOutputs());
  }
}
