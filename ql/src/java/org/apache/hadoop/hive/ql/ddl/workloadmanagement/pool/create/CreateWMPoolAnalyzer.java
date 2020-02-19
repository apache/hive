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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.pool.create;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.workloadmanagement.WMUtils;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

/**
 * Analyzer for create pool commands.
 */
@DDLType(types = HiveParser.TOK_CREATE_POOL)
public class CreateWMPoolAnalyzer extends BaseSemanticAnalyzer {
  public CreateWMPoolAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    // TODO: allow defaults for e.g. scheduling policy.
    if (root.getChildCount() < 3) {
      throw new SemanticException("Expected more arguments: " + root.toStringTree());
    }

    String resourcePlanName = unescapeIdentifier(root.getChild(0).getText());
    String poolPath = WMUtils.poolPath(root.getChild(1));
    Double allocFraction = null;
    Integer queryParallelism = null;
    String schedulingPolicy = null;

    for (int i = 2; i < root.getChildCount(); ++i) {
      Tree child = root.getChild(i);
      if (child.getChildCount() != 1) {
        throw new SemanticException("Expected 1 paramter for: " + child.getText());
      }

      String param = child.getChild(0).getText();
      switch (child.getType()) {
      case HiveParser.TOK_ALLOC_FRACTION:
        allocFraction = Double.parseDouble(param);
        break;
      case HiveParser.TOK_QUERY_PARALLELISM:
        queryParallelism = Integer.parseInt(param);
        break;
      case HiveParser.TOK_SCHEDULING_POLICY:
        schedulingPolicy = PlanUtils.stripQuotes(param);
        break;
      case HiveParser.TOK_PATH:
        throw new SemanticException("Invalid parameter path in create pool");
      default:
        throw new SemanticException("Invalid parameter " + child.getText() + " in create pool");
      }
    }

    if (allocFraction == null) {
      throw new SemanticException("alloc_fraction should be specified for a pool");
    }
    if (queryParallelism == null) {
      throw new SemanticException("query_parallelism should be specified for a pool");
    }

    CreateWMPoolDesc desc = new CreateWMPoolDesc(resourcePlanName, poolPath, allocFraction, queryParallelism,
        schedulingPolicy);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));

    DDLUtils.addServiceOutput(conf, getOutputs());
  }
}
