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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.pool.alter;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.workloadmanagement.WMUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

/**
 * Analyzer for alter pool commands.
 */
@DDLType(types = HiveParser.TOK_ALTER_POOL)
public class AlterWMPoolAnalyzer extends BaseSemanticAnalyzer {
  public AlterWMPoolAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    if (root.getChildCount() < 3) {
      throw new SemanticException("Invalid syntax for alter pool: " + root.toStringTree());
    }

    String resourcePlanName = unescapeIdentifier(root.getChild(0).getText());
    String poolPath = WMUtils.poolPath(root.getChild(1));
    Double allocFraction = null;
    Integer queryParallelism = null;
    String schedulingPolicy = null;
    boolean removeSchedulingPolicy = false;
    String newPath = null;

    for (int i = 2; i < root.getChildCount(); ++i) {
      Tree child = root.getChild(i);
      if (child.getChildCount() != 1) {
        throw new SemanticException("Invalid syntax in alter pool expected parameter.");
      }
      Tree param = child.getChild(0);
      switch (child.getType()) {
      case HiveParser.TOK_ALLOC_FRACTION:
        allocFraction = Double.parseDouble(param.getText());
        break;
      case HiveParser.TOK_QUERY_PARALLELISM:
        queryParallelism = Integer.parseInt(param.getText());
        break;
      case HiveParser.TOK_SCHEDULING_POLICY:
        if (param.getType() != HiveParser.TOK_NULL) {
          schedulingPolicy = PlanUtils.stripQuotes(param.getText());
        } else {
          removeSchedulingPolicy = true;
        }
        break;
      case HiveParser.TOK_PATH:
        newPath = WMUtils.poolPath(param);
        break;
      default:
        throw new SemanticException("Incorrect alter syntax: " + child.toStringTree());
      }
    }

    AlterWMPoolDesc desc = new AlterWMPoolDesc(resourcePlanName, poolPath, allocFraction, queryParallelism,
        schedulingPolicy, removeSchedulingPolicy, newPath);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));

    DDLUtils.addServiceOutput(conf, getOutputs());
  }
}
