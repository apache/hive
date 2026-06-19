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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.mapping;

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLDesc;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.workloadmanagement.WMUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

/**
 * Abstract ancestor of Create and Alter WM Mapping analyzers.
 */
public abstract class AbstractVMMappingAnalyzer extends BaseSemanticAnalyzer {
  public AbstractVMMappingAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    if (root.getChildCount() < 4 || root.getChildCount() > 5) {
      throw new SemanticException("Invalid syntax for create or alter mapping.");
    }

    String resourcePlanName = unescapeIdentifier(root.getChild(0).getText());
    String entityType = root.getChild(1).getText();
    String entityName = PlanUtils.stripQuotes(root.getChild(2).getText());
    String poolPath = root.getChild(3).getType() == HiveParser.TOK_UNMANAGED ?
        null : WMUtils.poolPath(root.getChild(3)); // Null path => unmanaged
    Integer ordering = root.getChildCount() == 5 ? Integer.valueOf(root.getChild(4).getText()) : null;

    DDLDesc desc = getDesc(resourcePlanName, entityType, entityName, poolPath, ordering);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));

    DDLUtils.addServiceOutput(conf, getOutputs());
  }

  protected abstract DDLDesc getDesc(String resourcePlanName, String entityType, String entityName, String poolPath,
      Integer ordering);
}
