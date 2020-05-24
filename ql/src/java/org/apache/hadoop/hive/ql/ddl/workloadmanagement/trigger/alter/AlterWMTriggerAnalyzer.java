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

package org.apache.hadoop.hive.ql.ddl.workloadmanagement.trigger.alter;

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.ddl.workloadmanagement.trigger.TriggerUtils;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for alter trigger commands.
 */
@DDLType(types = HiveParser.TOK_ALTER_TRIGGER)
public class AlterWMTriggerAnalyzer extends BaseSemanticAnalyzer {
  public AlterWMTriggerAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    if (root.getChildCount() != 4) {
      throw new SemanticException("Invalid syntax for alter trigger statement");
    }

    String resourcePlanName = unescapeIdentifier(root.getChild(0).getText());
    String triggerName = unescapeIdentifier(root.getChild(1).getText());
    String triggerExpression = TriggerUtils.buildTriggerExpression((ASTNode)root.getChild(2));
    String actionExpression = TriggerUtils.buildTriggerActionExpression((ASTNode)root.getChild(3));

    AlterWMTriggerDesc desc = new AlterWMTriggerDesc(resourcePlanName, triggerName, triggerExpression,
        actionExpression);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));

    DDLUtils.addServiceOutput(conf, getOutputs());
  }
}
