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

package org.apache.hadoop.hive.ql.ddl.policy.desc;

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.anon.utils.PolicyPrivilegeUtils;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import static org.apache.hadoop.hive.ql.anon.consts.AnonConst.PRIV_POLICY_VALIDATE;

/**
 * Analyzer for data erasure policy description commands.
 */
@DDLType(types = HiveParser.TOK_DESC_ERASURE_POLICY)
public class DescErasurePolicyAnalyzer extends BaseSemanticAnalyzer {
  public DescErasurePolicyAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    if (root.getChildCount() == 0 || root.getChildCount() > 2) {
      throw new SemanticException("Unexpected Tokens at DESCRIBE ERASURE POLICY");
    }

    ctx.setResFile(ctx.getLocalTmpPath());

    String policyName = stripQuotes(root.getChild(0).getText());
    // Optional VERSION 'label' selector (Listing 4): describe a specific version's
    // body instead of the active one. The 2nd child, when present, is the label.
    String versionLabel = (root.getChildCount() == 2)
        ? stripQuotes(root.getChild(1).getText()) : null;
    try {
      PolicyPrivilegeUtils.requirePrivilege(conf, Hive.get(conf), PRIV_POLICY_VALIDATE, policyName);
    } catch (org.apache.hadoop.hive.ql.metadata.HiveException he) {
      throw new SemanticException("DESCRIBE ERASURE POLICY: privilege check failed: " + he.getMessage());
    }

    DescErasurePolicyDesc desc = new DescErasurePolicyDesc(ctx.getResFile(), policyName, versionLabel, false);
    Task<DDLWork> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);

    task.setFetchSource(true);
    setFetchTask(createFetchTask(desc.getSchema()));
  }
}
