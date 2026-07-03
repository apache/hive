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

package org.apache.hadoop.hive.ql.ddl.policy.show;

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.anon.utils.PolicyPrivilegeUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for {@code SHOW ERASURE GRANT [FOR principal] [ON POLICY policyName]}.
 * Surfaces the {@code MPolicyPriv} catalog through SQL so operators can
 * inspect which principal holds which policy-scoped privilege without
 * going through the Metastore Thrift API directly.
 */
@DDLType(types = HiveParser.TOK_SHOW_ERASURE_GRANTS)
public class ShowErasureGrantsAnalyzer extends BaseSemanticAnalyzer {

  public ShowErasureGrantsAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    String principal = null;
    String policy = null;
    for (int i = 0; i < root.getChildCount(); i++) {
      ASTNode c = (ASTNode) root.getChild(i);
      if (c.getType() == HiveParser.TOK_GRANTS_FOR_PRINCIPAL && c.getChildCount() > 0) {
        principal = c.getChild(0).getText();
      } else if (c.getType() == HiveParser.TOK_GRANTS_FOR_POLICY && c.getChildCount() > 0) {
        policy = c.getChild(0).getText();
      }
    }

    // Inspection of the grant graph: permitted to the ERASURE_ADMIN role (its
    // oversight purpose) or to a POLICY_VALIDATE holder. Use the policy name if
    // one was given, otherwise wildcard scope. The operation appends the
    // configured ERASURE_ADMIN role-holders as synthetic rows (source=config),
    // so the fetch schema below carries the source column.
    try {
      PolicyPrivilegeUtils.requireGrantView(conf, Hive.get(conf),
          policy == null ? "*" : policy);
    } catch (org.apache.hadoop.hive.ql.metadata.HiveException he) {
      throw new SemanticException("SHOW ERASURE GRANTS: privilege check failed: " + he.getMessage());
    }

    ctx.setResFile(ctx.getLocalTmpPath());

    ShowErasureGrantsDesc desc = new ShowErasureGrantsDesc(ctx.getResFile(), principal, policy);
    Task<DDLWork> task = TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc));
    rootTasks.add(task);
    task.setFetchSource(true);
    setFetchTask(createFetchTask(ShowErasureGrantsDesc.SHOW_ERASURE_GRANTS_SCHEMA));
  }
}
