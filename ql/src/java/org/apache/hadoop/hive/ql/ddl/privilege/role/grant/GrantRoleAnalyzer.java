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

package org.apache.hadoop.hive.ql.ddl.privilege.role.grant;

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.privilege.AbstractPrivilegeAnalyzer;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

import com.google.common.annotations.VisibleForTesting;

/**
 * Analyzer for granting to role commands.
 */
@DDLType(type=HiveParser.TOK_GRANT_ROLE)
public class GrantRoleAnalyzer extends AbstractPrivilegeAnalyzer {
  public GrantRoleAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @VisibleForTesting
  public GrantRoleAnalyzer(QueryState queryState, Hive db) throws SemanticException {
    super(queryState, db);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    Task<?> task = hiveAuthorizationTaskFactory.createGrantRoleTask(root, getInputs(), getOutputs());
    if (task != null) {
      rootTasks.add(task);
    }
  }
}
