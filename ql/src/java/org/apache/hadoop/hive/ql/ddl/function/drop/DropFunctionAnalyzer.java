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

package org.apache.hadoop.hive.ql.ddl.function.drop;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.function.AbstractFunctionAnalyzer;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Analyzer for function dropping commands.
 */
@DDLType(types = HiveParser.TOK_DROPFUNCTION)
public class DropFunctionAnalyzer extends AbstractFunctionAnalyzer {
  public DropFunctionAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    String functionName = root.getChild(0).getText();
    boolean ifExists = (root.getFirstChildWithType(HiveParser.TOK_IFEXISTS) != null);
    boolean throwException = !ifExists && !HiveConf.getBoolVar(conf, ConfVars.DROP_IGNORES_NON_EXISTENT);
    boolean isTemporary = (root.getFirstChildWithType(HiveParser.TOK_TEMPORARY) != null);

    FunctionInfo info = FunctionRegistry.getFunctionInfo(functionName);
    if (info == null) {
      if (throwException) {
        throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(functionName));
      } else {
        return; // Fail silently
      }
    } else if (info.isBuiltIn()) {
      throw new SemanticException(ErrorMsg.DROP_NATIVE_FUNCTION.getMsg(functionName));
    }

    DropFunctionDesc desc = new DropFunctionDesc(functionName, isTemporary, null);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));

    addEntities(functionName, info.getClassName(), isTemporary, null);
  }
}
