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

package org.apache.hadoop.hive.ql.metadata;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.DDLTask;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.CreateTableDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;

public class DummySemanticAnalyzerHook1 extends AbstractSemanticAnalyzerHook {
  static int count = 0;
  int myCount = -1;
  boolean isCreateTable;

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
      throws SemanticException {
    LogHelper console = SessionState.getConsole();
    isCreateTable = (ast.getToken().getType() == HiveParser.TOK_CREATETABLE);
    myCount = count++;
    if (isCreateTable) {
      console.printError("DummySemanticAnalyzerHook1 Pre: Count " + myCount);
    }
    return ast;
  }

  public DummySemanticAnalyzerHook1() {
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
      List<Task<? extends Serializable>> rootTasks) throws SemanticException {
    count = 0;
    if (!isCreateTable) {
      return;
    }

    CreateTableDesc desc = ((DDLTask) rootTasks.get(rootTasks.size() - 1)).getWork()
        .getCreateTblDesc();
    Map<String, String> tblProps = desc.getTblProps();
    if (tblProps == null) {
      tblProps = new HashMap<String, String>();
    }
    tblProps.put("createdBy", DummyCreateTableHook.class.getName());
    tblProps.put("Message", "Hive rocks!! Count: " + myCount);

    LogHelper console = SessionState.getConsole();
    console.printError("DummySemanticAnalyzerHook1 Post: Hive rocks!! Count: " + myCount);
  }
}
