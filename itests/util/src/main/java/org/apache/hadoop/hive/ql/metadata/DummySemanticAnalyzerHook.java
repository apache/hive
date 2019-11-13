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

import org.apache.hadoop.hive.ql.ddl.DDLTask;
import org.apache.hadoop.hive.ql.ddl.table.create.CreateTableDesc;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.AbstractSemanticAnalyzerHook;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.HiveSemanticAnalyzerHookContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class DummySemanticAnalyzerHook extends AbstractSemanticAnalyzerHook{

  private AbstractSemanticAnalyzerHook hook;

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
  throws SemanticException {

    switch (ast.getToken().getType()) {

    case HiveParser.TOK_CREATETABLE:
      hook = new DummyCreateTableHook();
      return hook.preAnalyze(context, ast);

    case HiveParser.TOK_DROPTABLE:
    case HiveParser.TOK_DESCTABLE:
      return ast;

    default:
      throw new SemanticException("Operation not supported.");
    }
  }

  public DummySemanticAnalyzerHook() {

  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
      List<Task<?>> rootTasks) throws SemanticException {

    if(hook != null) {
      hook.postAnalyze(context, rootTasks);
    }
  }
}

class DummyCreateTableHook extends AbstractSemanticAnalyzerHook{

  @Override
  public ASTNode preAnalyze(HiveSemanticAnalyzerHookContext context, ASTNode ast)
  throws SemanticException {

    int numCh = ast.getChildCount();

    for (int num = 1; num < numCh; num++) {
      ASTNode child = (ASTNode) ast.getChild(num);

      switch (child.getToken().getType()) {

      case HiveParser.TOK_QUERY:
        throw new SemanticException("CTAS not supported.");
      }
    }
    return ast;
  }

  @Override
  public void postAnalyze(HiveSemanticAnalyzerHookContext context,
      List<Task<?>> rootTasks) throws SemanticException {
    CreateTableDesc desc = (CreateTableDesc) ((DDLTask)rootTasks.get(rootTasks.size()-1)).getWork().getDDLDesc();
    Map<String,String> tblProps = desc.getTblProps();
    if(tblProps == null) {
      tblProps = new HashMap<String, String>();
    }
    tblProps.put("createdBy", DummyCreateTableHook.class.getName());
    tblProps.put("Message", "Open Source rocks!!");
    desc.setTblProps(tblProps);
  }
}
