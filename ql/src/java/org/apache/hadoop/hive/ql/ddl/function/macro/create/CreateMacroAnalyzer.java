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

package org.apache.hadoop.hive.ql.ddl.function.macro.create;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.ddl.DDLWork;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

/**
 * Analyzer for macro creation commands.
 */
@DDLType(types = HiveParser.TOK_CREATEMACRO)
public class CreateMacroAnalyzer extends BaseSemanticAnalyzer {
  public CreateMacroAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {
    String macroName = root.getChild(0).getText();
    if (FunctionUtils.isQualifiedFunctionName(macroName)) {
      throw new SemanticException("Temporary macro cannot be created with a qualified name.");
    }

    List<FieldSchema> arguments = getColumns((ASTNode)root.getChild(1), true, conf);
    Set<String> actualColumnNames = getActualColumnNames(root, arguments);

    RowResolver rowResolver = new RowResolver();
    ArrayList<String> macroColumnNames = new ArrayList<String>(arguments.size());
    ArrayList<TypeInfo> macroColumnTypes = new ArrayList<TypeInfo>(arguments.size());

    getMacroColumnData(arguments, actualColumnNames, rowResolver, macroColumnNames, macroColumnTypes);
    ExprNodeDesc body = getBody(root, arguments, rowResolver);

    CreateMacroDesc desc = new CreateMacroDesc(macroName, macroColumnNames, macroColumnTypes, body);
    rootTasks.add(TaskFactory.get(new DDLWork(getInputs(), getOutputs(), desc)));

    Database database = getDatabase(Warehouse.DEFAULT_DATABASE_NAME);
    // This restricts macro creation to privileged users.
    outputs.add(new WriteEntity(database, WriteEntity.WriteType.DDL_NO_LOCK));
  }

  private Set<String> getActualColumnNames(ASTNode root, List<FieldSchema> arguments) throws SemanticException {
    final Set<String> actualColumnNames = new HashSet<String>();

    if (!arguments.isEmpty()) {
      // Walk down expression to see which arguments are actually used.
      Node expression = (Node) root.getChild(2);
      PreOrderWalker walker = new PreOrderWalker(new SemanticDispatcher() {
        @Override
        public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
          if (nd instanceof ASTNode) {
            ASTNode node = (ASTNode)nd;
            if (node.getType() == HiveParser.TOK_TABLE_OR_COL) {
              actualColumnNames.add(node.getChild(0).getText());
            }
          }
          return null;
        }
      });
      walker.startWalking(Collections.singletonList(expression), null);
    }

    return actualColumnNames;
  }

  private void getMacroColumnData(List<FieldSchema> arguments, Set<String> actualColumnNames, RowResolver rowResolver,
      ArrayList<String> macroColumnNames, ArrayList<TypeInfo> macroColumnTypes) throws SemanticException {
    for (FieldSchema argument : arguments) {
      TypeInfo columnType = TypeInfoUtils.getTypeInfoFromTypeString(argument.getType());
      rowResolver.put("", argument.getName(), new ColumnInfo(argument.getName(), columnType, "", false));
      macroColumnNames.add(argument.getName());
      macroColumnTypes.add(columnType);
    }
    Set<String> expectedColumnNames = new LinkedHashSet<String>(macroColumnNames);
    if (!expectedColumnNames.equals(actualColumnNames)) {
      throw new SemanticException("Expected columns " + expectedColumnNames + " but found " + actualColumnNames);
    }
    if (expectedColumnNames.size() != macroColumnNames.size()) {
      throw new SemanticException("At least one parameter name was used more than once " + macroColumnNames);
    }
  }

  private ExprNodeDesc getBody(ASTNode root, List<FieldSchema> arguments, RowResolver rowResolver)
      throws SemanticException {
    SemanticAnalyzer sa = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_ENABLED) ?
        new CalcitePlanner(queryState) : new SemanticAnalyzer(queryState);

    ExprNodeDesc body = arguments.isEmpty() ?
        sa.genExprNodeDesc((ASTNode)root.getChild(1), rowResolver) :
        sa.genExprNodeDesc((ASTNode)root.getChild(2), rowResolver);
    return body;
  }
}
