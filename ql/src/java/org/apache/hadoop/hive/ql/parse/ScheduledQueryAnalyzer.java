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

package org.apache.hadoop.hive.ql.parse;

import static org.apache.hadoop.hive.ql.parse.HiveParser.TOK_IFEXISTS;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Set;
import java.util.Stack;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.ScheduledQuery;
import org.apache.hadoop.hive.metastore.api.ScheduledQueryKey;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.FunctionUtils;
import org.apache.hadoop.hive.ql.exec.TaskFactory;
import org.apache.hadoop.hive.ql.hooks.WriteEntity;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.PreOrderWalker;
import org.apache.hadoop.hive.ql.plan.CreateMacroDesc;
import org.apache.hadoop.hive.ql.plan.DropMacroDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FunctionWork;
import org.apache.hadoop.hive.ql.schq.ScheduledQueryMaintWork;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ScheduledQueryAnalyzer extends BaseSemanticAnalyzer {
  private static final Logger LOG = LoggerFactory.getLogger(ScheduledQueryAnalyzer.class);
  private UnparseTranslator unparseTranslator;

  public ScheduledQueryAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
    this.unparseTranslator = new UnparseTranslator(conf);
  }

  @Override
  public void analyzeInternal(ASTNode ast) throws SemanticException {
    ScheduledQueryMaintWork work;
    switch (ast.getToken().getType()) {
    case HiveParser.TOK_CREATE_SCHEDULED_QUERY:
    case HiveParser.TOK_ALTER_SCHEDULED_QUERY:
    case HiveParser.TOK_DROP_SCHEDULED_QUERY:
      ScheduledQuery schq = interpretAstNode(ast);

      work = new ScheduledQueryMaintWork(ast.getToken().getType(), schq);
      rootTasks.add(TaskFactory.get(work));

      //      analyzeCreateScheduledQuery(ast);
      break;
    default:
      throw new SemanticException("Can't handle: " + ast.getToken().getType());
    }
  }

  private void analyzeCreateScheduledQuery(ASTNode ast) throws SemanticException {
    interpretAstNode(ast);
  }

  private ScheduledQuery interpretAstNode(ASTNode ast) throws SemanticException {
    // child0 is the schedule name
    String scheduleName = ast.getChild(0).getText();
    // FIXME: make this a hiveconf key
    String clusterNamespace = "default";
    ScheduledQueryKey key = new ScheduledQueryKey(scheduleName, clusterNamespace);
    ScheduledQuery ret = new ScheduledQuery(key);

    // child 1..n are arguments/options/etc
    for (int i = 1; i < ast.getChildCount(); i++) {
      //      processors.get(ast.getType())
      processSS(ret, (ASTNode) ast.getChild(i));
    }
    return ret;
  }

  private void processSS(ScheduledQuery schq, ASTNode child) throws SemanticException {
    switch (child.getType()) {
    case HiveParser.TOK_ENABLE:
      schq.setEnabled(true);
      return;
    case HiveParser.TOK_DISABLE:
      schq.setEnabled(false);
      return;
    case HiveParser.TOK_CRON:
      schq.setSchedule(child.getText());
      return;
    case HiveParser.TOK_EXECUTED_AS:
      schq.setUser(schq.getUser());
      return;
    case HiveParser.TOK_QUERY:
      schq.setQuery(unparseQuery(child.getChild(0)));
      return;
    default:
      throw new SemanticException("Unexpected token: " + child.getType());
    }
  }

  private String unparseQuery(Tree child) throws SemanticException {
    Tree viewSelect = child;
    //    String originalText =
    //        ctx.getTokenRewriteStream().toString(viewSelect.getTokenStartIndex(), viewSelect.getTokenStopIndex());
    //    String origText = originalText;

    ASTNode input = (ASTNode) child;
    BaseSemanticAnalyzer sem = SemanticAnalyzerFactory.get(queryState, input);
    sem.analyze(input, ctx);
    sem.validate();

    // Now expand the view definition with extras such as explicit column
    // references; this expanded form is what we'll re-parse when the view is
    // referenced later.
    unparseTranslator.applyTranslations(ctx.getTokenRewriteStream());
    String expandedText = ctx.getTokenRewriteStream().toString(input.getTokenStartIndex(), input.getTokenStopIndex());

    return expandedText;
  }

  //FIXME: del
  @SuppressWarnings("unchecked")
  private void analyzeCreateMacro(ASTNode ast) throws SemanticException {
    String functionName = ast.getChild(0).getText();

    // Temp macros are not allowed to have qualified names.
    if (FunctionUtils.isQualifiedFunctionName(functionName)) {
      throw new SemanticException("Temporary macro cannot be created with a qualified name.");
    }

    List<FieldSchema> arguments = BaseSemanticAnalyzer.getColumns((ASTNode) ast.getChild(1), true, conf);
    boolean isNoArgumentMacro = arguments.size() == 0;
    RowResolver rowResolver = new RowResolver();
    ArrayList<String> macroColNames = new ArrayList<String>(arguments.size());
    ArrayList<TypeInfo> macroColTypes = new ArrayList<TypeInfo>(arguments.size());
    final Set<String> actualColumnNames = new HashSet<String>();

    if (!isNoArgumentMacro) {
      /*
       * Walk down expression to see which arguments are actually used.
       */
      Node expression = (Node) ast.getChild(2);
      PreOrderWalker walker = new PreOrderWalker(new Dispatcher() {
        @Override
        public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs) throws SemanticException {
          if (nd instanceof ASTNode) {
            ASTNode node = (ASTNode) nd;
            if (node.getType() == HiveParser.TOK_TABLE_OR_COL) {
              actualColumnNames.add(node.getChild(0).getText());
            }
          }
          return null;
        }
      });
      walker.startWalking(Collections.singletonList(expression), null);
    }
    for (FieldSchema argument : arguments) {
      TypeInfo colType = TypeInfoUtils.getTypeInfoFromTypeString(argument.getType());
      rowResolver.put("", argument.getName(), new ColumnInfo(argument.getName(), colType, "", false));
      macroColNames.add(argument.getName());
      macroColTypes.add(colType);
    }
    Set<String> expectedColumnNames = new LinkedHashSet<String>(macroColNames);
    if (!expectedColumnNames.equals(actualColumnNames)) {
      throw new SemanticException("Expected columns " + expectedColumnNames + " but found " + actualColumnNames);
    }
    if (expectedColumnNames.size() != macroColNames.size()) {
      throw new SemanticException("At least one parameter name was used more than once " + macroColNames);
    }
    SemanticAnalyzer sa = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_CBO_ENABLED) ? new CalcitePlanner(queryState)
      : new SemanticAnalyzer(queryState);
    ;
    ExprNodeDesc body;
    if (isNoArgumentMacro) {
      body = sa.genExprNodeDesc((ASTNode) ast.getChild(1), rowResolver);
    } else {
      body = sa.genExprNodeDesc((ASTNode) ast.getChild(2), rowResolver);
    }
    CreateMacroDesc desc = new CreateMacroDesc(functionName, macroColNames, macroColTypes, body);
    rootTasks.add(TaskFactory.get(new FunctionWork(desc)));

    addEntities();
  }

  //FIXME: del
  @SuppressWarnings("unchecked")
  private void analyzeDropMacro(ASTNode ast) throws SemanticException {
    String functionName = ast.getChild(0).getText();
    boolean ifExists = (ast.getFirstChildWithType(TOK_IFEXISTS) != null);
    // we want to signal an error if the function doesn't exist and we're
    // configured not to ignore this
    boolean throwException = !ifExists && !HiveConf.getBoolVar(conf, ConfVars.DROPIGNORESNONEXISTENT);

    // Temp macros are not allowed to have qualified names.
    if (FunctionUtils.isQualifiedFunctionName(functionName)) {
      throw new SemanticException("Temporary macro name cannot be a qualified name.");
    }

    if (throwException && FunctionRegistry.getFunctionInfo(functionName) == null) {
      throw new SemanticException(ErrorMsg.INVALID_FUNCTION.getMsg(functionName));
    }

    DropMacroDesc desc = new DropMacroDesc(functionName);
    rootTasks.add(TaskFactory.get(new FunctionWork(desc)));

    addEntities();
  }

  //FIXME: del?
  private void addEntities() throws SemanticException {
    Database database = getDatabase(Warehouse.DEFAULT_DATABASE_NAME);
    // This restricts macro creation to privileged users.
    outputs.add(new WriteEntity(database, WriteEntity.WriteType.DDL_NO_LOCK));
  }
}
