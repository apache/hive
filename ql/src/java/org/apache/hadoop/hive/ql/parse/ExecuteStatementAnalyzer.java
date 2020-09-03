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

import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.ddl.DDLSemanticAnalyzerFactory.DDLType;
import org.apache.hadoop.hive.ql.exec.ExplainTask;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezTask;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.parse.type.TypeCheckCtx;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprDynamicParamDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.parquet.Preconditions;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Analyzer for Execute statement.
 * This analyzer
 *  retrieves cached {@link BaseSemanticAnalyzer},
 *  makes copy of all tasks by serializing/deserializing it,
 *  bind dynamic parameters inside cached {@link BaseSemanticAnalyzer} using values provided
 */
@DDLType(types = HiveParser.TOK_EXECUTE)
public class ExecuteStatementAnalyzer extends BaseSemanticAnalyzer {

  public ExecuteStatementAnalyzer(QueryState queryState) throws SemanticException {
    super(queryState);
  }

  /**
   * This class encapsulate all {@link Task} required to be copied.
   * This is required because {@link FetchTask} list of {@link Task} may hold reference to same
   * objects (e.g. list of result files) and are required to be serialized/de-serialized together.
   */
  private class PlanCopy {
    FetchTask fetchTask;
    List<Task<?>> tasks;

    PlanCopy(FetchTask fetchTask, List<Task<?>> tasks) {
      this.fetchTask = fetchTask;
      this.tasks = tasks;
    }

    FetchTask getFetchTask() {
      return fetchTask;
    }

    List<Task<?>> getTasks()  {
      return tasks;
    }
  }

  private String getQueryName(ASTNode root) {
    ASTNode queryNameAST = (ASTNode)(root.getChild(1));
    return queryNameAST.getText();
  }

  /**
   * Utility method to create copy of provided object using kryo serialization/de-serialization.
   */
  private <T> T makeCopy(final Object task, Class<T> objClass) {
    ByteArrayOutputStream baos = new ByteArrayOutputStream();
    SerializationUtilities.serializePlan(task, baos);

    return SerializationUtilities.deserializePlan(
        new ByteArrayInputStream(baos.toByteArray()), objClass);
  }

  /**
   * Given a {@link BaseSemanticAnalyzer} (cached) this method make copies of all tasks
   * (including {@link FetchTask}) and update the existing {@link ExecuteStatementAnalyzer}
   */
  private void createTaskCopy(final BaseSemanticAnalyzer cachedPlan) {
    PlanCopy planCopy = new PlanCopy(cachedPlan.getFetchTask(), cachedPlan.getAllRootTasks());
    planCopy = makeCopy(planCopy, planCopy.getClass());
    this.setFetchTask(planCopy.getFetchTask());
    this.rootTasks = planCopy.getTasks();
  }

  private String getParamLiteralValue(Map<Integer, ASTNode> paramMap, int paramIndex) {
    Preconditions.checkArgument(paramMap.containsKey(paramIndex), "Index not found.");
    ASTNode node = paramMap.get(paramIndex);

    if (node.getType() == HiveParser.StringLiteral) {
      // remove quotes
      return BaseSemanticAnalyzer.unescapeSQLString(node.getText());

    } else {
      return node.getText();
    }
  }


  /**
   * This method creates a constant expression to replace the given dynamic expression.
   * @param dynamicExpr Expression node representing Dynamic expression
   * @param typeInfo Type info used to create constant expression from ASTNode
   * @param parameterMap Integer to AST node map
   */
  private ExprNodeConstantDesc getConstant(ExprDynamicParamDesc dynamicExpr, TypeInfo typeInfo,
      Map<Integer, ASTNode> parameterMap) throws SemanticException {
    Preconditions.checkArgument(parameterMap.containsKey(dynamicExpr.getIndex()),
        "Paramter index not found");

    ASTNode paramNode = parameterMap.get(dynamicExpr.getIndex());

    TypeCheckCtx typeCheckCtx = new TypeCheckCtx(null);
    ExprNodeDesc node = ExprNodeTypeCheck.genExprNode(paramNode, typeCheckCtx).get(paramNode);
    Preconditions.checkArgument(node instanceof ExprNodeConstantDesc,
        "Invalid expression created");
    return (ExprNodeConstantDesc)node;
  }

  /**
   * Given a list of expressions this method traverse the expression tree and replaces
   * all {@link ExprDynamicParamDesc} nodes with constant expression.
   * @param exprList
   * @param paramMap
   */
  private List<ExprNodeDesc> replaceDynamicParamsInExprList(List<ExprNodeDesc> exprList,
      Map<Integer, ASTNode> paramMap) throws SemanticException{
    List<ExprNodeDesc> updatedExprList = new ArrayList<>();
    for (ExprNodeDesc expr:exprList) {
      expr = replaceDynamicParamsWithConstant(expr, expr.getTypeInfo(), paramMap);
      updatedExprList.add(expr);
    }
    return updatedExprList;
  }

  /**
   * Given an expression tree root at expr and type info of the expression this method traverse
   * the expression tree and replaces all dynamic expression with the constant expression.
   * This method also does type inference for the new constant expression.
   * Note about type inference
   * Since dynamic parameter lacks type we need to figure out appropriate type to create constant
   * out of string value. To do this, we choose the type of first child of the parent expression
   * which isn't dynamic parameter
   */
  private ExprNodeDesc replaceDynamicParamsWithConstant(ExprNodeDesc expr, TypeInfo typeInfo,
      Map<Integer, ASTNode> paramMap) throws SemanticException{
    if (expr.getChildren() == null || expr.getChildren().isEmpty()) {
      if (expr instanceof ExprDynamicParamDesc) {
        return getConstant((ExprDynamicParamDesc)expr, typeInfo, paramMap);
      }
      return expr;
    }

    for(ExprNodeDesc child:expr.getChildren()) {
      // we need typeinfo
      if(child instanceof ExprDynamicParamDesc) {
        continue;
      } else if( child.getTypeInfo() != TypeInfoFactory.voidTypeInfo) {
        typeInfo = child.getTypeInfo();
        break;
      }
    }
    Preconditions.checkArgument(typeInfo != null, "TypeInfo is null");

    List<ExprNodeDesc> exprList = new ArrayList<>();
    for(ExprNodeDesc child: expr.getChildren()) {
      child = replaceDynamicParamsWithConstant(child, typeInfo, paramMap);
      exprList.add(child);
    }
    expr.getChildren().clear();
    expr.getChildren().addAll(exprList);
    return expr;
  }

  /**
   * Given map of index and ASTNode this traverse all operators within all tasks
   * including Fetch Task and all root tasks to find and replace all dynamic expressions
   */
  private void bindDynamicParams(Map<Integer, ASTNode> parameterMap) throws SemanticException{
    Preconditions.checkArgument(!parameterMap.isEmpty(), "Parameter map is empty");

    Set<Operator<?>> operators = new HashSet<>();
    if (this.getFetchTask() != null) {
      operators.addAll(OperatorUtils.getAllFetchOperators(this.getFetchTask()));
    }
    List<Task<?>> allTasks = this.getRootTasks();
    List<TezTask> rootTasks = Utilities.getTezTasks(this.getRootTasks());
    for(Task task:allTasks) {
      List<BaseWork> baseWorks = new ArrayList<>();
      if (task instanceof ExplainTask) {
        ExplainTask explainTask = (ExplainTask) task;
        for (Task explainRootTask : explainTask.getWork().getRootTasks()) {
          if (explainRootTask instanceof TezTask) {
            TezTask explainTezTask = (TezTask) explainRootTask;
            baseWorks.addAll(explainTezTask.getWork().getAllWork());
          }
        }
      } else if (task instanceof TezTask) {
        baseWorks = ((TezTask) task).getWork().getAllWork();
      }
      for (BaseWork baseWork : baseWorks) {
        operators.addAll(baseWork.getAllOperators());
      }
    }

    for (Operator<?> op : operators) {
      switch(op.getType()) {
      case FILTER:
        FilterOperator filterOp = (FilterOperator)op;
        ExprNodeDesc predicate = filterOp.getConf().getPredicate();
        filterOp.getConf().setPredicate(
            replaceDynamicParamsWithConstant(predicate, TypeInfoFactory.booleanTypeInfo, parameterMap));
        break;
      }
    }
  }

  @Override
  public void analyzeInternal(ASTNode root) throws SemanticException {

    SessionState ss = SessionState.get();
    Preconditions.checkNotNull(ss, "SessionState object must not be NULL");
    String queryName = getQueryName(root);
    if (ss.getPreparePlans().containsKey(queryName)) {
      // retrieve cached plan from session state
      BaseSemanticAnalyzer cachedPlan = ss.getPreparePlans().get(queryName);

      // make copy of the plan
      createTaskCopy(cachedPlan);

      // bind dynamic params
      Map<Integer, ASTNode> parameterMap = findParams(root);
      bindDynamicParams(parameterMap);

      // reset prepare flag
      this.prepareQuery = false;

      // reset config
      this.conf.syncFromConf(cachedPlan.getQueryState().getConf());

      // set rest of the params
      this.inputs = cachedPlan.getInputs();
      this.outputs = cachedPlan.getOutputs();

      //lineage info
      this.setLineageInfo(cachedPlan.getLineageInfo());
      this.setTableAccessInfo(cachedPlan.getTableAccessInfo());
      this.setColumnAccessInfo(cachedPlan.getColumnAccessInfo());


      this.idToTableNameMap = new HashMap<String, String>(cachedPlan.getIdToTableNameMap());

      this.queryProperties = cachedPlan.getQueryProperties();
      this.setAutoCommitValue(cachedPlan.getAutoCommitValue());
      this.transactionalInQuery = cachedPlan.hasTransactionalInQuery();
      this.acidFileSinks.addAll(cachedPlan.getAcidFileSinks());
      this.initCtx(cachedPlan.getCtx());
      this.ctx.setCboInfo(cachedPlan.getCboInfo());
    } else {
      throw new SemanticException("No existing plan found for the execute statement. "
          + "Please make sure to add one using prepare");
    }
  }

  /**
   * Given an AST root at EXECUTE node this method parses the tree to build a map
   * with key as monotonically increasing integer and value as the node representing value
   * of the parameter
   */
  private Map<Integer, ASTNode> findParams(ASTNode executeRoot) {
    Preconditions.checkArgument(executeRoot.getType() == HiveParser.TOK_EXECUTE,
        "Unexpected ASTNode type");
    ASTNode executeParamList = (ASTNode)executeRoot.getChildren().get(0);
    Preconditions.checkArgument(executeParamList.getType() == HiveParser.TOK_EXECUTE_PARAM_LIST,
        "Unexpected execute parameter type");

    Map<Integer, ASTNode> paramMap = new HashMap<>();

    int idx=0;
    for (int i=0; i<executeParamList.getChildCount(); i++) {
      ASTNode param = (ASTNode)executeParamList.getChild(i);
      paramMap.put(++idx, param);
    }
    return paramMap;
  }
}
