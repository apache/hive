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

package org.apache.hadoop.hive.ql;

import java.io.*;
import java.util.*;

import org.apache.hadoop.hive.common.type.*;
import org.apache.hadoop.hive.conf.*;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.exec.repl.util.ReplUtils;
import org.apache.hadoop.hive.ql.exec.tez.*;
import org.apache.hadoop.hive.ql.exec.vector.*;
import org.apache.hadoop.hive.ql.hooks.HookUtils;
import org.apache.hadoop.hive.ql.io.AcidUtils;
import org.apache.hadoop.hive.ql.lockmgr.HiveTxnManager;
import org.apache.hadoop.hive.ql.lockmgr.LockException;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.AuthorizationException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.physical.*;
import org.apache.hadoop.hive.ql.parse.*;
import org.apache.hadoop.hive.ql.parse.type.*;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.security.authorization.command.CommandAuthorizer;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.ql.session.SessionState.LogHelper;
import org.apache.hadoop.hive.ql.udf.generic.*;
import org.apache.hadoop.hive.serde.*;
import org.apache.hadoop.hive.serde2.typeinfo.*;
import org.apache.hadoop.util.StringUtils;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableMap;

/**
 * The compiler compiles the command, by creating a QueryPlan from a String command.
 * Also opens a transaction if necessary.
 */
public class PreparePlanUtils {

  private static final Logger LOG = LoggerFactory.getLogger(PreparePlanUtils.class);

  private static Set<Operator<?>> getAllFetchOperators(FetchTask task) {
    if (task.getWork().getSource() == null)  {
      return Collections.EMPTY_SET;
    }
    Set<Operator<?>> operatorList =  new HashSet<>();
    operatorList.add(task.getWork().getSource());
    return AnnotateRunTimeStatsOptimizer.getAllOperatorsForSimpleFetch(operatorList);
  }

  /*
   * Retrieve name for PREPARE/EXECUTE statement
   * Make sure the tree is either EXECUTE or PREPARE
   */
  protected static String getPrepareStatementName(ASTNode tree) {
    if (tree.getType() == HiveParser.TOK_EXPLAIN) {
      tree = (ASTNode)tree.getChildren().get(0);
    }
    assert (tree.getType() == HiveParser.TOK_PREPARE
        || tree.getType() == HiveParser.TOK_EXECUTE);
    return ((ASTNode)tree.getChildren().get(1)).getText();
  }

  public static void bindDynamicParams(QueryPlan plan, Map<Integer, String> parameterMap) {
    if (parameterMap == null ||  parameterMap.isEmpty()) {
      //TODO: LOG
       return;
    }
    Set<Operator<?>> operators = new HashSet<>();
    if (plan.getFetchTask() != null) {
      operators.addAll(getAllFetchOperators(plan.getFetchTask()));
    }
    List<Task<?>> allTasks = plan.getRootTasks();
    List<TezTask> rootTasks = Utilities.getTezTasks(plan.getRootTasks());
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
      case SELECT:
        if (op instanceof VectorSelectOperator) {
          VectorSelectOperator selectOperator = (VectorSelectOperator) op;
          List<ExprNodeDesc> selectExprList = selectOperator.getConf().getColList();
          if (selectExprList != null) {
            selectOperator.getConf().setColList(replaceDynamicParamsInExprList(selectExprList, parameterMap));
          }
        } else {
          SelectOperator selectOperator = (SelectOperator)op;
          List<ExprNodeDesc> selectExprList = selectOperator.getConf().getColList();
          if (selectExprList != null) {
            selectOperator.getConf().setColList(replaceDynamicParamsInExprList(selectExprList, parameterMap));
          }
        }
        break;
      default:
        //exception
      }
    }
  }

  private static List<ExprNodeDesc> replaceDynamicParamsInExprList(List<ExprNodeDesc> exprList,
      Map<Integer, String> paramMap) {
    List<ExprNodeDesc> updatedExprList = new ArrayList<>();
    for (ExprNodeDesc expr:exprList) {
      expr = replaceDynamicParamsWithConstant(expr, expr.getTypeInfo(), paramMap);
      updatedExprList.add(expr);
    }
    return updatedExprList;
  }

  // Note about type inference
  // Since dynamic parameter lacks type we need to figure out appropriate type to create constant
  // out of string value. To do this, we choose the type of first child of the parent expression
  // which isn't dynamic parameter
  // TODO: cases to consider/cover
  //  exprs have no children, expres have all dynamic parameters
  private static ExprNodeDesc replaceDynamicParamsWithConstant(ExprNodeDesc expr,
      TypeInfo typeInfo, Map<Integer, String> paramMap) {
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
      } else {
        typeInfo = child.getTypeInfo();
        break;
      }
    }
    //TODO: this could be null in case expr doesn't have any child
    assert(typeInfo != null);

    List<ExprNodeDesc> exprList = new ArrayList<>();
    for(ExprNodeDesc child: expr.getChildren()) {
      if(child instanceof ExprDynamicParamDesc) {
        child = getConstant((ExprDynamicParamDesc)child, typeInfo, paramMap);
      } else {
        child = replaceDynamicParamsWithConstant(child, typeInfo, paramMap);
      }
      exprList.add(child);
    }
    expr.getChildren().clear();
    expr.getChildren().addAll(exprList);
    return expr;
  }

  private static ExprNodeConstantDesc getConstant(ExprDynamicParamDesc dynamicExpr,
      TypeInfo typeInfo, Map<Integer, String> parameterMap) {
    assert(parameterMap.containsKey(dynamicExpr.getIndex()));
    String value = parameterMap.get(dynamicExpr.getIndex());

    //TODO: probably should create single instance and reuse it
    ExprNodeDescExprFactory factory = new ExprNodeDescExprFactory();
    if (typeInfo.equals(TypeInfoFactory.booleanTypeInfo)) {
      return factory.createBooleanConstantExpr(value);
    } else if (typeInfo.equals(TypeInfoFactory.intTypeInfo)) {
      return factory.createIntConstantExpr(value);
    } else if (typeInfo.equals(TypeInfoFactory.longTypeInfo)) {
      return factory.createBigintConstantExpr(value);
    } else if (typeInfo.equals(TypeInfoFactory.stringTypeInfo)) {
      return factory.createStringConstantExpr(value);
    } else if (typeInfo.equals(TypeInfoFactory.charTypeInfo)) {
      //TODO
      assert(false);
    } else if (typeInfo.equals(TypeInfoFactory.varcharTypeInfo)) {
      //TODO
      assert(false);
    } else if (typeInfo.equals(TypeInfoFactory.floatTypeInfo)) {
      return factory.createFloatConstantExpr(value);
    } else if (typeInfo.equals(TypeInfoFactory.doubleTypeInfo)) {
      return factory.createDoubleConstantExpr(value);
    } else if (typeInfo.equals(TypeInfoFactory.byteTypeInfo)) {
      //TODO
      assert(false);
    } else if (typeInfo.equals(TypeInfoFactory.shortTypeInfo)) {
      return factory.createSmallintConstantExpr(value);
    } else if (typeInfo.equals(TypeInfoFactory.dateTypeInfo)) {
      return factory.createDateConstantExpr(value);
    } else if (typeInfo.equals(TypeInfoFactory.timestampTypeInfo)) {
      return factory.createTimestampConstantExpr(value);
    } else if (typeInfo.equals(TypeInfoFactory.intervalYearMonthTypeInfo)) {
      return factory.createIntervalYearMonthConstantExpr(value);
    } else if (typeInfo.equals(TypeInfoFactory.intervalDayTimeTypeInfo)) {
      return factory.createIntervalDayTimeConstantExpr(value);
    } else if (typeInfo.equals(TypeInfoFactory.binaryTypeInfo)) {
      //TODO
      assert(false);
    }
    // we will let constant expression itself infer the type
    return new ExprNodeConstantDesc(parameterMap.get(dynamicExpr.getIndex()));
  }

  public static QueryPlan makeCopy(final QueryPlan queryPlan) {
      QueryPlan newPlan = null;
      try {
        //Serialization of query plan
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        SerializationUtilities.serializePlan(queryPlan, baos);

        newPlan = SerializationUtilities.deserializePlan(
            new ByteArrayInputStream(baos.toByteArray()),
            queryPlan.getClass());

      } catch (Exception e) {
        LOG.info("during deep copy of query plan: " + e.getMessage());
      }

      return newPlan;
    //Verify that object is not corrupt

      //validateNameParts(fName);
      //validateNameParts(lName);

    }
}
