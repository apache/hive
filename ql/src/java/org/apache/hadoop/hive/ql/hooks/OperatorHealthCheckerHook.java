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

package org.apache.hadoop.hive.ql.hooks;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.Stack;

import com.google.common.collect.Lists;

import com.google.common.collect.Sets;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.ScriptOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.apache.hadoop.hive.ql.plan.TezWork;

/**
 * Checks some operator quality rules.
 *
 * Checks whenever operator ids are not reused.
 * Checks some level of expression/schema consistency
 * Some sanity checks on SelectOperators
 */
public class OperatorHealthCheckerHook implements ExecuteWithHookContext {

  static class OperatorHealthCheckerProcessor implements SemanticNodeProcessor {

    Map<String, Operator<?>> opMap = new HashMap<>();

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {

      Operator<?> op = (Operator<?>) nd;
      checkOperator(op);
      String opKey = op.getOperatorId();
      if (opMap.containsKey(opKey)) {
        throw new RuntimeException("operator id reuse found: " + opKey);
      }
      opMap.put(opKey, op);
      return null;
    }
  }

  public static void checkOperator(Operator<?> op) {
    OperatorDesc conf = op.getConf();
    Map<String, ExprNodeDesc> exprMap = conf.getColumnExprMap();
    RowSchema schema = op.getSchema();

    checkSchema(schema);
    if (op instanceof SelectOperator) {
      checkSelectOperator((SelectOperator) op);
    }
    if (schema != null && exprMap != null) {
      for (Entry<String, ExprNodeDesc> c : exprMap.entrySet()) {
        if (c.getValue() instanceof ExprNodeConstantDesc) {
          continue;
        }
        ColumnInfo ci = schema.getColumnInfo(c.getKey());
        if (c.getKey().startsWith(Utilities.ReduceField.KEY + ".reducesinkkey")) {
          continue;
        }
        if (ci == null && conf.getComputedFields().contains(c.getKey())) {
          continue;
        }
        if (ci == null) {
          throw new RuntimeException("schema not found for " + c + " in " + schema);
        }
      }
      for (ColumnInfo sig : schema.getSignature()) {
        if (op instanceof ScriptOperator) {
          continue;
        }
        String iName = sig.getInternalName();
        ExprNodeDesc e = exprMap.get(iName);
        if (isSemiJoinRS(op)) {
          continue;
        }
        if (op.getConf() instanceof GroupByDesc) {
          continue;
        }

        if (e == null) {
          throw new RuntimeException("expr not found for " + iName + " in " + exprMap);
        }
      }
    }

  }

  private static void checkSchema(RowSchema schema) {
    if (schema != null) {
    List<String> cn = schema.getColumnNames();
    Set<String> cn2 = new HashSet<>(cn);
    if (cn.size() != cn2.size()) {
        throw new RuntimeException("ambiguous columns in the schema!");
    }
    }
  }

  private static void checkSelectOperator(SelectOperator op) {
    SelectDesc conf = op.getConf();
    RowSchema schema = op.getSchema();
    if (schema == null) {
      throw new RuntimeException("I expect a schema for all SelectOp" + op);
    }

    Set<String> cn = new HashSet<>(schema.getColumnNames());
    Set<String> ocn = new HashSet<>(conf.getOutputColumnNames());
    Set<String> diff = Sets.symmetricDifference(cn, ocn);
    if (!diff.isEmpty()) {
      throw new RuntimeException("SelOp output/schema mismatch ");
    }

  }

  private static boolean isSemiJoinRS(Operator<?> op) {
    if (op instanceof ReduceSinkOperator) {
      List<Operator<?>> children = op.getChildOperators();
      for (Operator<?> c : children) {
        if (!(c instanceof TableScanOperator)) {
          return false;
        }
      }
      return true;
    }
    return false;

  }

  public static void runCheck(ParseContext pctx) throws SemanticException {
    if (OperatorHealthCheckerHook.class.desiredAssertionStatus()) {
      OperatorHealthCheckerHook h = new OperatorHealthCheckerHook();
      Collection<Node> rootOps = new HashSet<>();
      rootOps.addAll(pctx.getTopOps().values());
      h.walkTree(rootOps);
    }
  }

  @Override
  public void run(HookContext hookContext) throws Exception {

    List<Node> rootOps = Lists.newArrayList();

    List<Task<?>> roots = hookContext.getQueryPlan().getRootTasks();
    for (Task<?> task : roots) {

      Object work = task.getWork();
      if (work instanceof MapredWork) {
        MapredWork mapredWork = (MapredWork) work;
        MapWork mapWork = mapredWork.getMapWork();
        if (mapWork != null) {
          rootOps.addAll(mapWork.getAllRootOperators());
        }
        ReduceWork reduceWork = mapredWork.getReduceWork();
        if (reduceWork != null) {
          rootOps.addAll(reduceWork.getAllRootOperators());
        }
      }
      if (work instanceof TezWork) {
        for (BaseWork bw : ((TezWork) work).getAllWorkUnsorted()) {
          rootOps.addAll(bw.getAllRootOperators());
        }
      }
    }
    walkTree(rootOps);
  }

  private void walkTree(Collection<Node> rootOps) throws SemanticException {
    if (rootOps.isEmpty()) {
      return;
    }

    SemanticDispatcher disp = new DefaultRuleDispatcher(new OperatorHealthCheckerProcessor(), new HashMap<>(), null);
    SemanticGraphWalker ogw = new DefaultGraphWalker(disp);

    HashMap<Node, Object> nodeOutput = new HashMap<Node, Object>();
    ogw.startWalking(rootOps, nodeOutput);
  }
}
