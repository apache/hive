/**
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
package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.calcite.util.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.ForwardWalker;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderOnceWalker;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.TypeRule;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc.ExprNodeDescEqualityWrapper;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStruct;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ImmutableSortedSet;
import com.google.common.collect.ListMultimap;

/**
 * This optimization will take a Filter expression, and if its predicate contains
 * an OR operator whose children are constant equality expressions, it will try
 * to generate an IN clause (which is more efficient). If the OR operator contains
 * AND operator children, the optimization might generate an IN clause that uses
 * structs.
 */
public class PointLookupOptimizer implements Transform {

  private static final Log LOG = LogFactory.getLog(PointLookupOptimizer.class);
  private static final String IN_UDF =
          GenericUDFIn.class.getAnnotation(Description.class).name();
  private static final String STRUCT_UDF =
          GenericUDFStruct.class.getAnnotation(Description.class).name();
  private static final String AND_UDF =
      GenericUDFOPAnd.class.getAnnotation(Description.class).name();

  // these are closure-bound for all the walkers in context
  public final int minOrExpr;
  public final boolean extract;
  public final boolean testMode;

  /*
   * Pass in configs and pre-create a parse context
   */
  public PointLookupOptimizer(final int min, final boolean extract, final boolean testMode) {
    this.minOrExpr = min;
    this.extract = extract;
    this.testMode = testMode;
  }

  // Hash Set iteration isn't ordered, but force string sorted order
  // to get a consistent test run.
  private Collection<ExprNodeDescEqualityWrapper> sortForTests(
      Set<ExprNodeDescEqualityWrapper> valuesExpr) {
    if (!testMode) {
      // normal case - sorting is wasted for an IN()
      return valuesExpr;
    }
    final Collection<ExprNodeDescEqualityWrapper> sortedValues;

    sortedValues = ImmutableSortedSet.copyOf(
        new Comparator<ExprNodeDescEqualityWrapper>() {
          @Override
          public int compare(ExprNodeDescEqualityWrapper w1,
              ExprNodeDescEqualityWrapper w2) {
            // fail if you find nulls (this is a test-code section)
            if (w1.equals(w2)) {
              return 0;
            }
            return w1.getExprNodeDesc().getExprString()
                .compareTo(w2.getExprNodeDesc().getExprString());
          }
        }, valuesExpr);

    return sortedValues;
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    // 1. Trigger transformation
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", FilterOperator.getOperatorName() + "%"), new FilterTransformer());

    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, null);
    GraphWalker ogw = new ForwardWalker(disp);

    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  private class FilterTransformer implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      FilterOperator filterOp = (FilterOperator) nd;
      ExprNodeDesc predicate = filterOp.getConf().getPredicate();

      // Generate the list bucketing pruning predicate
      ExprNodeDesc newPredicate = generateInClause(predicate);
      if (newPredicate != null) {
        // Replace filter in current FIL with new FIL
        if (LOG.isDebugEnabled()) {
          LOG.debug("Generated new predicate with IN clause: " + newPredicate);
        }
        if (!extract) {
          filterOp.getConf().setOrigPredicate(predicate);
        }
        filterOp.getConf().setPredicate(newPredicate);
      }

      return null;
    }

    private ExprNodeDesc generateInClause(ExprNodeDesc predicate) throws SemanticException {
      Map<Rule, NodeProcessor> exprRules = new LinkedHashMap<Rule, NodeProcessor>();
      exprRules.put(new TypeRule(ExprNodeGenericFuncDesc.class), new OrExprProcessor());

      // The dispatcher fires the processor corresponding to the closest matching
      // rule and passes the context along
      Dispatcher disp = new DefaultRuleDispatcher(null, exprRules, null);
      GraphWalker egw = new PreOrderOnceWalker(disp);

      List<Node> startNodes = new ArrayList<Node>();
      startNodes.add(predicate);

      HashMap<Node, Object> outputMap = new HashMap<Node, Object>();
      egw.startWalking(startNodes, outputMap);
      return (ExprNodeDesc) outputMap.get(predicate);
    }
  }

  private class OrExprProcessor implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprNodeGenericFuncDesc fd = (ExprNodeGenericFuncDesc) nd;

      // 1. If it is not an OR operator, we bail out.
      if (!FunctionRegistry.isOpOr(fd)) {
        return null;
      }

      // 2. It is an OR operator with enough children
      List<ExprNodeDesc> children = fd.getChildren();
      if (children.size() < minOrExpr) {
        return null;
      }
      ListMultimap<String,Pair<ExprNodeColumnDesc, ExprNodeConstantDesc>> columnConstantsMap =
              ArrayListMultimap.create();
      boolean modeAnd = false;
      for (int i = 0; i < children.size(); i++) {
        ExprNodeDesc child = children.get(i);

        // - If the child is an AND operator, extract its children
        // - Otherwise, take the child itself
        final List<ExprNodeDesc> conjunctions;
        if (FunctionRegistry.isOpAnd(child)) {
          // If it is the first child, we set the mode variable value
          // Otherwise, if the mode we are working on is different, we
          // bail out
          if (i == 0) {
            modeAnd = true;
          } else {
            if (!modeAnd) {
              return null;
            }
          }

          // Multiple children
          conjunctions = child.getChildren();
        } else {
          // If it is the first child, we set the mode variable value
          // Otherwise, if the mode we are working on is different, we
          // bail out
          if (i == 0) {
            modeAnd = false;
          } else {
            if (modeAnd) {
              return null;
            }
          }

          // One child
          conjunctions = new ArrayList<ExprNodeDesc>(1);
          conjunctions.add(child);
        }

        // 3. We will extract the literals to introduce in the IN clause.
        //    If the patterns OR-AND-EqOp or OR-EqOp are not matched, we bail out
        for (ExprNodeDesc conjunction: conjunctions) {
          if (!(conjunction instanceof ExprNodeGenericFuncDesc)) {
            return null;
          }

          ExprNodeGenericFuncDesc conjCall = (ExprNodeGenericFuncDesc) conjunction;
          Class<? extends GenericUDF> genericUdfClass = conjCall.getGenericUDF().getClass();
          if(GenericUDFOPEqual.class == genericUdfClass) {
            if (conjCall.getChildren().get(0) instanceof ExprNodeColumnDesc &&
                    conjCall.getChildren().get(1) instanceof ExprNodeConstantDesc) {
              ExprNodeColumnDesc ref = (ExprNodeColumnDesc) conjCall.getChildren().get(0);
              String refString = ref.toString();
              columnConstantsMap.put(refString,
                      new Pair<ExprNodeColumnDesc,ExprNodeConstantDesc>(
                              ref, (ExprNodeConstantDesc) conjCall.getChildren().get(1)));
              if (columnConstantsMap.get(refString).size() != i+1) {
                // If we have not added to this column desc before, we bail out
                return null;
              }
            } else if (conjCall.getChildren().get(1) instanceof ExprNodeColumnDesc &&
                    conjCall.getChildren().get(0) instanceof ExprNodeConstantDesc) {
              ExprNodeColumnDesc ref = (ExprNodeColumnDesc) conjCall.getChildren().get(1);
              String refString = ref.toString();
              columnConstantsMap.put(refString,
                      new Pair<ExprNodeColumnDesc,ExprNodeConstantDesc>(
                              ref, (ExprNodeConstantDesc) conjCall.getChildren().get(0)));
              if (columnConstantsMap.get(refString).size() != i+1) {
                // If we have not added to this column desc before, we bail out
                return null;
              }
            } else {
              // We bail out
              return null;
            }
          } else {
            // We bail out
            return null;
          }
        }
      }

      // 4. We build the new predicate and return it
      ExprNodeDesc newPredicate = null;
      List<ExprNodeDesc> newChildren = new ArrayList<ExprNodeDesc>(children.size());
      // 4.1 Create structs
      List<ExprNodeDesc> columns = new ArrayList<ExprNodeDesc>();
      List<String> names = new ArrayList<String>();
      List<TypeInfo> typeInfos = new ArrayList<TypeInfo>();
      for (int i = 0; i < children.size(); i++) {
        List<ExprNodeDesc> constantFields = new ArrayList<ExprNodeDesc>(children.size());

        for (String keyString : columnConstantsMap.keySet()) {
          Pair<ExprNodeColumnDesc, ExprNodeConstantDesc> columnConstant =
                  columnConstantsMap.get(keyString).get(i);
          if (i == 0) {
            columns.add(columnConstant.left);
            names.add(columnConstant.left.getColumn());
            typeInfos.add(columnConstant.left.getTypeInfo());
          }
          constantFields.add(columnConstant.right);
        }

        if (i == 0) {
          ExprNodeDesc columnsRefs;
          if (columns.size() == 1) {
            columnsRefs = columns.get(0);
          } else {
            columnsRefs = new ExprNodeGenericFuncDesc(
                    TypeInfoFactory.getStructTypeInfo(names, typeInfos),
                    FunctionRegistry.getFunctionInfo(STRUCT_UDF).getGenericUDF(),
                    columns);
          }
          newChildren.add(columnsRefs);
        }
        ExprNodeDesc values;
        if (constantFields.size() == 1) {
          values = constantFields.get(0);
        } else {
          values = new ExprNodeGenericFuncDesc(
                  TypeInfoFactory.getStructTypeInfo(names, typeInfos),
                  FunctionRegistry.getFunctionInfo(STRUCT_UDF).getGenericUDF(),
                  constantFields);
        }
        newChildren.add(values);
      }
      newPredicate = new ExprNodeGenericFuncDesc(TypeInfoFactory.booleanTypeInfo,
              FunctionRegistry.getFunctionInfo(IN_UDF).getGenericUDF(), newChildren);

      if (extract && columns.size() > 1) {
        final List<ExprNodeDesc> subExpr = new ArrayList<ExprNodeDesc>(columns.size()+1);

        // extract pre-conditions for the tuple expressions
        // (a,b) IN ((1,2),(2,3)) ->
        //          ((a) IN (1,2) and b in (2,3)) and (a,b) IN ((1,2),(2,3))

        for (String keyString : columnConstantsMap.keySet()) {
          final Set<ExprNodeDescEqualityWrapper> valuesExpr = 
              new HashSet<ExprNodeDescEqualityWrapper>(children.size());
          final List<Pair<ExprNodeColumnDesc, ExprNodeConstantDesc>> partial = 
              columnConstantsMap.get(keyString);
          for (int i = 0; i < children.size(); i++) {
            Pair<ExprNodeColumnDesc, ExprNodeConstantDesc> columnConstant = partial
                .get(i);
            valuesExpr
                .add(new ExprNodeDescEqualityWrapper(columnConstant.right));
          }
          ExprNodeColumnDesc lookupCol = partial.get(0).left;
          // generate a partial IN clause, if the column is a partition column
          if (lookupCol.getIsPartitionColOrVirtualCol()
              || valuesExpr.size() < children.size()) {
            // optimize only nDV reductions
            final List<ExprNodeDesc> inExpr = new ArrayList<ExprNodeDesc>();
            inExpr.add(lookupCol);
            for (ExprNodeDescEqualityWrapper value : sortForTests(valuesExpr)) {
              inExpr.add(value.getExprNodeDesc());
            }
            subExpr.add(new ExprNodeGenericFuncDesc(
                TypeInfoFactory.booleanTypeInfo, FunctionRegistry
                    .getFunctionInfo(IN_UDF).getGenericUDF(), inExpr));
          }
        }
        // loop complete, inspect the sub expressions generated
        if (subExpr.size() > 0) {
          // add the newPredicate to the end & produce an AND clause
          subExpr.add(newPredicate);
          newPredicate = new ExprNodeGenericFuncDesc(
              TypeInfoFactory.booleanTypeInfo, FunctionRegistry
                  .getFunctionInfo(AND_UDF).getGenericUDF(), subExpr);
        }
        // else, newPredicate is unmodified
      }

      return newPredicate;
    }

  }

}
