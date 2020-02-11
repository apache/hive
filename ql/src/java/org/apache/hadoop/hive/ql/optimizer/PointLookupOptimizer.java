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
package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.calcite.util.Pair;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.SemanticDispatcher;
import org.apache.hadoop.hive.ql.lib.ForwardWalker;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.PreOrderOnceWalker;
import org.apache.hadoop.hive.ql.lib.SemanticRule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.lib.TypeRule;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFStruct;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.ListMultimap;

/**
 * This optimization will take a Filter expression, and if its predicate contains
 * an OR operator whose children are constant equality expressions, it will try
 * to generate an IN clause (which is more efficient). If the OR operator contains
 * AND operator children, the optimization might generate an IN clause that uses
 * structs.
 */
public class PointLookupOptimizer extends Transform {

  private static final Logger LOG = LoggerFactory.getLogger(PointLookupOptimizer.class);
  private static final String IN_UDF =
          GenericUDFIn.class.getAnnotation(Description.class).name();
  private static final String STRUCT_UDF =
          GenericUDFStruct.class.getAnnotation(Description.class).name();
  // these are closure-bound for all the walkers in context
  public final int minOrExpr;

  /*
   * Pass in configs and pre-create a parse context
   */
  public PointLookupOptimizer(final int min) {
    this.minOrExpr = min;
  }

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    // 1. Trigger transformation
    Map<SemanticRule, SemanticNodeProcessor> opRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    opRules.put(new RuleRegExp("R1", FilterOperator.getOperatorName() + "%"), new FilterTransformer());

    SemanticDispatcher disp = new DefaultRuleDispatcher(null, opRules, null);
    SemanticGraphWalker ogw = new ForwardWalker(disp);

    List<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  private class FilterTransformer implements SemanticNodeProcessor {

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
        filterOp.getConf().setPredicate(newPredicate);
      }

      return null;
    }

    private ExprNodeDesc generateInClause(ExprNodeDesc predicate) throws SemanticException {
      Map<SemanticRule, SemanticNodeProcessor> exprRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
      exprRules.put(new TypeRule(ExprNodeGenericFuncDesc.class), new OrExprProcessor());

      // The dispatcher fires the processor corresponding to the closest matching
      // rule and passes the context along
      SemanticDispatcher disp = new DefaultRuleDispatcher(null, exprRules, null);
      SemanticGraphWalker egw = new PreOrderOnceWalker(disp);

      List<Node> startNodes = new ArrayList<Node>();
      startNodes.add(predicate);

      HashMap<Node, Object> outputMap = new HashMap<Node, Object>();
      egw.startWalking(startNodes, outputMap);
      return (ExprNodeDesc) outputMap.get(predicate);
    }
  }

  private class OrExprProcessor implements SemanticNodeProcessor {

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

      return newPredicate;
    }

  }

}
