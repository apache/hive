/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements. See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership. The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License. You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.calcite.util.Pair;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDynamicListDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;


/**
 * Takes a Filter operator on top of a TableScan and removes dynamic pruning conditions
 * if static partition pruning has been triggered already.
 * 
 * This transformation is executed when CBO is on and hence we can guarantee that the filtering
 * conditions on the partition columns will be immediately on top of the TableScan operator.
 *
 */
public class RedundantDynamicPruningConditionsRemoval extends Transform {

  private static final Logger LOG = LoggerFactory.getLogger(RedundantDynamicPruningConditionsRemoval.class);


  /**
   * Transform the query tree.
   *
   * @param pctx the current parse context
   */
  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    // Make sure semijoin is not enabled. If it is, then do not remove the dynamic partition pruning predicates.
    if (!pctx.getConf().getBoolVar(HiveConf.ConfVars.TEZ_DYNAMIC_SEMIJOIN_REDUCTION)) {
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      opRules.put(new RuleRegExp("R1", TableScanOperator.getOperatorName() + "%" +
              FilterOperator.getOperatorName() + "%"), new FilterTransformer());

      Dispatcher disp = new DefaultRuleDispatcher(null, opRules, null);
      GraphWalker ogw = new DefaultGraphWalker(disp);

      List<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(pctx.getTopOps().values());
      ogw.startWalking(topNodes, null);
    }
    return pctx;
  }

  private class FilterTransformer implements NodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {
      FilterOperator filter = (FilterOperator) nd;
      FilterDesc desc = filter.getConf();

      TableScanOperator ts = (TableScanOperator) stack.get(stack.size() - 2);

      // collect
      CollectContext removalContext = new CollectContext();
      collect(desc.getPredicate(), removalContext);
      CollectContext tsRemovalContext = new CollectContext();
      collect(ts.getConf().getFilterExpr(), tsRemovalContext);

      for (Pair<ExprNodeDesc,ExprNodeDesc> pair : removalContext.dynamicListNodes) {
        ExprNodeDesc child = pair.left;
        ExprNodeDesc columnDesc = child.getChildren().get(0);
        assert child.getChildren().get(1) instanceof ExprNodeDynamicListDesc;
        ExprNodeDesc parent = pair.right;

        String column = ExprNodeDescUtils.extractColName(columnDesc);
        if (column != null) {
          Table table = ts.getConf().getTableMetadata();

          boolean generate = false;
          if (table != null && table.isPartitionKey(column)) {
            generate = true;
            for (ExprNodeDesc filterColumnDesc : removalContext.comparatorNodes) {
              if (columnDesc.isSame(filterColumnDesc)) {
                generate = false;
                break;
              }
            }
          }
          if (!generate) {
            // We can safely remove the condition by replacing it with "true"
            ExprNodeDesc constNode = new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, Boolean.TRUE);
            if (parent == null) {
              desc.setPredicate(constNode);
            } else {
              int i = parent.getChildren().indexOf(child);
              parent.getChildren().remove(i);
              parent.getChildren().add(i, constNode);
            }
            // We remove it from the TS too if it was pushed
            for (Pair<ExprNodeDesc,ExprNodeDesc> tsPair : tsRemovalContext.dynamicListNodes) {
              ExprNodeDesc tsChild = tsPair.left;
              ExprNodeDesc tsParent = tsPair.right;
              if (tsChild.isSame(child)) {
                if (tsParent == null) {
                  ts.getConf().setFilterExpr(null);
                } else {
                  int i = tsParent.getChildren().indexOf(tsChild);
                  if (i != -1) {
                    tsParent.getChildren().remove(i);
                    tsParent.getChildren().add(i, constNode);
                  }
                }
                break;
              }
            }
            if (LOG.isInfoEnabled()) {
              LOG.info("Dynamic pruning condition removed: " + child);
            }
          }
        }
      }
      return false;
    }
  }

  private static void collect(ExprNodeDesc pred, CollectContext listContext) {
    collect(null, pred, listContext);
  }

  private static void collect(ExprNodeDesc parent, ExprNodeDesc child, CollectContext listContext) {
    if (child instanceof ExprNodeGenericFuncDesc &&
            ((ExprNodeGenericFuncDesc)child).getGenericUDF() instanceof GenericUDFIn) {
      if (child.getChildren().get(1) instanceof ExprNodeDynamicListDesc) {
        listContext.dynamicListNodes.add(new Pair<ExprNodeDesc,ExprNodeDesc>(child, parent));
      }
      return;
    }
    if (child instanceof ExprNodeGenericFuncDesc &&
            ((ExprNodeGenericFuncDesc)child).getGenericUDF() instanceof GenericUDFBaseCompare &&
            child.getChildren().size() == 2) {
      ExprNodeDesc leftCol = child.getChildren().get(0);
      ExprNodeDesc rightCol = child.getChildren().get(1);
      ExprNodeColumnDesc leftColDesc = ExprNodeDescUtils.getColumnExpr(leftCol);
      if (leftColDesc != null) {
        boolean rightConstant = false;
        if (rightCol instanceof ExprNodeConstantDesc) {
          rightConstant = true;
        } else if (rightCol instanceof ExprNodeGenericFuncDesc) {
          ExprNodeDesc foldedExpr = ConstantPropagateProcFactory.foldExpr((ExprNodeGenericFuncDesc)rightCol);
          rightConstant = foldedExpr != null;
        }
        if (rightConstant) {
          listContext.comparatorNodes.add(leftColDesc);
        }
      } else {
        ExprNodeColumnDesc rightColDesc = ExprNodeDescUtils.getColumnExpr(rightCol);
        if (rightColDesc != null) {
          boolean leftConstant = false;
          if (leftCol instanceof ExprNodeConstantDesc) {
            leftConstant = true;
          } else if (leftCol instanceof ExprNodeGenericFuncDesc) {
            ExprNodeDesc foldedExpr = ConstantPropagateProcFactory.foldExpr((ExprNodeGenericFuncDesc)leftCol);
            leftConstant = foldedExpr != null;
          }
          if (leftConstant) {
            listContext.comparatorNodes.add(rightColDesc);
          }
        }
      }
      return;
    }
    if (FunctionRegistry.isOpAnd(child)) {
      for (ExprNodeDesc newChild : child.getChildren()) {
        collect(child, newChild, listContext);
      }
    }
  }

  private class CollectContext implements NodeProcessorCtx {

    private final List<Pair<ExprNodeDesc,ExprNodeDesc>> dynamicListNodes;
    private final List<ExprNodeDesc> comparatorNodes;

    public CollectContext() {
      this.dynamicListNodes = Lists.<Pair<ExprNodeDesc,ExprNodeDesc>>newArrayList();
      this.comparatorNodes = Lists.<ExprNodeDesc>newArrayList();
    }

  }

}
