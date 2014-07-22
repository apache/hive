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
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SelectOperator;
import org.apache.hadoop.hive.ql.lib.DefaultGraphWalker;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AggregationDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;

// execute final aggregation stage for simple fetch query on fetch task
public class SimpleFetchAggregation implements Transform {

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    if (pctx.getFetchTask() != null || !pctx.getQB().getIsQuery() ||
        pctx.getQB().isAnalyzeRewrite() || pctx.getQB().isCTAS() ||
        pctx.getLoadFileWork().size() > 1 || !pctx.getLoadTableWork().isEmpty()) {
      return pctx;
    }
    String GBY = GroupByOperator.getOperatorName() + "%";
    String RS = ReduceSinkOperator.getOperatorName() + "%";
    String SEL = SelectOperator.getOperatorName() + "%";
    String FS = FileSinkOperator.getOperatorName() + "%";

    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1", GBY + RS + GBY + SEL + FS), new SingleGBYProcessor(pctx));

    Dispatcher disp = new DefaultRuleDispatcher(null, opRules, null);
    GraphWalker ogw = new DefaultGraphWalker(disp);

    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pctx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    return pctx;
  }

  static class SingleGBYProcessor implements NodeProcessor {

    private ParseContext pctx;

    public SingleGBYProcessor(ParseContext pctx) {
      this.pctx = pctx;
    }

    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      FileSinkOperator FS = (FileSinkOperator) nd;
      GroupByOperator cGBY = (GroupByOperator) stack.get(stack.size() - 3);
      ReduceSinkOperator RS = (ReduceSinkOperator) stack.get(stack.size() - 4);
      if (RS.getConf().getNumReducers() != 1 || !RS.getConf().getKeyCols().isEmpty()) {
        return null;
      }
      GroupByOperator pGBY = (GroupByOperator) stack.get(stack.size() - 5);

      Path fileName = FS.getConf().getFinalDirName();
      TableDesc tsDesc = createIntermediateFS(pGBY, fileName);

      for (AggregationDesc aggregation : cGBY.getConf().getAggregators()) {
        List<ExprNodeDesc> parameters = aggregation.getParameters();
        aggregation.setParameters(ExprNodeDescUtils.backtrack(parameters, cGBY, RS));
      }

      pctx.setFetchTabledesc(tsDesc);
      pctx.setFetchSource(cGBY);
      pctx.setFetchSink(SimpleFetchOptimizer.replaceFSwithLS(FS, "NULL"));

      RS.setParentOperators(null);
      RS.setChildOperators(null);
      cGBY.setParentOperators(null);
      return null;
    }

    private TableDesc createIntermediateFS(Operator<?> parent, Path fileName) {
      TableDesc tsDesc = PlanUtils.getIntermediateFileTableDesc(PlanUtils
          .getFieldSchemasFromRowSchema(parent.getSchema(), "temporarycol"));

      // Create a file sink operator for this file name
      FileSinkDesc desc = new FileSinkDesc(fileName, tsDesc, false);
      FileSinkOperator newFS = (FileSinkOperator) OperatorFactory.get(desc, parent.getSchema());

      newFS.setParentOperators(new ArrayList<Operator<? extends OperatorDesc>>());
      newFS.getParentOperators().add(parent);

      parent.getChildOperators().clear();
      parent.getChildOperators().add(newFS);

      return tsDesc;
    }
  }
}
