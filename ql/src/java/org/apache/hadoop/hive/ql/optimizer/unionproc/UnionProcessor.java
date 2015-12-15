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

package org.apache.hadoop.hive.ql.optimizer.unionproc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.lib.DefaultRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.Dispatcher;
import org.apache.hadoop.hive.ql.lib.GraphWalker;
import org.apache.hadoop.hive.ql.lib.LevelOrderWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.Rule;
import org.apache.hadoop.hive.ql.lib.RuleRegExp;
import org.apache.hadoop.hive.ql.optimizer.Transform;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Implementation of the union processor. This can be enhanced later on.
 * Currently, it does the following: Identify if both the subqueries of UNION
 * are map-only. Store that fact in the unionDesc/UnionOperator. If either of
 * the sub-query involves a map-reduce job, a FS is introduced on top of the
 * UNION. This can be later optimized to clone all the operators above the
 * UNION.
 *
 * The parse Context is not changed.
 */
public class UnionProcessor extends Transform {

  /**
   * empty constructor.
   */
  public UnionProcessor() {
  }

  /**
   * Transform the query tree. For each union, store the fact whether both the
   * sub-queries are map-only
   *
   * @param pCtx
   *          the current parse context
   */
  public ParseContext transform(ParseContext pCtx) throws SemanticException {
    // create a walker which walks the tree in a BFS manner while maintaining
    // the operator stack.
    Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
    opRules.put(new RuleRegExp("R1",
      ReduceSinkOperator.getOperatorName() + "%.*" + UnionOperator.getOperatorName() + "%"),
      UnionProcFactory.getMapRedUnion());
    opRules.put(new RuleRegExp("R2",
      UnionOperator.getOperatorName() + "%.*" + UnionOperator.getOperatorName() + "%"),
      UnionProcFactory.getUnknownUnion());
    opRules.put(new RuleRegExp("R3",
      TableScanOperator.getOperatorName() + "%.*" + UnionOperator.getOperatorName() + "%"),
      UnionProcFactory.getMapUnion());

    // The dispatcher fires the processor for the matching rule and passes the
    // context along
    UnionProcContext uCtx = new UnionProcContext();
    uCtx.setParseContext(pCtx);
    Dispatcher disp = new DefaultRuleDispatcher(UnionProcFactory.getNoUnion(),
        opRules, uCtx);
    LevelOrderWalker ogw = new LevelOrderWalker(disp);
    ogw.setNodeTypes(UnionOperator.class);

    // Create a list of topop nodes
    ArrayList<Node> topNodes = new ArrayList<Node>();
    topNodes.addAll(pCtx.getTopOps().values());
    ogw.startWalking(topNodes, null);
    pCtx.setUCtx(uCtx);

    // Walk the tree again to see if the union can be removed completely
    HiveConf conf = pCtx.getConf();
    opRules.clear();
    if (conf.getBoolVar(HiveConf.ConfVars.HIVE_OPTIMIZE_UNION_REMOVE)
      && !conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("spark")) {

      opRules.put(new RuleRegExp("R5", UnionOperator.getOperatorName() + "%" +
                                 ".*" + FileSinkOperator.getOperatorName() + "%"),
        UnionProcFactory.getUnionNoProcessFile());

      disp = new DefaultRuleDispatcher(UnionProcFactory.getNoUnion(), opRules, uCtx);
      ogw = new LevelOrderWalker(disp);
      ogw.setNodeTypes(FileSinkOperator.class);

      // Create a list of topop nodes
      topNodes.clear();
      topNodes.addAll(pCtx.getTopOps().values());
      ogw.startWalking(topNodes, null);
    }

    return pCtx;
  }
}
