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

package org.apache.hadoop.hive.ql.optimizer.physical.index;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.index.bitmap.BitmapIndexHandler;
import org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler;
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
import org.apache.hadoop.hive.ql.optimizer.IndexUtils;
import org.apache.hadoop.hive.ql.optimizer.physical.PhysicalContext;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.MapredWork;

/**
 *
 * IndexWhereTaskDispatcher.  Walks a Task tree, and for the right kind of Task,
 * walks the operator tree to create an index subquery.  Then attaches the
 * subquery task to the task tree.
 *
 */
public class IndexWhereTaskDispatcher implements Dispatcher {

  private final PhysicalContext physicalContext;

  public IndexWhereTaskDispatcher(PhysicalContext context) {
    super();
    physicalContext = context;
  }

  @Override
  public Object dispatch(Node nd, Stack<Node> stack, Object... nodeOutputs)
      throws SemanticException {

    Task<? extends Serializable> task = (Task<? extends Serializable>) nd;

    ParseContext pctx = physicalContext.getParseContext();

    // create the regex's so the walker can recognize our WHERE queries
    Map<Rule, NodeProcessor> operatorRules = createOperatorRules(pctx);

    // check for no indexes on any table
    if (operatorRules == null) {
      return null;
    }

    // create context so the walker can carry the current task with it.
    IndexWhereProcCtx indexWhereOptimizeCtx = new IndexWhereProcCtx(task, pctx);

    // create the dispatcher, which fires the processor according to the rule that
    // best matches
    Dispatcher dispatcher = new DefaultRuleDispatcher(getDefaultProcessor(),
                                                      operatorRules,
                                                      indexWhereOptimizeCtx);

    // walk the mapper operator(not task) tree for each specific task
    GraphWalker ogw = new DefaultGraphWalker(dispatcher);
    ArrayList<Node> topNodes = new ArrayList<Node>();
    if (task.getWork() instanceof MapredWork) {
      topNodes.addAll(((MapredWork)task.getWork()).getAliasToWork().values());
    } else {
      return null;
    }
    ogw.startWalking(topNodes, null);

    return null;
  }

  /**
   * Create a set of rules that only matches WHERE predicates on columns we have
   * an index on.
   * @return
   */
  private Map<Rule, NodeProcessor> createOperatorRules(ParseContext pctx) throws SemanticException {
    Map<Rule, NodeProcessor> operatorRules = new LinkedHashMap<Rule, NodeProcessor>();

    List<String> supportedIndexes = new ArrayList<String>();
    supportedIndexes.add(CompactIndexHandler.class.getName());
    supportedIndexes.add(BitmapIndexHandler.class.getName());

    // query the metastore to know what columns we have indexed
    Collection<Table> topTables = pctx.getTopToTable().values();
    Map<Table, List<Index>> indexes = new HashMap<Table, List<Index>>();
    for (Table tbl : topTables)
    {
      List<Index> tblIndexes = IndexUtils.getIndexes(tbl, supportedIndexes);
      if (tblIndexes.size() > 0) {
        indexes.put(tbl, tblIndexes);
      }
    }

    // quit if our tables don't have any indexes
    if (indexes.size() == 0) {
      return null;
    }

    // We set the pushed predicate from the WHERE clause as the filter expr on
    // all table scan operators, so we look for table scan operators(TS%)
    operatorRules.put(new RuleRegExp("RULEWhere", "TS%"), new IndexWhereProcessor(indexes));

    return operatorRules;
  }


  private NodeProcessor getDefaultProcessor() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                            Object... nodeOutputs) throws SemanticException {
        return null;
      }
    };
  }

}
