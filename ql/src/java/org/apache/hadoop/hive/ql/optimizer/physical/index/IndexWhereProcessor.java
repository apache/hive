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

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.ContentSummary;
import org.apache.hadoop.hive.metastore.api.Index;
import org.apache.hadoop.hive.ql.exec.MapRedTask;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.Task;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.hooks.ReadEntity;
import org.apache.hadoop.hive.ql.index.HiveIndexHandler;
import org.apache.hadoop.hive.ql.index.HiveIndexQueryContext;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.HiveUtils;
import org.apache.hadoop.hive.ql.metadata.Partition;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.IndexUtils;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

/**
*
* IndexWhereProcessor.
* Processes Operator Nodes to look for WHERE queries with a predicate column
* on which we have an index.  Creates an index subquery Task for these
* WHERE queries to use the index automatically.
*/
public class IndexWhereProcessor implements NodeProcessor {

  private static final Log LOG = LogFactory.getLog(IndexWhereProcessor.class.getName());
  private final Map<Table, List<Index>> indexes;

  public IndexWhereProcessor(Map<Table, List<Index>> indexes) {
    super();
    this.indexes = indexes;
  }

  @Override
  /**
   * Process a node of the operator tree. This matches on the rule in IndexWhereTaskDispatcher
   */
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                        Object... nodeOutputs) throws SemanticException {

    TableScanOperator operator = (TableScanOperator) nd;
    List<Node> opChildren = operator.getChildren();
    TableScanDesc operatorDesc = operator.getConf();
    if (operatorDesc == null) {
      return null;
    }
    ExprNodeDesc predicate = operatorDesc.getFilterExpr();

    IndexWhereProcCtx context = (IndexWhereProcCtx) procCtx;
    ParseContext pctx = context.getParseContext();
    LOG.info("Processing predicate for index optimization");

    if (predicate == null) {
      LOG.info("null predicate pushed down");
      return null;
    }
    LOG.info(predicate.getExprString());

    // check if we have indexes on all partitions in this table scan
    Set<Partition> queryPartitions;
    try {
      queryPartitions = IndexUtils.checkPartitionsCoveredByIndex(operator, pctx, indexes);
      if (queryPartitions == null) { // partitions not covered
        return null;
      }
    } catch (HiveException e) {
      LOG.error("Fatal Error: problem accessing metastore", e);
      throw new SemanticException(e);
    }

    // we can only process MapReduce tasks to check input size
    if (!context.getCurrentTask().isMapRedTask()) {
      return null;
    }
    MapRedTask currentTask = (MapRedTask) context.getCurrentTask();

    // get potential reentrant index queries from each index
    Map<Index, HiveIndexQueryContext> queryContexts = new HashMap<Index, HiveIndexQueryContext>();
    // make sure we have an index on the table being scanned
    TableDesc tblDesc = operator.getTableDesc();
    Table srcTable = pctx.getTopToTable().get(operator);
    if (indexes == null || indexes.get(srcTable) == null) {
      return null;
    }

    List<Index> tableIndexes = indexes.get(srcTable);
    Map<String, List<Index>> indexesByType = new HashMap<String, List<Index>>();
    for (Index indexOnTable : tableIndexes) {
      if (indexesByType.get(indexOnTable.getIndexHandlerClass()) == null) {
        List<Index> newType = new ArrayList<Index>();
        newType.add(indexOnTable);
        indexesByType.put(indexOnTable.getIndexHandlerClass(), newType);
      } else {
        indexesByType.get(indexOnTable.getIndexHandlerClass()).add(indexOnTable);
      }
    }

    // choose index type with most indexes of the same type on the table
    // TODO HIVE-2130 This would be a good place for some sort of cost based choice?
    List<Index> bestIndexes = indexesByType.values().iterator().next();
    for (List<Index> indexTypes : indexesByType.values()) {
      if (bestIndexes.size() < indexTypes.size()) {
        bestIndexes = indexTypes;
      }
    }

    // rewrite index queries for the chosen index type
    HiveIndexQueryContext tmpQueryContext = new HiveIndexQueryContext();
    tmpQueryContext.setQueryPartitions(queryPartitions);
    rewriteForIndexes(predicate, bestIndexes, pctx, currentTask, tmpQueryContext);
    List<Task<?>> indexTasks = tmpQueryContext.getQueryTasks();

    if (indexTasks != null && indexTasks.size() > 0) {
      queryContexts.put(bestIndexes.get(0), tmpQueryContext);
    }
    // choose an index rewrite to use
    if (queryContexts.size() > 0) {
      // TODO HIVE-2130 This would be a good place for some sort of cost based choice?
      Index chosenIndex = queryContexts.keySet().iterator().next();

      // modify the parse context to use indexing
      // we need to delay this until we choose one index so that we don't attempt to modify pctx multiple times
      HiveIndexQueryContext queryContext = queryContexts.get(chosenIndex);

      // prepare the map reduce job to use indexing
      MapredWork work = currentTask.getWork();
      work.setInputformat(queryContext.getIndexInputFormat());
      work.addIndexIntermediateFile(queryContext.getIndexIntermediateFile());
      // modify inputs based on index query
      Set<ReadEntity> inputs = pctx.getSemanticInputs();
      inputs.addAll(queryContext.getAdditionalSemanticInputs());
      List<Task<?>> chosenRewrite = queryContext.getQueryTasks();

      // add dependencies so index query runs first
      insertIndexQuery(pctx, context, chosenRewrite);
    }

    return null;
  }

  /**
   * Get a list of Tasks to activate use of indexes.
   * Generate the tasks for the index query (where we store results of
   * querying the index in a tmp file) inside the IndexHandler
   * @param predicate Predicate of query to rewrite
   * @param index Index to use for rewrite
   * @param pctx
   * @param task original task before rewrite
   * @param queryContext stores return values
   */
  private void rewriteForIndexes(ExprNodeDesc predicate, List<Index> indexes,
                                ParseContext pctx, Task<MapredWork> task,
                                HiveIndexQueryContext queryContext)
                                throws SemanticException {
    HiveIndexHandler indexHandler;
    // All indexes in the list are of the same type, and therefore can use the
    // same handler to generate the index query tasks
    Index index = indexes.get(0);
    try {
      indexHandler = HiveUtils.getIndexHandler(pctx.getConf(), index.getIndexHandlerClass());
    } catch (HiveException e) {
      LOG.error("Exception while loading IndexHandler: " + index.getIndexHandlerClass(), e);
      throw new SemanticException("Failed to load indexHandler: " + index.getIndexHandlerClass(), e);
    }

    // check the size
    try {
      ContentSummary inputSummary = Utilities.getInputSummary(pctx.getContext(), task.getWork(), null);
      long inputSize = inputSummary.getLength();
      if (!indexHandler.checkQuerySize(inputSize, pctx.getConf())) {
        queryContext.setQueryTasks(null);
        return;
      }
    } catch (IOException e) {
      throw new SemanticException("Failed to get task size", e);
    }

    // use the IndexHandler to generate the index query
    indexHandler.generateIndexQuery(indexes, predicate, pctx, queryContext);
    // TODO HIVE-2115 use queryContext.residualPredicate to process residual predicate

    return;
  }


  /**
   * Insert the rewrite tasks at the head of the pctx task tree
   * @param pctx
   * @param context
   * @param chosenRewrite
   */
  private void insertIndexQuery(ParseContext pctx, IndexWhereProcCtx context, List<Task<?>> chosenRewrite) {
    Task<?> wholeTableScan = context.getCurrentTask();
    LinkedHashSet<Task<?>> rewriteLeaves = new LinkedHashSet<Task<?>>();
    findLeaves(chosenRewrite, rewriteLeaves);

    for (Task<?> leaf : rewriteLeaves) {
      leaf.addDependentTask(wholeTableScan); // add full scan task as child for every index query task
    }

    // replace the original with the index sub-query as a root task
    pctx.replaceRootTask(wholeTableScan, chosenRewrite);
  }

  /**
   * Find the leaves of the task tree
   */
  private void findLeaves(List<Task<?>> tasks, Set<Task<?>> leaves) {
    for (Task<?> t : tasks) {
      if (t.getDependentTasks() == null) {
        leaves.add(t);
      } else {
        findLeaves(t.getDependentTasks(), leaves);
      }
    }
  }

}

