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

package org.apache.hadoop.hive.ql.parse.spark;

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Strings;
import org.apache.hadoop.fs.Path;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.*;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.*;

import com.google.common.base.Preconditions;

/**
 * GenSparkUtils is a collection of shared helper methods to produce SparkWork
 * Cloned from GenTezUtils.
 * TODO: need to make it fit to Spark
 */
public class GenSparkUtils {
  private static final Log logger = LogFactory.getLog(GenSparkUtils.class.getName());

  // sequence number is used to name vertices (e.g.: Map 1, Reduce 14, ...)
  private int sequenceNumber = 0;

  // singleton
  private static GenSparkUtils utils;

  public static GenSparkUtils getUtils() {
    if (utils == null) {
      utils = new GenSparkUtils();
    }
    return utils;
  }

  protected GenSparkUtils() {
  }

  public void resetSequenceNumber() {
    sequenceNumber = 0;
  }

  public UnionWork createUnionWork(GenSparkProcContext context, Operator<?> operator, SparkWork sparkWork) {
    UnionWork unionWork = new UnionWork("Union "+ (++sequenceNumber));
    context.unionWorkMap.put(operator, unionWork);
    sparkWork.add(unionWork);
    return unionWork;
  }

  public ReduceWork createReduceWork(GenSparkProcContext context, Operator<?> root, SparkWork sparkWork) {
    Preconditions.checkArgument(!root.getParentOperators().isEmpty(),
        "AssertionError: expected root.getParentOperators() to be non-empty");

    ReduceWork reduceWork = new ReduceWork("Reducer "+ (++sequenceNumber));
    logger.debug("Adding reduce work (" + reduceWork.getName() + ") for " + root);
    reduceWork.setReducer(root);
    reduceWork.setNeedsTagging(GenMapRedUtils.needsTagging(reduceWork));

    // All parents should be reduce sinks. We pick the one we just walked
    // to choose the number of reducers. In the join/union case they will
    // all be -1. In sort/order case where it matters there will be only
    // one parent.
    Preconditions.checkArgument(context.parentOfRoot instanceof ReduceSinkOperator,
        "AssertionError: expected context.parentOfRoot to be an instance of ReduceSinkOperator, but was " +
            context.parentOfRoot.getClass().getName());
    ReduceSinkOperator reduceSink = (ReduceSinkOperator) context.parentOfRoot;

    reduceWork.setNumReduceTasks(reduceSink.getConf().getNumReducers());

    setupReduceSink(context, reduceWork, reduceSink);

    sparkWork.add(reduceWork);

    // Use group-by as the default shuffler
    SparkEdgeProperty edgeProp = new SparkEdgeProperty(SparkEdgeProperty.SHUFFLE_GROUP,
        reduceWork.getNumReduceTasks());

    String sortOrder = Strings.nullToEmpty(reduceSink.getConf().getOrder()).trim();
    if (!sortOrder.isEmpty() && isSortNecessary(reduceSink)) {
      edgeProp.setShuffleSort();
    }

    sparkWork.connect(
        context.preceedingWork,
        reduceWork, edgeProp);
    context.connectedReduceSinks.add(reduceSink);

    return reduceWork;
  }

  protected void setupReduceSink(GenSparkProcContext context, ReduceWork reduceWork,
      ReduceSinkOperator reduceSink) {

    logger.debug("Setting up reduce sink: " + reduceSink
        + " with following reduce work: " + reduceWork.getName());

    // need to fill in information about the key and value in the reducer
    GenMapRedUtils.setKeyAndValueDesc(reduceWork, reduceSink);

    // remember which parent belongs to which tag
    reduceWork.getTagToInput().put(reduceSink.getConf().getTag(),
         context.preceedingWork.getName());

    // remember the output name of the reduce sink
    reduceSink.getConf().setOutputName(reduceWork.getName());
  }

  public MapWork createMapWork(GenSparkProcContext context, Operator<?> root,
      SparkWork sparkWork, PrunedPartitionList partitions) throws SemanticException {
    Preconditions.checkArgument(root.getParentOperators().isEmpty(),
        "AssertionError: expected root.getParentOperators() to be empty");
    MapWork mapWork = new MapWork("Map "+ (++sequenceNumber));
    logger.debug("Adding map work (" + mapWork.getName() + ") for " + root);

    // map work starts with table scan operators
    Preconditions.checkArgument(root instanceof TableScanOperator,
        "AssertionError: expected root to be an instance of TableScanOperator, but was " +
            root.getClass().getName());
    String alias = ((TableScanOperator)root).getConf().getAlias();

    setupMapWork(mapWork, context, partitions, root, alias);

    // add new item to the Spark work
    sparkWork.add(mapWork);

    return mapWork;
  }

  // this method's main use is to help unit testing this class
  protected void setupMapWork(MapWork mapWork, GenSparkProcContext context,
      PrunedPartitionList partitions, Operator<? extends OperatorDesc> root,
      String alias) throws SemanticException {
    // All the setup is done in GenMapRedUtils
    GenMapRedUtils.setMapWork(mapWork, context.parseContext,
        context.inputs, partitions, root, alias, context.conf, false);
  }

  // removes any union operator and clones the plan
  public void removeUnionOperators(Configuration conf, GenSparkProcContext context,
      BaseWork work)
    throws SemanticException {

    List<Operator<?>> roots = new ArrayList<Operator<?>>();
    roots.addAll(work.getAllRootOperators());
    if (work.getDummyOps() != null) {
      roots.addAll(work.getDummyOps());
    }

    // need to clone the plan.
    List<Operator<?>> newRoots = Utilities.cloneOperatorTree(conf, roots);

    // we're cloning the operator plan but we're retaining the original work. That means
    // that root operators have to be replaced with the cloned ops. The replacement map
    // tells you what that mapping is.
    Map<Operator<?>, Operator<?>> replacementMap = new HashMap<Operator<?>, Operator<?>>();

    // there's some special handling for dummyOps required. Mapjoins won't be properly
    // initialized if their dummy parents aren't initialized. Since we cloned the plan
    // we need to replace the dummy operators in the work with the cloned ones.
    List<HashTableDummyOperator> dummyOps = new LinkedList<HashTableDummyOperator>();

    Iterator<Operator<?>> it = newRoots.iterator();
    for (Operator<?> orig: roots) {
      Operator<?> newRoot = it.next();
      if (newRoot instanceof HashTableDummyOperator) {
        dummyOps.add((HashTableDummyOperator)newRoot);
        it.remove();
      } else {
        replacementMap.put(orig,newRoot);
      }
    }

    // now we remove all the unions. we throw away any branch that's not reachable from
    // the current set of roots. The reason is that those branches will be handled in
    // different tasks.
    Deque<Operator<?>> operators = new LinkedList<Operator<?>>();
    operators.addAll(newRoots);

    Set<Operator<?>> seen = new HashSet<Operator<?>>();

    while(!operators.isEmpty()) {
      Operator<?> current = operators.pop();
      seen.add(current);

      if (current instanceof FileSinkOperator) {
        FileSinkOperator fileSink = (FileSinkOperator)current;

        // remember it for additional processing later
        context.fileSinkSet.add(fileSink);

        FileSinkDesc desc = fileSink.getConf();
        Path path = desc.getDirName();
        List<FileSinkDesc> linked;

        if (!context.linkedFileSinks.containsKey(path)) {
          linked = new ArrayList<FileSinkDesc>();
          context.linkedFileSinks.put(path, linked);
        }
        linked = context.linkedFileSinks.get(path);
        linked.add(desc);
        desc.setLinkedFileSinkDesc(linked);
      }

      if (current instanceof UnionOperator) {
        Operator<?> parent = null;
        int count = 0;

        for (Operator<?> op: current.getParentOperators()) {
          if (seen.contains(op)) {
            ++count;
            parent = op;
          }
        }

        // we should have been able to reach the union from only one side.
        Preconditions.checkArgument(count <= 1,
            "AssertionError: expected count to be <= 1, but was " + count);

        if (parent == null) {
          // root operator is union (can happen in reducers)
          replacementMap.put(current, current.getChildOperators().get(0));
        } else {
          parent.removeChildAndAdoptItsChildren(current);
        }
      }

      if (current instanceof FileSinkOperator
          || current instanceof ReduceSinkOperator) {
        current.setChildOperators(null);
      } else {
        operators.addAll(current.getChildOperators());
      }
    }
    work.setDummyOps(dummyOps);
    work.replaceRoots(replacementMap);
  }

  public void processFileSink(GenSparkProcContext context, FileSinkOperator fileSink)
      throws SemanticException {

    ParseContext parseContext = context.parseContext;

    boolean isInsertTable = // is INSERT OVERWRITE TABLE
        GenMapRedUtils.isInsertInto(parseContext, fileSink);
    HiveConf hconf = parseContext.getConf();

    boolean chDir = GenMapRedUtils.isMergeRequired(context.moveTask,
        hconf, fileSink, context.currentTask, isInsertTable);

    Path finalName = GenMapRedUtils.createMoveTask(context.currentTask,
        chDir, fileSink, parseContext, context.moveTask, hconf, context.dependencyTask);

    if (chDir) {
      // Merge the files in the destination table/partitions by creating Map-only merge job
      // If underlying data is RCFile a RCFileBlockMerge task would be created.
      logger.info("using CombineHiveInputformat for the merge job");
      GenMapRedUtils.createMRWorkForMergingFiles(fileSink, finalName,
          context.dependencyTask, context.moveTask,
          hconf, context.currentTask);
    }

    FetchTask fetchTask = parseContext.getFetchTask();
    if (fetchTask != null && context.currentTask.getNumChild() == 0) {
      if (fetchTask.isFetchFrom(fileSink.getConf())) {
        context.currentTask.setFetchSource(true);
      }
    }
  }

  /**
   * Test if the sort order in the RS is necessary.
   * Unnecessary sort is mainly introduced when GBY is created. Therefore, if the sorting
   * keys, partitioning keys and grouping keys are the same, we ignore the sort and use
   * GroupByShuffler to shuffle the data. In this case a group-by transformation should be
   * sufficient to produce the correct results, i.e. data is properly grouped by the keys
   * but keys are not guaranteed to be sorted.
   */
  public static boolean isSortNecessary(ReduceSinkOperator reduceSinkOperator) {
    List<Operator<? extends OperatorDesc>> children = reduceSinkOperator.getChildOperators();
    if (children != null && children.size() == 1 &&
        children.get(0) instanceof GroupByOperator) {
      GroupByOperator child = (GroupByOperator) children.get(0);
      if (reduceSinkOperator.getConf().getKeyCols().equals(
          reduceSinkOperator.getConf().getPartitionCols()) &&
          reduceSinkOperator.getConf().getKeyCols().size() == child.getConf().getKeys().size()) {
        return false;
      }
    }
    return true;
  }
}
