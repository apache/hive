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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.ForwardOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.spark.SparkUtilities;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkPartitionPruningSinkDesc;
import org.apache.hadoop.hive.ql.optimizer.spark.SparkSortMergeJoinFactory;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.PrunedPartitionList;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkEdgeProperty;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

/**
 * GenSparkUtils is a collection of shared helper methods to produce SparkWork
 * Cloned from GenTezUtils.
 */
public class GenSparkUtils {
  private static final Logger LOG = LoggerFactory.getLogger(GenSparkUtils.class.getName());

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

  public ReduceWork createReduceWork(GenSparkProcContext context, Operator<?> root,
    SparkWork sparkWork) throws SemanticException {
    Preconditions.checkArgument(!root.getParentOperators().isEmpty(),
        "AssertionError: expected root.getParentOperators() to be non-empty");

    ReduceWork reduceWork = new ReduceWork("Reducer " + (++sequenceNumber));
    LOG.debug("Adding reduce work (" + reduceWork.getName() + ") for " + root);
    reduceWork.setReducer(root);
    reduceWork.setNeedsTagging(GenMapRedUtils.needsTagging(reduceWork));

    // Pick the maximum # reducers across all parents as the # of reduce tasks.
    int maxExecutors = -1;
    for (Operator<? extends OperatorDesc> parentOfRoot : root.getParentOperators()) {
      Preconditions.checkArgument(parentOfRoot instanceof ReduceSinkOperator,
          "AssertionError: expected parentOfRoot to be an "
              + "instance of ReduceSinkOperator, but was "
              + parentOfRoot.getClass().getName());
      ReduceSinkOperator reduceSink = (ReduceSinkOperator) parentOfRoot;
      maxExecutors = Math.max(maxExecutors, reduceSink.getConf().getNumReducers());
    }
    reduceWork.setNumReduceTasks(maxExecutors);

    ReduceSinkOperator reduceSink = (ReduceSinkOperator) context.parentOfRoot;
    setupReduceSink(context, reduceWork, reduceSink);
    sparkWork.add(reduceWork);
    SparkEdgeProperty edgeProp = getEdgeProperty(reduceSink, reduceWork);

    sparkWork.connect(context.preceedingWork, reduceWork, edgeProp);

    return reduceWork;
  }

  protected void setupReduceSink(GenSparkProcContext context, ReduceWork reduceWork,
      ReduceSinkOperator reduceSink) {

    LOG.debug("Setting up reduce sink: " + reduceSink
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
    return createMapWork(context, root, sparkWork, partitions, false);
  }

  public MapWork createMapWork(GenSparkProcContext context, Operator<?> root,
      SparkWork sparkWork, PrunedPartitionList partitions, boolean deferSetup) throws SemanticException {
    Preconditions.checkArgument(root.getParentOperators().isEmpty(),
        "AssertionError: expected root.getParentOperators() to be empty");
    MapWork mapWork = new MapWork("Map " + (++sequenceNumber));
    LOG.debug("Adding map work (" + mapWork.getName() + ") for " + root);

    // map work starts with table scan operators
    Preconditions.checkArgument(root instanceof TableScanOperator,
      "AssertionError: expected root to be an instance of TableScanOperator, but was "
      + root.getClass().getName());
    String alias = ((TableScanOperator) root).getConf().getAlias();

    if (!deferSetup) {
      setupMapWork(mapWork, context, partitions,(TableScanOperator) root, alias);
    }

    // add new item to the Spark work
    sparkWork.add(mapWork);

    return mapWork;
  }

  // this method's main use is to help unit testing this class
  protected void setupMapWork(MapWork mapWork, GenSparkProcContext context,
      PrunedPartitionList partitions, TableScanOperator root,
      String alias) throws SemanticException {
    // All the setup is done in GenMapRedUtils
    GenMapRedUtils.setMapWork(mapWork, context.parseContext,
        context.inputs, partitions, root, alias, context.conf, false);
  }

  private void collectOperators(Operator<?> op, List<Operator<?>> opList) {
    opList.add(op);
    for (Object child : op.getChildOperators()) {
      if (child != null) {
        collectOperators((Operator<?>) child, opList);
      }
    }
  }

  // removes any union operator and clones the plan
  public void removeUnionOperators(GenSparkProcContext context, BaseWork work)
    throws SemanticException {

    List<Operator<?>> roots = new ArrayList<Operator<?>>();

    // For MapWork, getAllRootOperators is not suitable, since it checks
    // getPathToAliases, and will return null if this is empty. Here we are
    // replacing getAliasToWork, so should use that information instead.
    if (work instanceof MapWork) {
      roots.addAll(((MapWork) work).getAliasToWork().values());
    } else {
      roots.addAll(work.getAllRootOperators());
    }
    if (work.getDummyOps() != null) {
      roots.addAll(work.getDummyOps());
    }

    // need to clone the plan.
    List<Operator<?>> newRoots = SerializationUtilities.cloneOperatorTree(roots);

    // Build a map to map the original FileSinkOperator and the cloned FileSinkOperators
    // This map is used for set the stats flag for the cloned FileSinkOperators in later process
    Iterator<Operator<?>> newRootsIt = newRoots.iterator();
    for (Operator<?> root : roots) {
      Operator<?> newRoot = newRootsIt.next();
      List<Operator<?>> newOpQueue = new LinkedList<Operator<?>>();
      collectOperators(newRoot, newOpQueue);
      List<Operator<?>> opQueue = new LinkedList<Operator<?>>();
      collectOperators(root, opQueue);
      Iterator<Operator<?>> newOpQueueIt = newOpQueue.iterator();
      for (Operator<?> op : opQueue) {
        Operator<?> newOp = newOpQueueIt.next();

        // We need to update rootToWorkMap in case the op is a key, since even
        // though we clone the op tree, we're still using the same MapWork/ReduceWork.
        if (context.rootToWorkMap.containsKey(op)) {
          context.rootToWorkMap.put(newOp, context.rootToWorkMap.get(op));
        }
        // Don't remove the old entry - in SparkPartitionPruningSink it still
        // refers to the old TS, and we need to lookup it later in
        // processPartitionPruningSink.

        if (op instanceof FileSinkOperator) {
          List<FileSinkOperator> fileSinkList = context.fileSinkMap.get(op);
          if (fileSinkList == null) {
            fileSinkList = new LinkedList<FileSinkOperator>();
          }
          fileSinkList.add((FileSinkOperator) newOp);
          context.fileSinkMap.put((FileSinkOperator) op, fileSinkList);
        } else if (op instanceof SparkPartitionPruningSinkOperator) {
          SparkPartitionPruningSinkOperator oldPruningSink = (SparkPartitionPruningSinkOperator) op;
          SparkPartitionPruningSinkOperator newPruningSink = (SparkPartitionPruningSinkOperator) newOp;
          newPruningSink.getConf().setTableScan(oldPruningSink.getConf().getTableScan());
          context.pruningSinkSet.add(newPruningSink);
          context.pruningSinkSet.remove(oldPruningSink);
        }
      }
    }

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
        dummyOps.add((HashTableDummyOperator) newRoot);
        it.remove();
      } else {
        replacementMap.put(orig, newRoot);
      }
    }

    // now we remove all the unions. we throw away any branch that's not reachable from
    // the current set of roots. The reason is that those branches will be handled in
    // different tasks.
    Deque<Operator<?>> operators = new LinkedList<Operator<?>>();
    operators.addAll(newRoots);

    Set<Operator<?>> seen = new HashSet<Operator<?>>();

    while (!operators.isEmpty()) {
      Operator<?> current = operators.pop();
      seen.add(current);

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

    boolean  chDir = GenMapRedUtils.isMergeRequired(context.moveTask,
         hconf, fileSink, context.currentTask, isInsertTable);
    // Set stats config for FileSinkOperators which are cloned from the fileSink
    List<FileSinkOperator> fileSinkList = context.fileSinkMap.get(fileSink);
    if (fileSinkList != null) {
      for (FileSinkOperator fsOp : fileSinkList) {
        fsOp.getConf().setGatherStats(fileSink.getConf().isGatherStats());
        fsOp.getConf().setStatsReliable(fileSink.getConf().isStatsReliable());
      }
    }

    Path finalName = GenMapRedUtils.createMoveTask(context.currentTask,
        chDir, fileSink, parseContext, context.moveTask, hconf, context.dependencyTask);

    if (chDir) {
      // Merge the files in the destination table/partitions by creating Map-only merge job
      // If underlying data is RCFile a RCFileBlockMerge task would be created.
      LOG.info("using CombineHiveInputformat for the merge job");
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
   * Populate partition pruning information from the pruning sink operator to the
   * target MapWork (the MapWork for the big table side). The information include the source table
   * name, column name, and partition key expression. It also set up the temporary path used to
   * communicate between the target MapWork and source BaseWork.
   *
   * Here "source" refers to the small table side, while "target" refers to the big
   * table side.
   *
   * @param context the spark context.
   * @param pruningSink the pruner sink operator being processed.
   */
  public void processPartitionPruningSink(GenSparkProcContext context,
      SparkPartitionPruningSinkOperator pruningSink) {
    SparkPartitionPruningSinkDesc desc = pruningSink.getConf();
    TableScanOperator ts = desc.getTableScan();
    MapWork targetWork = (MapWork) context.rootToWorkMap.get(ts);

    Preconditions.checkArgument(
        targetWork != null,
        "No targetWork found for tablescan " + ts);

    String targetId = SparkUtilities.getWorkId(targetWork);

    BaseWork sourceWork = getEnclosingWork(pruningSink, context);
    String sourceId = SparkUtilities.getWorkId(sourceWork);

    // set up temporary path to communicate between the small/big table
    Path tmpPath = targetWork.getTmpPathForPartitionPruning();
    if (tmpPath == null) {
      Path baseTmpPath = context.parseContext.getContext().getMRTmpPath();
      tmpPath = SparkUtilities.generateTmpPathForPartitionPruning(baseTmpPath, targetId);
      targetWork.setTmpPathForPartitionPruning(tmpPath);
      LOG.info("Setting tmp path between source work and target work:\n" + tmpPath);
    }

    desc.setPath(new Path(tmpPath, sourceId));
    desc.setTargetWork(targetWork.getName());

    // store table descriptor in map-targetWork
    if (!targetWork.getEventSourceTableDescMap().containsKey(sourceId)) {
      targetWork.getEventSourceTableDescMap().put(sourceId, new LinkedList<TableDesc>());
    }
    List<TableDesc> tables = targetWork.getEventSourceTableDescMap().get(sourceId);
    tables.add(pruningSink.getConf().getTable());

    // store column name in map-targetWork
    if (!targetWork.getEventSourceColumnNameMap().containsKey(sourceId)) {
      targetWork.getEventSourceColumnNameMap().put(sourceId, new LinkedList<String>());
    }
    List<String> columns = targetWork.getEventSourceColumnNameMap().get(sourceId);
    columns.add(desc.getTargetColumnName());

    // store partition key expr in map-targetWork
    if (!targetWork.getEventSourcePartKeyExprMap().containsKey(sourceId)) {
      targetWork.getEventSourcePartKeyExprMap().put(sourceId, new LinkedList<ExprNodeDesc>());
    }
    List<ExprNodeDesc> keys = targetWork.getEventSourcePartKeyExprMap().get(sourceId);
    keys.add(desc.getPartKey());
  }

  public static SparkEdgeProperty getEdgeProperty(ReduceSinkOperator reduceSink,
      ReduceWork reduceWork) throws SemanticException {
    SparkEdgeProperty edgeProperty = new SparkEdgeProperty(SparkEdgeProperty.SHUFFLE_NONE);
    edgeProperty.setNumPartitions(reduceWork.getNumReduceTasks());
    String sortOrder = Strings.nullToEmpty(reduceSink.getConf().getOrder()).trim();

    if (hasGBYOperator(reduceSink)) {
      edgeProperty.setShuffleGroup();
      // test if the group by needs partition level sort, if so, use the MR style shuffle
      // SHUFFLE_SORT shouldn't be used for this purpose, see HIVE-8542
      if (!sortOrder.isEmpty() && groupByNeedParLevelOrder(reduceSink)) {
        edgeProperty.setMRShuffle();
      }
    }

    if (reduceWork.getReducer() instanceof JoinOperator) {
      //reduce-side join, use MR-style shuffle
      edgeProperty.setMRShuffle();
    }

    //If its a FileSink to bucketed files, also use MR-style shuffle to
    // get compatible taskId for bucket-name
    FileSinkOperator fso = getChildOperator(reduceWork.getReducer(), FileSinkOperator.class);
    if (fso != null) {
      String bucketCount = fso.getConf().getTableInfo().getProperties().getProperty(
          hive_metastoreConstants.BUCKET_COUNT);
      if (bucketCount != null && Integer.parseInt(bucketCount) > 1) {
        edgeProperty.setMRShuffle();
      }
    }

    // test if we need partition/global order, SHUFFLE_SORT should only be used for global order
    if (edgeProperty.isShuffleNone() && !sortOrder.isEmpty()) {
      if ((reduceSink.getConf().getPartitionCols() == null
          || reduceSink.getConf().getPartitionCols().isEmpty()
          || isSame(reduceSink.getConf().getPartitionCols(), reduceSink.getConf().getKeyCols()))
          && reduceSink.getConf().hasOrderBy()) {
        edgeProperty.setShuffleSort();
      } else {
        edgeProperty.setMRShuffle();
      }
    }

    // set to groupby-shuffle if it's still NONE
    // simple distribute-by goes here
    if (edgeProperty.isShuffleNone()) {
      edgeProperty.setShuffleGroup();
    }

    return edgeProperty;
  }

  /**
   * Test if we need partition level order for group by query.
   * GBY needs partition level order when distinct is present. Therefore, if the sorting
   * keys, partitioning keys and grouping keys are the same, we ignore the sort and use
   * GroupByShuffler to shuffle the data. In this case a group-by transformation should be
   * sufficient to produce the correct results, i.e. data is properly grouped by the keys
   * but keys are not guaranteed to be sorted.
   */
  private static boolean groupByNeedParLevelOrder(ReduceSinkOperator reduceSinkOperator) {
    // whether we have to enforce sort anyway, e.g. in case of RS deduplication
    if (reduceSinkOperator.getConf().isDeduplicated()) {
      return true;
    }
    List<Operator<? extends OperatorDesc>> children = reduceSinkOperator.getChildOperators();
    if (children != null && children.size() == 1
      && children.get(0) instanceof GroupByOperator) {
      GroupByOperator child = (GroupByOperator) children.get(0);
      if (isSame(reduceSinkOperator.getConf().getKeyCols(),
          reduceSinkOperator.getConf().getPartitionCols())
          && reduceSinkOperator.getConf().getKeyCols().size() == child.getConf().getKeys().size()) {
        return false;
      }
    }
    return true;
  }

  /**
   * Test if two lists of ExprNodeDesc are semantically same.
   */
  private static boolean isSame(List<ExprNodeDesc> list1, List<ExprNodeDesc> list2) {
    if (list1 != list2) {
      if (list1 != null && list2 != null) {
        if (list1.size() != list2.size()) {
          return false;
        }
        for (int i = 0; i < list1.size(); i++) {
          if (!list1.get(i).isSame(list2.get(i))) {
            return false;
          }
        }
      } else {
        return false;
      }
    }
    return true;
  }

  @SuppressWarnings("unchecked")
  public static <T> T getChildOperator(Operator<?> op, Class<T> klazz) throws SemanticException {
    if (klazz.isInstance(op)) {
      return (T) op;
    }
    List<Operator<?>> childOperators = op.getChildOperators();
    for (Operator<?> childOp : childOperators) {
      T result = getChildOperator(childOp, klazz);
      if (result != null) {
        return result;
      }
    }
    return null;
  }

  /**
   * Fill MapWork with 'local' work and bucket information for SMB Join.
   * @param context context, containing references to MapWorks and their SMB information.
   * @throws SemanticException
   */
  public void annotateMapWork(GenSparkProcContext context) throws SemanticException {
    for (SMBMapJoinOperator smbMapJoinOp : context.smbMapJoinCtxMap.keySet()) {
      //initialize mapwork with smbMapJoin information.
      SparkSMBMapJoinInfo smbMapJoinInfo = context.smbMapJoinCtxMap.get(smbMapJoinOp);
      MapWork work = smbMapJoinInfo.mapWork;
      SparkSortMergeJoinFactory.annotateMapWork(context, work, smbMapJoinOp,
        (TableScanOperator) smbMapJoinInfo.bigTableRootOp, false);
      for (Operator<?> smallTableRootOp : smbMapJoinInfo.smallTableRootOps) {
        SparkSortMergeJoinFactory.annotateMapWork(context, work, smbMapJoinOp,
          (TableScanOperator) smallTableRootOp, true);
      }
    }
  }

  public synchronized int getNextSeqNumber() {
    return ++sequenceNumber;
  }

  // test if we need group-by shuffle
  private static boolean hasGBYOperator(ReduceSinkOperator rs) {
    if (rs.getChildOperators().size() == 1) {
      if (rs.getChildOperators().get(0) instanceof GroupByOperator) {
        return true;
      } else if (rs.getChildOperators().get(0) instanceof ForwardOperator) {
        for (Operator grandChild : rs.getChildOperators().get(0).getChildOperators()) {
          if (!(grandChild instanceof GroupByOperator)) {
            return false;
          }
        }
        return true;
      }
    }
    return false;
  }

  /**
   * getEncosingWork finds the BaseWork any given operator belongs to.
   */
  public BaseWork getEnclosingWork(Operator<?> op, GenSparkProcContext procCtx) {
    List<Operator<?>> ops = new ArrayList<Operator<?>>();
    OperatorUtils.findRoots(op, ops);
    for (Operator<?> r : ops) {
      BaseWork work = procCtx.rootToWorkMap.get(r);
      if (work != null) {
        return work;
      }
    }
    return null;
  }
}
