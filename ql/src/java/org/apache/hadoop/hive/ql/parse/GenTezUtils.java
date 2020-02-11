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

package org.apache.hadoop.hive.ql.parse;

import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.AUTOPARALLEL;
import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.UNIFORM;

import java.util.*;

import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.AbstractFileMergeOperator;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SerializationUtilities;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.*;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.plan.*;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFInBloomFilter;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

/**
 * GenTezUtils is a collection of shared helper methods to produce TezWork.
 * All the methods in this class should be static, but some aren't; this is to facilitate testing.
 * Methods are made non-static on as needed basis.
 */
public class GenTezUtils {
  static final private Logger LOG = LoggerFactory.getLogger(GenTezUtils.class);

  public GenTezUtils() {
  }

  public static UnionWork createUnionWork(
      GenTezProcContext context, Operator<?> root, Operator<?> leaf, TezWork tezWork) {
    UnionWork unionWork = new UnionWork("Union "+ context.nextSequenceNumber());
    context.rootUnionWorkMap.put(root, unionWork);
    context.unionWorkMap.put(leaf, unionWork);
    tezWork.add(unionWork);
    return unionWork;
  }

  public static ReduceWork createReduceWork(
      GenTezProcContext context, Operator<?> root, TezWork tezWork) {
    assert !root.getParentOperators().isEmpty();

    boolean isAutoReduceParallelism =
        context.conf.getBoolVar(HiveConf.ConfVars.TEZ_AUTO_REDUCER_PARALLELISM);

    float maxPartitionFactor =
        context.conf.getFloatVar(HiveConf.ConfVars.TEZ_MAX_PARTITION_FACTOR);
    float minPartitionFactor = context.conf.getFloatVar(HiveConf.ConfVars.TEZ_MIN_PARTITION_FACTOR);
    long bytesPerReducer = context.conf.getLongVar(HiveConf.ConfVars.BYTESPERREDUCER);
    int defaultTinyBufferSize = context.conf.getIntVar(HiveConf.ConfVars.TEZ_SIMPLE_CUSTOM_EDGE_TINY_BUFFER_SIZE_MB);

    ReduceWork reduceWork = new ReduceWork(Utilities.REDUCENAME + context.nextSequenceNumber());
    LOG.debug("Adding reduce work (" + reduceWork.getName() + ") for " + root);
    reduceWork.setReducer(root);
    reduceWork.setNeedsTagging(GenMapRedUtils.needsTagging(reduceWork));

    // All parents should be reduce sinks. We pick the one we just walked
    // to choose the number of reducers. In the join/union case they will
    // all be -1. In sort/order case where it matters there will be only
    // one parent.
    assert context.parentOfRoot instanceof ReduceSinkOperator;
    ReduceSinkOperator reduceSink = (ReduceSinkOperator) context.parentOfRoot;

    reduceWork.setNumReduceTasks(reduceSink.getConf().getNumReducers());
    reduceWork.setSlowStart(reduceSink.getConf().isSlowStart());
    reduceWork.setUniformDistribution(reduceSink.getConf().getReducerTraits().contains(UNIFORM));

    if (isAutoReduceParallelism && reduceSink.getConf().getReducerTraits().contains(AUTOPARALLEL)) {

      // configured limit for reducers
      final int maxReducers = context.conf.getIntVar(HiveConf.ConfVars.MAXREDUCERS);
      // estimated number of reducers
      final int nReducers = reduceSink.getConf().getNumReducers();

      // min we allow tez to pick
      int minPartition = Math.max(1, (int) (nReducers * minPartitionFactor));
      minPartition = (minPartition > maxReducers) ? maxReducers : minPartition;

      // max we allow tez to pick
      int maxPartition = Math.max(1, (int) (nReducers * maxPartitionFactor));
      maxPartition = (maxPartition > maxReducers) ? maxReducers : maxPartition;

      // reduce only if the parameters are significant
      if (minPartition < maxPartition &&
          nReducers * minPartitionFactor >= 1.0) {
        reduceWork.setAutoReduceParallelism(true);

        reduceWork.setMinReduceTasks(minPartition);
        reduceWork.setMaxReduceTasks(maxPartition);
      } else if (nReducers < maxPartition) {
        // the max is good, the min is too low
        reduceWork.setNumReduceTasks(maxPartition);
      }
    }

    setupReduceSink(context, reduceWork, reduceSink);

    tezWork.add(reduceWork);

    TezEdgeProperty edgeProp;
    EdgeType edgeType = determineEdgeType(context.preceedingWork, reduceWork, reduceSink);
    if (reduceWork.isAutoReduceParallelism()) {
      edgeProp =
          new TezEdgeProperty(context.conf, edgeType, true, reduceWork.isSlowStart(),
              reduceWork.getMinReduceTasks(), reduceWork.getMaxReduceTasks(), bytesPerReducer);
    } else {
      edgeProp = new TezEdgeProperty(edgeType);
      edgeProp.setSlowStart(reduceWork.isSlowStart());
    }
    edgeProp.setBufferSize(obtainBufferSize(root, reduceSink, defaultTinyBufferSize));
    reduceWork.setEdgePropRef(edgeProp);

    tezWork.connect(
        context.preceedingWork,
        reduceWork, edgeProp);
    context.connectedReduceSinks.add(reduceSink);

    return reduceWork;
  }

  private static void setupReduceSink(
      GenTezProcContext context, ReduceWork reduceWork, ReduceSinkOperator reduceSink) {

    LOG.debug("Setting up reduce sink: " + reduceSink
        + " with following reduce work: " + reduceWork.getName());

    // need to fill in information about the key and value in the reducer
    GenMapRedUtils.setKeyAndValueDesc(reduceWork, reduceSink);

    // remember which parent belongs to which tag
    int tag = reduceSink.getConf().getTag();
    reduceWork.getTagToInput().put(tag == -1 ? 0 : tag,
         context.preceedingWork.getName());

    // remember the output name of the reduce sink
    reduceSink.getConf().setOutputName(reduceWork.getName());
  }

  public MapWork createMapWork(GenTezProcContext context, Operator<?> root,
      TezWork tezWork, PrunedPartitionList partitions) throws SemanticException {
    assert root.getParentOperators().isEmpty();
    MapWork mapWork = new MapWork(Utilities.MAPNAME + context.nextSequenceNumber());
    LOG.debug("Adding map work (" + mapWork.getName() + ") for " + root);

    // map work starts with table scan operators
    assert root instanceof TableScanOperator;
    TableScanOperator ts = (TableScanOperator) root;

    String alias = ts.getConf().getAlias();

    setupMapWork(mapWork, context, partitions, ts, alias);

    if (ts.getConf().getTableMetadata() != null && ts.getConf().getTableMetadata().isDummyTable()) {
      mapWork.setDummyTableScan(true);
    }

    if (ts.getConf().getNumBuckets() > 0) {
      mapWork.setIncludedBuckets(ts.getConf().getIncludedBuckets());
    }

    // add new item to the tez work
    tezWork.add(mapWork);

    return mapWork;
  }

  // this method's main use is to help unit testing this class
  protected void setupMapWork(MapWork mapWork, GenTezProcContext context,
      PrunedPartitionList partitions, TableScanOperator root,
      String alias) throws SemanticException {
    // All the setup is done in GenMapRedUtils
    GenMapRedUtils.setMapWork(mapWork, context.parseContext,
        context.inputs, partitions, root, alias, context.conf, false);
  }

  // removes any union operator and clones the plan
  public static void removeUnionOperators(GenTezProcContext context, BaseWork work, int indexForTezUnion)
    throws SemanticException {

    List<Operator<?>> roots = new ArrayList<Operator<?>>();
    roots.addAll(work.getAllRootOperators());
    if (work.getDummyOps() != null) {
      roots.addAll(work.getDummyOps());
    }
    roots.addAll(context.eventOperatorSet);

    // need to clone the plan.
    List<Operator<?>> newRoots = SerializationUtilities.cloneOperatorTree(roots);

    // we're cloning the operator plan but we're retaining the original work. That means
    // that root operators have to be replaced with the cloned ops. The replacement map
    // tells you what that mapping is.
    BiMap<Operator<?>, Operator<?>> replacementMap = HashBiMap.create();

    // there's some special handling for dummyOps required. Mapjoins won't be properly
    // initialized if their dummy parents aren't initialized. Since we cloned the plan
    // we need to replace the dummy operators in the work with the cloned ones.
    List<HashTableDummyOperator> dummyOps = new LinkedList<HashTableDummyOperator>();

    Iterator<Operator<?>> it = newRoots.iterator();
    for (Operator<?> orig: roots) {
      Set<FileSinkOperator> fsOpSet = OperatorUtils.findOperators(orig, FileSinkOperator.class);
      for (FileSinkOperator fsOp : fsOpSet) {
        context.fileSinkSet.remove(fsOp);
      }

      Operator<?> newRoot = it.next();

      replacementMap.put(orig, newRoot);

      if (newRoot instanceof HashTableDummyOperator) {
        // dummy ops need to be updated to the cloned ones.
        dummyOps.add((HashTableDummyOperator) newRoot);
        it.remove();
      } else if (newRoot instanceof AppMasterEventOperator) {
        // event operators point to table scan operators. When cloning these we
        // need to restore the original scan.
        if (newRoot.getConf() instanceof DynamicPruningEventDesc) {
          TableScanOperator ts = ((DynamicPruningEventDesc) orig.getConf()).getTableScan();
          if (ts == null) {
            throw new AssertionError("No table scan associated with dynamic event pruning. " + orig);
          }
          ((DynamicPruningEventDesc) newRoot.getConf()).setTableScan(ts);
        }
        it.remove();
      } else {
        if (newRoot instanceof TableScanOperator) {
          if (context.tsToEventMap.containsKey(orig)) {
            // we need to update event operators with the cloned table scan
            for (AppMasterEventOperator event : context.tsToEventMap.get(orig)) {
              ((DynamicPruningEventDesc) event.getConf()).setTableScan((TableScanOperator) newRoot);
            }
          }
          // This TableScanOperator could be part of semijoin optimization.
          Map<ReduceSinkOperator, SemiJoinBranchInfo> rsToSemiJoinBranchInfo =
                  context.parseContext.getRsToSemiJoinBranchInfo();
          for (ReduceSinkOperator rs : rsToSemiJoinBranchInfo.keySet()) {
            SemiJoinBranchInfo sjInfo = rsToSemiJoinBranchInfo.get(rs);
            if (sjInfo.getTsOp() == orig) {
              SemiJoinBranchInfo newSJInfo = new SemiJoinBranchInfo(
                      (TableScanOperator) newRoot, sjInfo.getIsHint());
              rsToSemiJoinBranchInfo.put(rs, newSJInfo);
            }
          }
          // This TableScanOperator could also be part of other events in eventOperatorSet.
          for(AppMasterEventOperator event: context.eventOperatorSet) {
            if(event.getConf() instanceof DynamicPruningEventDesc) {
              TableScanOperator ts = ((DynamicPruningEventDesc) event.getConf()).getTableScan();
              if(ts.equals(orig)){
                ((DynamicPruningEventDesc) event.getConf()).setTableScan((TableScanOperator) newRoot);
              }
            }
          }
        }
        context.rootToWorkMap.remove(orig);
        context.rootToWorkMap.put(newRoot, work);
      }
    }

    // now we remove all the unions. we throw away any branch that's not reachable from
    // the current set of roots. The reason is that those branches will be handled in
    // different tasks.
    Deque<Operator<?>> operators = new LinkedList<Operator<?>>();
    operators.addAll(newRoots);

    Set<Operator<?>> seen = new HashSet<Operator<?>>();

    Set<FileStatus> fileStatusesToFetch = null;
    if(context.parseContext.getFetchTask() != null) {
      // File sink operator keeps a reference to a list of files. This reference needs to be passed on
      // to other file sink operators which could have been added by removal of Union Operator
      fileStatusesToFetch = context.parseContext.getFetchTask().getWork().getFilesToFetch();
    }

    while(!operators.isEmpty()) {
      Operator<?> current = operators.pop();
      seen.add(current);

      if (current instanceof FileSinkOperator) {
        FileSinkOperator fileSink = (FileSinkOperator)current;

        // remember it for additional processing later
        if (context.fileSinkSet.contains(fileSink)) {
          continue;
        } else {
          context.fileSinkSet.add(fileSink);
        }

        FileSinkDesc desc = fileSink.getConf();
        Path path = desc.getDirName();
        List<FileSinkDesc> linked;

        if (!context.linkedFileSinks.containsKey(path)) {
          linked = new ArrayList<FileSinkDesc>();
          context.linkedFileSinks.put(path, linked);
        }
        linked = context.linkedFileSinks.get(path);
        linked.add(desc);

        desc.setDirName(new Path(path, AbstractFileMergeOperator.UNION_SUDBIR_PREFIX + linked.size()));
        Utilities.FILE_OP_LOGGER.debug("removing union - new desc with "
            + desc.getDirName() + "; parent " + path);
        desc.setLinkedFileSink(true);
        desc.setLinkedFileSinkDesc(linked);
        desc.setFilesToFetch(fileStatusesToFetch);
      }

      if (current instanceof AppMasterEventOperator) {
        // remember for additional processing later
        context.eventOperatorSet.add((AppMasterEventOperator) current);

        // mark the original as abandoned. Don't need it anymore.
        context.abandonedEventOperatorSet.add((AppMasterEventOperator) replacementMap.inverse()
            .get(current));
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
        assert count <= 1;

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
    LOG.debug("Setting dummy ops for work " + work.getName() + ": " + dummyOps);
    work.setDummyOps(dummyOps);
    work.replaceRoots(replacementMap);
  }

  public static void processFileSink(GenTezProcContext context, FileSinkOperator fileSink)
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
      // If underlying data is RCFile or OrcFile, RCFileBlockMerge task or
      // OrcFileStripeMerge task would be created.
      LOG.info("using CombineHiveInputformat for the merge job");
      Utilities.FILE_OP_LOGGER.debug("will generate MR work for merging files from "
          + fileSink.getConf().getDirName() + " to " + finalName);
      GenMapRedUtils.createMRWorkForMergingFiles(fileSink, finalName,
          context.dependencyTask, context.moveTask,
          hconf, context.currentTask,
          parseContext.getQueryState().getLineageState());
    }

    FetchTask fetchTask = parseContext.getFetchTask();
    if (fetchTask != null && context.currentTask.getNumChild() == 0) {
      if (fetchTask.isFetchFrom(fileSink.getConf())) {
        context.currentTask.setFetchSource(true);
      }
    }
  }

  /**
   * processAppMasterEvent sets up the event descriptor and the MapWork.
   *
   * @param procCtx
   * @param event
   */
  public static void processAppMasterEvent(
      GenTezProcContext procCtx, AppMasterEventOperator event) {
    if (procCtx.abandonedEventOperatorSet.contains(event)) {
      // don't need this anymore
      return;
    }

    DynamicPruningEventDesc eventDesc = (DynamicPruningEventDesc)event.getConf();
    TableScanOperator ts = eventDesc.getTableScan();

    MapWork work = (MapWork) procCtx.rootToWorkMap.get(ts);
    if (work == null) {
      throw new AssertionError("No work found for tablescan " + ts);
    }

    BaseWork enclosingWork = getEnclosingWork(event, procCtx);
    if (enclosingWork == null) {
      throw new AssertionError("Cannot find work for operator" + event);
    }
    String sourceName = enclosingWork.getName();

    // store the vertex name in the operator pipeline
    eventDesc.setVertexName(work.getName());
    eventDesc.setInputName(work.getAliases().get(0));

    // store table descriptor in map-work
    if (!work.getEventSourceTableDescMap().containsKey(sourceName)) {
      work.getEventSourceTableDescMap().put(sourceName, new LinkedList<TableDesc>());
    }
    List<TableDesc> tables = work.getEventSourceTableDescMap().get(sourceName);
    tables.add(event.getConf().getTable());

    // store column name in map-work
    if (!work.getEventSourceColumnNameMap().containsKey(sourceName)) {
      work.getEventSourceColumnNameMap().put(sourceName, new LinkedList<String>());
    }
    List<String> columns = work.getEventSourceColumnNameMap().get(sourceName);
    columns.add(eventDesc.getTargetColumnName());

    if (!work.getEventSourceColumnTypeMap().containsKey(sourceName)) {
      work.getEventSourceColumnTypeMap().put(sourceName, new LinkedList<String>());
    }
    List<String> columnTypes = work.getEventSourceColumnTypeMap().get(sourceName);
    columnTypes.add(eventDesc.getTargetColumnType());

    // store partition key expr in map-work
    if (!work.getEventSourcePartKeyExprMap().containsKey(sourceName)) {
      work.getEventSourcePartKeyExprMap().put(sourceName, new LinkedList<ExprNodeDesc>());
    }
    List<ExprNodeDesc> keys = work.getEventSourcePartKeyExprMap().get(sourceName);
    keys.add(eventDesc.getPartKey());

  }

  /**
   * getEncosingWork finds the BaseWork any given operator belongs to.
   */
  public static BaseWork getEnclosingWork(Operator<?> op, GenTezProcContext procCtx) {
    List<Operator<?>> ops = new ArrayList<Operator<?>>();
    findRoots(op, ops);
    for (Operator<?> r : ops) {
      BaseWork work = procCtx.rootToWorkMap.get(r);
      if (work != null) {
        return work;
      }
    }
    return null;
  }

  /*
   * findRoots returns all root operators (in ops) that result in operator op
   */
  private static void findRoots(Operator<?> op, List<Operator<?>> ops) {
    List<Operator<?>> parents = op.getParentOperators();
    if (parents == null || parents.isEmpty()) {
      ops.add(op);
      return;
    }
    for (Operator<?> p : parents) {
      findRoots(p, ops);
    }
  }

  /**
   * Remove an operator branch. When we see a fork, we know it's time to do the removal.
   * @param event the leaf node of which branch to be removed
   */
  public static Operator<?> removeBranch(Operator<?> event) {
    Operator<?> child = event;
    Operator<?> curr = event;

    while (curr.getChildOperators().size() <= 1) {
      child = curr;
      curr = curr.getParentOperators().get(0);
    }

    curr.removeChild(child);

    return child;
  }

  public static EdgeType determineEdgeType(BaseWork preceedingWork, BaseWork followingWork,
      ReduceSinkOperator reduceSinkOperator) {
    // The 1-1 edge should also work for sorted cases, however depending on the details of the shuffle
    // this might end up writing multiple compressed files or end up using an in-memory partitioned kv writer
    // the condition about ordering = false can be removed at some point with a tweak to the unordered writer
    // to never split a single output across multiple files (and never attempt a final merge)
    if (reduceSinkOperator.getConf().isForwarding() && !reduceSinkOperator.getConf().isOrdering()) {
      return EdgeType.ONE_TO_ONE_EDGE;
    }
    if (followingWork instanceof ReduceWork) {
      // Ideally there should be a better way to determine that the followingWork contains
      // a dynamic partitioned hash join, but in some cases (createReduceWork()) it looks like
      // the work must be created/connected first, before the GenTezProcContext can be updated
      // with the mapjoin/work relationship.
      ReduceWork reduceWork = (ReduceWork) followingWork;
      if (reduceWork.getReducer() instanceof MapJoinOperator) {
        MapJoinOperator joinOp = (MapJoinOperator) reduceWork.getReducer();
        if (joinOp.getConf().isDynamicPartitionHashJoin()) {
          return EdgeType.CUSTOM_SIMPLE_EDGE;
        }
      }
    }
    if (!reduceSinkOperator.getConf().isOrdering()) {
      //if no sort keys are specified, use an edge that does not sort
      return EdgeType.CUSTOM_SIMPLE_EDGE;
    }
    return EdgeType.SIMPLE_EDGE;
  }

  public static void processDynamicSemiJoinPushDownOperator(
          GenTezProcContext procCtx, RuntimeValuesInfo runtimeValuesInfo,
          ReduceSinkOperator rs)
          throws SemanticException {
    SemiJoinBranchInfo sjInfo = procCtx.parseContext.getRsToSemiJoinBranchInfo().get(rs);

    List<BaseWork> rsWorkList = procCtx.childToWorkMap.get(rs);
    if (sjInfo == null || rsWorkList == null) {
      // This happens when the ReduceSink's edge has been removed by cycle
      // detection logic. Nothing to do here.
      return;
    }

    if (rsWorkList.size() != 1) {
      StringBuilder sb = new StringBuilder();
      for (BaseWork curWork : rsWorkList) {
        if ( sb.length() > 0) {
          sb.append(", ");
        }
        sb.append(curWork.getName());
      }
      throw new SemanticException(rs + " belongs to multiple BaseWorks: " + sb.toString());
    }

    TableScanOperator ts = sjInfo.getTsOp();
    LOG.debug("ResduceSink " + rs + " to TableScan " + ts);

    BaseWork parentWork = rsWorkList.get(0);
    BaseWork childWork = procCtx.rootToWorkMap.get(ts);

    // Connect parent/child work with a brodacast edge.
    LOG.debug("Connecting Baswork - " + parentWork.getName() + " to " + childWork.getName());
    TezEdgeProperty edgeProperty = new TezEdgeProperty(EdgeType.BROADCAST_EDGE);
    TezWork tezWork = procCtx.currentTask.getWork();
    tezWork.connect(parentWork, childWork, edgeProperty);

    // Set output names in ReduceSink
    rs.getConf().setOutputName(childWork.getName());

    // Set up the dynamic values in the childWork.
    RuntimeValuesInfo childRuntimeValuesInfo =
            new RuntimeValuesInfo();
    childRuntimeValuesInfo.setTableDesc(runtimeValuesInfo.getTableDesc());
    childRuntimeValuesInfo.setDynamicValueIDs(runtimeValuesInfo.getDynamicValueIDs());
    childRuntimeValuesInfo.setColExprs(runtimeValuesInfo.getColExprs());
    childWork.setInputSourceToRuntimeValuesInfo(
            parentWork.getName(), childRuntimeValuesInfo);
  }

  // Functionality to remove semi-join optimization
  public static void removeSemiJoinOperator(ParseContext context,
                                     ReduceSinkOperator rs,
                                     TableScanOperator ts) throws SemanticException{
    // Cleanup the synthetic predicate in the tablescan operator by
    // replacing it with "true"
    LOG.debug("Removing ReduceSink " + rs + " and TableScan " + ts);
    ExprNodeDesc constNode = new ExprNodeConstantDesc(
            TypeInfoFactory.booleanTypeInfo, Boolean.TRUE);
    // TS operator
    DynamicValuePredicateContext filterDynamicValuePredicatesCollection =
            new DynamicValuePredicateContext();
    if (ts.getConf().getFilterExpr() != null) {
      collectDynamicValuePredicates(ts.getConf().getFilterExpr(),
              filterDynamicValuePredicatesCollection);
      for (ExprNodeDesc nodeToRemove : filterDynamicValuePredicatesCollection
              .childParentMapping.keySet()) {
        // Find out if this synthetic predicate belongs to the current cycle
        if (removeSemiJoinPredicate(context, rs, nodeToRemove)) {
          ExprNodeDesc nodeParent = filterDynamicValuePredicatesCollection
                  .childParentMapping.get(nodeToRemove);
          if (nodeParent == null) {
            // This was the only predicate, set filter expression to null
            ts.getConf().setFilterExpr(null);
          } else {
            int i = nodeParent.getChildren().indexOf(nodeToRemove);
            nodeParent.getChildren().remove(i);
            nodeParent.getChildren().add(i, constNode);
          }
        }
      }
    }
    // Filter operator
    for (Operator<?> op : ts.getChildOperators()) {
      if (!(op instanceof FilterOperator)) {
        continue;
      }
      FilterDesc filterDesc = ((FilterOperator) op).getConf();
      filterDynamicValuePredicatesCollection = new DynamicValuePredicateContext();
      collectDynamicValuePredicates(filterDesc.getPredicate(),
              filterDynamicValuePredicatesCollection);
      for (ExprNodeDesc nodeToRemove : filterDynamicValuePredicatesCollection
              .childParentMapping.keySet()) {
        // Find out if this synthetic predicate belongs to the current cycle
        if (removeSemiJoinPredicate(context, rs, nodeToRemove)) {
          ExprNodeDesc nodeParent = filterDynamicValuePredicatesCollection
                  .childParentMapping.get(nodeToRemove);
          if (nodeParent == null) {
            // This was the only predicate, set filter expression to const
            filterDesc.setPredicate(constNode);
          } else {
            int i = nodeParent.getChildren().indexOf(nodeToRemove);
            nodeParent.getChildren().remove(i);
            nodeParent.getChildren().add(i, constNode);
          }
        }
      }
    }
    context.getRsToSemiJoinBranchInfo().remove(rs);
  }

  /** Find out if this predicate constains the synthetic predicate to be removed */
  private static boolean removeSemiJoinPredicate(ParseContext context,
          ReduceSinkOperator rs, ExprNodeDesc nodeToRemove) {
    boolean remove = false;
    for (ExprNodeDesc expr : nodeToRemove.getChildren()) {
      if (expr instanceof ExprNodeDynamicValueDesc ) {
        String dynamicValueIdFromExpr = ((ExprNodeDynamicValueDesc) expr)
                .getDynamicValue().getId();
        List<String> dynamicValueIdsFromMap = context.
                getRsToRuntimeValuesInfoMap().get(rs).getDynamicValueIDs();
        for (String dynamicValueIdFromMap : dynamicValueIdsFromMap) {
          if (dynamicValueIdFromExpr.equals(dynamicValueIdFromMap)) {
            // Intended predicate to be removed
            remove = true;
            break;
          }
        }
      }
    }
    return remove;
  }

  // Functionality to remove semi-join optimization
  public static void removeSemiJoinOperator(ParseContext context,
          AppMasterEventOperator eventOp, TableScanOperator ts) throws SemanticException{
    // Cleanup the synthetic predicate in the tablescan operator and filter by
    // replacing it with "true"
    LOG.debug("Removing AppMasterEventOperator " + eventOp + " and TableScan " + ts);
    ExprNodeDesc constNode = new ExprNodeConstantDesc(
            TypeInfoFactory.booleanTypeInfo, Boolean.TRUE);
    // Retrieve generator
    DynamicPruningEventDesc dped = (DynamicPruningEventDesc) eventOp.getConf();
    // TS operator
    DynamicPartitionPrunerContext filterDynamicListPredicatesCollection =
            new DynamicPartitionPrunerContext();
    if (ts.getConf().getFilterExpr() != null) {
      collectDynamicPruningConditions(
              ts.getConf().getFilterExpr(), filterDynamicListPredicatesCollection);
      for (DynamicListContext ctx : filterDynamicListPredicatesCollection) {
        if (ctx.generator != dped.getGenerator()) {
          continue;
        }
        // remove the condition by replacing it with "true"
        if (ctx.grandParent == null) {
          // This was the only predicate, set filter expression to const
          ts.getConf().setFilterExpr(null);
        } else {
          int i = ctx.grandParent.getChildren().indexOf(ctx.parent);
          ctx.grandParent.getChildren().remove(i);
          ctx.grandParent.getChildren().add(i, constNode);
        }
      }
    }
    // Filter operator
    filterDynamicListPredicatesCollection.dynLists.clear();
    for (Operator<?> op : ts.getChildOperators()) {
      if (!(op instanceof FilterOperator)) {
        continue;
      }
      FilterDesc filterDesc = ((FilterOperator) op).getConf();
      collectDynamicPruningConditions(
              filterDesc.getPredicate(), filterDynamicListPredicatesCollection);
      for (DynamicListContext ctx : filterDynamicListPredicatesCollection) {
        if (ctx.generator != dped.getGenerator()) {
          continue;
        }
        // remove the condition by replacing it with "true"
        if (ctx.grandParent == null) {
          // This was the only predicate, set filter expression to const
          filterDesc.setPredicate(constNode);
        } else {
          int i = ctx.grandParent.getChildren().indexOf(ctx.parent);
          ctx.grandParent.getChildren().remove(i);
          ctx.grandParent.getChildren().add(i, constNode);
        }
      }
    }
  }

  private static class DynamicValuePredicateContext implements NodeProcessorCtx {
    HashMap<ExprNodeDesc, ExprNodeDesc> childParentMapping = new HashMap<ExprNodeDesc, ExprNodeDesc>();
  }

  private static class DynamicValuePredicateProc implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                          Object... nodeOutputs) throws SemanticException {
      DynamicValuePredicateContext ctx = (DynamicValuePredicateContext) procCtx;
      ExprNodeDesc parent = (ExprNodeDesc) stack.get(stack.size() - 2);
      if (parent instanceof ExprNodeGenericFuncDesc) {
        ExprNodeGenericFuncDesc parentFunc = (ExprNodeGenericFuncDesc) parent;
        if (parentFunc.getGenericUDF() instanceof GenericUDFBetween ||
                parentFunc.getGenericUDF() instanceof GenericUDFInBloomFilter) {
          ExprNodeDesc grandParent = stack.size() >= 3 ?
                  (ExprNodeDesc) stack.get(stack.size() - 3) : null;
          ctx.childParentMapping.put(parentFunc, grandParent);
        }
      }

      return null;
    }
  }

  private static void collectDynamicValuePredicates(ExprNodeDesc pred, NodeProcessorCtx ctx)
          throws SemanticException {
    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<SemanticRule, SemanticNodeProcessor> exprRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    exprRules.put(new RuleRegExp("R1", ExprNodeDynamicValueDesc.class.getName() + "%"), new DynamicValuePredicateProc());
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, exprRules, ctx);
    SemanticGraphWalker egw = new DefaultGraphWalker(disp);
    List<Node> startNodes = new ArrayList<Node>();
    startNodes.add(pred);

    egw.startWalking(startNodes, null);
  }

  public static class DynamicListContext {
    public ExprNodeDynamicListDesc desc;
    public ExprNodeDesc parent;
    public ExprNodeDesc grandParent;
    public ReduceSinkOperator generator;

    public DynamicListContext(ExprNodeDynamicListDesc desc, ExprNodeDesc parent,
        ExprNodeDesc grandParent, ReduceSinkOperator generator) {
      this.desc = desc;
      this.parent = parent;
      this.grandParent = grandParent;
      this.generator = generator;
    }

    public ExprNodeDesc getKeyCol() {
      ExprNodeDesc keyCol = desc.getTarget();
      if (keyCol != null) {
        return keyCol;
      }

      return generator.getConf().getKeyCols().get(desc.getKeyIndex());
    }
  }

  public static class DynamicPartitionPrunerContext implements NodeProcessorCtx,
      Iterable<DynamicListContext> {
    public List<DynamicListContext> dynLists = new ArrayList<DynamicListContext>();

    public void addDynamicList(ExprNodeDynamicListDesc desc, ExprNodeDesc parent,
        ExprNodeDesc grandParent, ReduceSinkOperator generator) {
      dynLists.add(new DynamicListContext(desc, parent, grandParent, generator));
    }

    @Override
    public Iterator<DynamicListContext> iterator() {
      return dynLists.iterator();
    }
  }

  public static class DynamicPartitionPrunerProc implements SemanticNodeProcessor {

    /**
     * process simply remembers all the dynamic partition pruning expressions
     * found
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      ExprNodeDynamicListDesc desc = (ExprNodeDynamicListDesc) nd;
      DynamicPartitionPrunerContext context = (DynamicPartitionPrunerContext) procCtx;

      // Rule is searching for dynamic pruning expr. There's at least an IN
      // expression wrapping it.
      ExprNodeDesc parent = (ExprNodeDesc) stack.get(stack.size() - 2);
      ExprNodeDesc grandParent = stack.size() >= 3 ? (ExprNodeDesc) stack.get(stack.size() - 3) : null;

      context.addDynamicList(desc, parent, grandParent, (ReduceSinkOperator) desc.getSource());

      return context;
    }
  }

  public static Map<Node, Object> collectDynamicPruningConditions(ExprNodeDesc pred, NodeProcessorCtx ctx)
      throws SemanticException {

    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<SemanticRule, SemanticNodeProcessor> exprRules = new LinkedHashMap<SemanticRule, SemanticNodeProcessor>();
    exprRules.put(new RuleRegExp("R1", ExprNodeDynamicListDesc.class.getName() + "%"),
        new DynamicPartitionPrunerProc());

    // The dispatcher fires the processor corresponding to the closest matching
    // rule and passes the context along
    SemanticDispatcher disp = new DefaultRuleDispatcher(null, exprRules, ctx);
    SemanticGraphWalker egw = new DefaultGraphWalker(disp);

    List<Node> startNodes = new ArrayList<Node>();
    startNodes.add(pred);

    HashMap<Node, Object> outputMap = new HashMap<Node, Object>();
    egw.startWalking(startNodes, outputMap);
    return outputMap;
  }

  private static Integer obtainBufferSize(Operator<?> op, ReduceSinkOperator rsOp, int defaultTinyBufferSize) {
    if (op instanceof GroupByOperator) {
      GroupByOperator groupByOperator = (GroupByOperator) op;
      if (groupByOperator.getConf().getKeys().isEmpty() &&
          groupByOperator.getConf().getMode() == GroupByDesc.Mode.MERGEPARTIAL) {
        // Check configuration and value is -1, infer value
        int result = defaultTinyBufferSize == -1 ?
            (int) Math.ceil((double) groupByOperator.getStatistics().getDataSize() / 1E6) :
            defaultTinyBufferSize;
        if (LOG.isDebugEnabled()) {
          LOG.debug("Buffer size for output from operator {} can be set to {}Mb", rsOp, result);
        }
        return result;
      }
    }
    return null;
  }

}
