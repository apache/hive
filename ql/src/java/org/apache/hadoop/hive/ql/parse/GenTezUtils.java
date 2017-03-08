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

package org.apache.hadoop.hive.ql.parse;

import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.AUTOPARALLEL;

import java.util.*;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.FilterOperator;
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
          new TezEdgeProperty(context.conf, edgeType, true,
              reduceWork.getMinReduceTasks(), reduceWork.getMaxReduceTasks(), bytesPerReducer);
    } else {
      edgeProp = new TezEdgeProperty(edgeType);
    }

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
    // we also collect table stats while collecting column stats.
    if (context.parseContext.getAnalyzeRewrite() != null) {
      mapWork.setGatheringStats(true);
    }
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
    List<Operator<?>> newRoots = SerializationUtilities.cloneOperatorTree(roots, indexForTezUnion);

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
          Map<ReduceSinkOperator, TableScanOperator> rsOpToTsOpMap =
                  context.parseContext.getRsOpToTsOpMap();
          for (ReduceSinkOperator rs : rsOpToTsOpMap.keySet()) {
            if (rsOpToTsOpMap.get(rs) == orig) {
              rsOpToTsOpMap.put(rs, (TableScanOperator) newRoot);
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

        desc.setDirName(new Path(path, "" + linked.size()));
        desc.setLinkedFileSink(true);
        desc.setParentDir(path);
        desc.setLinkedFileSinkDesc(linked);
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
  public static void removeBranch(Operator<?> event) {
    Operator<?> child = event;
    Operator<?> curr = event;

    while (curr.getChildOperators().size() <= 1) {
      child = curr;
      curr = curr.getParentOperators().get(0);
    }

    curr.removeChild(child);
  }

  public static EdgeType determineEdgeType(BaseWork preceedingWork, BaseWork followingWork, ReduceSinkOperator reduceSinkOperator) {
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
    if(!reduceSinkOperator.getConf().isOrdering()) {
      //if no sort keys are specified, use an edge that does not sort
      return EdgeType.CUSTOM_SIMPLE_EDGE;
    }
    return EdgeType.SIMPLE_EDGE;
  }

  public static void processDynamicMinMaxPushDownOperator(
          GenTezProcContext procCtx, RuntimeValuesInfo runtimeValuesInfo,
          ReduceSinkOperator rs)
          throws SemanticException {
    TableScanOperator ts = procCtx.parseContext.getRsOpToTsOpMap().get(rs);

    List<BaseWork> rsWorkList = procCtx.childToWorkMap.get(rs);
    if (ts == null || rsWorkList == null) {
      // This happens when the ReduceSink's edge has been removed by cycle
      // detection logic. Nothing to do here.
      return;
    }
    LOG.debug("ResduceSink " + rs + " to TableScan " + ts);

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
    DynamicValuePredicateContext filterDynamicValuePredicatesCollection =
            new DynamicValuePredicateContext();
    FilterDesc filterDesc = ((FilterOperator)(ts.getChildOperators().get(0))).getConf();
    collectDynamicValuePredicates(filterDesc.getPredicate(),
            filterDynamicValuePredicatesCollection);
    for (ExprNodeDesc nodeToRemove : filterDynamicValuePredicatesCollection
            .childParentMapping.keySet()) {
      // Find out if this synthetic predicate belongs to the current cycle
      boolean skip = true;
      for (ExprNodeDesc expr : nodeToRemove.getChildren()) {
        if (expr instanceof ExprNodeDynamicValueDesc ) {
          String dynamicValueIdFromExpr = ((ExprNodeDynamicValueDesc) expr)
                  .getDynamicValue().getId();
          List<String> dynamicValueIdsFromMap = context.
                  getRsToRuntimeValuesInfoMap().get(rs).getDynamicValueIDs();
          for (String dynamicValueIdFromMap : dynamicValueIdsFromMap) {
            if (dynamicValueIdFromExpr.equals(dynamicValueIdFromMap)) {
              // Intended predicate to be removed
              skip = false;
              break;
            }
          }
        }
      }
      if (!skip) {
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
        // skip the rest of the predicates
        skip = true;
      }
    }
    context.getRsOpToTsOpMap().remove(rs);
  }

  private static class DynamicValuePredicateContext implements NodeProcessorCtx {
    HashMap<ExprNodeDesc, ExprNodeDesc> childParentMapping = new HashMap<ExprNodeDesc, ExprNodeDesc>();
  }

  private static class DynamicValuePredicateProc implements NodeProcessor {

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

  private static void collectDynamicValuePredicates(ExprNodeDesc pred, NodeProcessorCtx ctx) throws SemanticException {
    // create a walker which walks the tree in a DFS manner while maintaining
    // the operator stack. The dispatcher
    // generates the plan from the operator tree
    Map<Rule, NodeProcessor> exprRules = new LinkedHashMap<Rule, NodeProcessor>();
    exprRules.put(new RuleRegExp("R1", ExprNodeDynamicValueDesc.class.getName() + "%"), new DynamicValuePredicateProc());
    Dispatcher disp = new DefaultRuleDispatcher(null, exprRules, ctx);
    GraphWalker egw = new DefaultGraphWalker(disp);
    List<Node> startNodes = new ArrayList<Node>();
    startNodes.add(pred);

    egw.startWalking(startNodes, null);
  }
}
