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

import java.util.ArrayList;
import java.util.Deque;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.FetchTask;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.exec.UnionOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.FileSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty;
import org.apache.hadoop.hive.ql.plan.TezEdgeProperty.EdgeType;
import org.apache.hadoop.hive.ql.plan.TezWork;
import org.apache.hadoop.hive.ql.plan.UnionWork;

import com.google.common.collect.BiMap;
import com.google.common.collect.HashBiMap;

import static org.apache.hadoop.hive.ql.plan.ReduceSinkDesc.ReducerTraits.AUTOPARALLEL;

/**
 * GenTezUtils is a collection of shared helper methods to produce
 * TezWork
 */
public class GenTezUtils {

  static final private Log LOG = LogFactory.getLog(GenTezUtils.class.getName());

  // sequence number is used to name vertices (e.g.: Map 1, Reduce 14, ...)
  private int sequenceNumber = 0;

  // singleton
  private static GenTezUtils utils;

  public static GenTezUtils getUtils() {
    if (utils == null) {
      utils = new GenTezUtils();
    }
    return utils;
  }

  protected GenTezUtils() {
  }

  public void resetSequenceNumber() {
    sequenceNumber = 0;
  }

  public UnionWork createUnionWork(GenTezProcContext context, Operator<?> operator, TezWork tezWork) {
    UnionWork unionWork = new UnionWork("Union "+ (++sequenceNumber));
    context.unionWorkMap.put(operator, unionWork);
    tezWork.add(unionWork);
    return unionWork;
  }

  public ReduceWork createReduceWork(GenTezProcContext context, Operator<?> root, TezWork tezWork) {
    assert !root.getParentOperators().isEmpty();

    boolean isAutoReduceParallelism =
        context.conf.getBoolVar(HiveConf.ConfVars.TEZ_AUTO_REDUCER_PARALLELISM);

    float maxPartitionFactor =
        context.conf.getFloatVar(HiveConf.ConfVars.TEZ_MAX_PARTITION_FACTOR);
    float minPartitionFactor = context.conf.getFloatVar(HiveConf.ConfVars.TEZ_MIN_PARTITION_FACTOR);
    long bytesPerReducer = context.conf.getLongVar(HiveConf.ConfVars.BYTESPERREDUCER);

    ReduceWork reduceWork = new ReduceWork("Reducer "+ (++sequenceNumber));
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
      reduceWork.setAutoReduceParallelism(true);

      // configured limit for reducers
      int maxReducers = context.conf.getIntVar(HiveConf.ConfVars.MAXREDUCERS);

      // min we allow tez to pick
      int minPartition = Math.max(1, (int) (reduceSink.getConf().getNumReducers()
        * minPartitionFactor));
      minPartition = (minPartition > maxReducers) ? maxReducers : minPartition;

      // max we allow tez to pick
      int maxPartition = (int) (reduceSink.getConf().getNumReducers() * maxPartitionFactor);
      maxPartition = (maxPartition > maxReducers) ? maxReducers : maxPartition;

      reduceWork.setMinReduceTasks(minPartition);
      reduceWork.setMaxReduceTasks(maxPartition);
    }

    setupReduceSink(context, reduceWork, reduceSink);

    tezWork.add(reduceWork);

    TezEdgeProperty edgeProp;
    if (reduceWork.isAutoReduceParallelism()) {
      edgeProp =
          new TezEdgeProperty(context.conf, EdgeType.SIMPLE_EDGE, true,
              reduceWork.getMinReduceTasks(), reduceWork.getMaxReduceTasks(), bytesPerReducer);
    } else {
      edgeProp = new TezEdgeProperty(EdgeType.SIMPLE_EDGE);
    }

    tezWork.connect(
        context.preceedingWork,
        reduceWork, edgeProp);
    context.connectedReduceSinks.add(reduceSink);

    return reduceWork;
  }

  protected void setupReduceSink(GenTezProcContext context, ReduceWork reduceWork,
      ReduceSinkOperator reduceSink) {

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
    MapWork mapWork = new MapWork("Map "+ (++sequenceNumber));
    LOG.debug("Adding map work (" + mapWork.getName() + ") for " + root);

    // map work starts with table scan operators
    assert root instanceof TableScanOperator;
    TableScanOperator ts = (TableScanOperator) root;

    String alias = ts.getConf().getAlias();

    setupMapWork(mapWork, context, partitions, root, alias);

    if (ts.getConf().getTableMetadata() != null && ts.getConf().getTableMetadata().isDummyTable()) {
      mapWork.setDummyTableScan(true);
    }

    // add new item to the tez work
    tezWork.add(mapWork);

    return mapWork;
  }

  // this method's main use is to help unit testing this class
  protected void setupMapWork(MapWork mapWork, GenTezProcContext context,
      PrunedPartitionList partitions, Operator<? extends OperatorDesc> root,
      String alias) throws SemanticException {
    // All the setup is done in GenMapRedUtils
    GenMapRedUtils.setMapWork(mapWork, context.parseContext,
        context.inputs, partitions, root, alias, context.conf, false);
  }

  // removes any union operator and clones the plan
  public void removeUnionOperators(Configuration conf, GenTezProcContext context,
      BaseWork work)
    throws SemanticException {

    List<Operator<?>> roots = new ArrayList<Operator<?>>();
    roots.addAll(work.getAllRootOperators());
    if (work.getDummyOps() != null) {
      roots.addAll(work.getDummyOps());
    }
    roots.addAll(context.eventOperatorSet);

    // need to clone the plan.
    List<Operator<?>> newRoots = Utilities.cloneOperatorTree(conf, roots);

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

        desc.setDirName(new Path(path, ""+linked.size()));
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
    work.setDummyOps(dummyOps);
    work.replaceRoots(replacementMap);
  }

  public void processFileSink(GenTezProcContext context, FileSinkOperator fileSink)
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
  public void processAppMasterEvent(GenTezProcContext procCtx, AppMasterEventOperator event) {

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
  public BaseWork getEnclosingWork(Operator<?> op, GenTezProcContext procCtx) {
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
  private void findRoots(Operator<?> op, List<Operator<?>> ops) {
    List<Operator<?>> parents = op.getParentOperators();
    if (parents == null || parents.isEmpty()) {
      ops.add(op);
      return;
    }
    for (Operator<?> p : parents) {
      findRoots(p, ops);
    }
  }
}
