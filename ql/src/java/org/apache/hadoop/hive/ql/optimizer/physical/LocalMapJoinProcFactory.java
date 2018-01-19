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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.HashTableSinkOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
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
import org.apache.hadoop.hive.ql.optimizer.physical.MapJoinResolver.LocalMapJoinProcCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.HashTableDummyDesc;
import org.apache.hadoop.hive.ql.plan.HashTableSinkDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PlanUtils;
import org.apache.hadoop.hive.ql.plan.TableDesc;

/**
 * Node processor factory for map join resolver. What it did is to replace the
 * map-join operator in the local map join task with a hash-table dump operator.
 * And if the map join is followed by a group by, the hash-table sink
 * operator/mapjoin operator should be configured to use less memory to avoid
 * OOM in group by operator.
 */
public final class LocalMapJoinProcFactory {
  private static final Logger LOG = LoggerFactory.getLogger(LocalMapJoinProcFactory.class);

  public static NodeProcessor getJoinProc() {
    return new LocalMapJoinProcessor();
  }

  public static NodeProcessor getGroupByProc() {
    return new MapJoinFollowedByGroupByProcessor();
  }

  public static NodeProcessor getDefaultProc() {
    return new NodeProcessor() {
      @Override
      public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
          Object... nodeOutputs) throws SemanticException {
        return null;
      }
    };
  }

  /**
   * MapJoinFollowByProcessor.
   *
   */
  public static class MapJoinFollowedByGroupByProcessor implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      LocalMapJoinProcCtx context = (LocalMapJoinProcCtx) ctx;
      if (!nd.getName().equals("GBY")) {
        return null;
      }
      context.setFollowedByGroupBy(true);
      GroupByOperator groupByOp = (GroupByOperator) nd;
      float groupByMemoryUsage = context.getParseCtx().getConf().getFloatVar(
          HiveConf.ConfVars.HIVEMAPJOINFOLLOWEDBYMAPAGGRHASHMEMORY);
      groupByOp.getConf().setGroupByMemoryUsage(groupByMemoryUsage);
      return null;
    }
  }

  /**
   * LocalMapJoinProcessor.
   *
   */
  public static class LocalMapJoinProcessor implements NodeProcessor {
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx, Object... nodeOutputs)
        throws SemanticException {
      LocalMapJoinProcCtx context = (LocalMapJoinProcCtx) ctx;
      if (!nd.getName().equals("MAPJOIN")) {
        return null;
      }
      MapJoinOperator mapJoinOp = (MapJoinOperator) nd;
      try {
        hasGroupBy(mapJoinOp, context);
      } catch (Exception e) {
        e.printStackTrace();
      }

      MapJoinDesc mapJoinDesc = mapJoinOp.getConf();

      // mapjoin should not affected by join reordering
      mapJoinDesc.resetOrder();

      HiveConf conf = context.getParseCtx().getConf();
      // set hashtable memory usage
      float hashtableMemoryUsage;
      if (context.isFollowedByGroupBy()) {
        hashtableMemoryUsage = conf.getFloatVar(
            HiveConf.ConfVars.HIVEHASHTABLEFOLLOWBYGBYMAXMEMORYUSAGE);
      } else {
        hashtableMemoryUsage = conf.getFloatVar(
            HiveConf.ConfVars.HIVEHASHTABLEMAXMEMORYUSAGE);
      }
      mapJoinDesc.setHashTableMemoryUsage(hashtableMemoryUsage);
      LOG.info("Setting max memory usage to " + hashtableMemoryUsage + " for table sink "
          + (context.isFollowedByGroupBy() ? "" : "not") + " followed by group by");

      HashTableSinkDesc hashTableSinkDesc = new HashTableSinkDesc(mapJoinDesc);
      HashTableSinkOperator hashTableSinkOp = (HashTableSinkOperator) OperatorFactory
          .get(mapJoinOp.getCompilationOpContext(), hashTableSinkDesc);

      // get the last operator for processing big tables
      int bigTable = mapJoinDesc.getPosBigTable();

      // todo: support tez/vectorization
      boolean useNontaged = conf.getBoolVar(
          HiveConf.ConfVars.HIVECONVERTJOINUSENONSTAGED) &&
          conf.getVar(HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("mr") &&
          !conf.getBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED);

      // the parent ops for hashTableSinkOp
      List<Operator<? extends OperatorDesc>> smallTablesParentOp =
        new ArrayList<Operator<? extends OperatorDesc>>();
      List<Operator<? extends OperatorDesc>> dummyOperators =
        new ArrayList<Operator<? extends OperatorDesc>>();
      List<Operator<? extends OperatorDesc>> directOperators =
          new ArrayList<Operator<? extends OperatorDesc>>();
      // get all parents
      List<Operator<? extends OperatorDesc>> parentsOp = mapJoinOp.getParentOperators();
      for (byte i = 0; i < parentsOp.size(); i++) {
        if (i == bigTable) {
          smallTablesParentOp.add(null);
          directOperators.add(null);
          continue;
        }
        Operator<? extends OperatorDesc> parent = parentsOp.get(i);
        boolean directFetchable = useNontaged &&
            (parent instanceof TableScanOperator || parent instanceof MapJoinOperator);
        if (directFetchable) {
          // no filter, no projection. no need to stage
          smallTablesParentOp.add(null);
          directOperators.add(parent);
          hashTableSinkDesc.getKeys().put(i, null);
          hashTableSinkDesc.getExprs().put(i, null);
          hashTableSinkDesc.getFilters().put(i, null);
        } else {
          // keep the parent id correct
          smallTablesParentOp.add(parent);
          directOperators.add(null);
          int[] valueIndex = mapJoinDesc.getValueIndex(i);
          if (valueIndex != null) {
            // remove values in key exprs
            // schema for value is already fixed in MapJoinProcessor#convertJoinOpMapJoinOp
            List<ExprNodeDesc> newValues = new ArrayList<ExprNodeDesc>();
            List<ExprNodeDesc> values = hashTableSinkDesc.getExprs().get(i);
            for (int index = 0; index < values.size(); index++) {
              if (valueIndex[index] < 0) {
                newValues.add(values.get(index));
              }
            }
            hashTableSinkDesc.getExprs().put(i, newValues);
          }
        }
        // let hashtable Op be the child of this parent
        parent.replaceChild(mapJoinOp, hashTableSinkOp);
        if (directFetchable) {
          parent.setChildOperators(null);
        }

        // create new operator: HashTable DummyOperator, which share the table desc
        HashTableDummyDesc desc = new HashTableDummyDesc();
        HashTableDummyOperator dummyOp = (HashTableDummyOperator) OperatorFactory.get(
            parent.getCompilationOpContext(), desc);
        TableDesc tbl;

        if (parent.getSchema() == null) {
          if (parent instanceof TableScanOperator) {
            tbl = ((TableScanOperator) parent).getTableDescSkewJoin();
          } else {
            throw new SemanticException("Expected parent operator of type TableScanOperator." +
              "Found " + parent.getClass().getName() + " instead.");
          }
        } else {
          // get parent schema
          RowSchema rowSchema = parent.getSchema();
          tbl = PlanUtils.getIntermediateFileTableDesc(PlanUtils.getFieldSchemasFromRowSchema(
              rowSchema, ""));
        }
        dummyOp.getConf().setTbl(tbl);
        // let the dummy op be the parent of mapjoin op
        mapJoinOp.replaceParent(parent, dummyOp);
        List<Operator<? extends OperatorDesc>> dummyChildren =
          new ArrayList<Operator<? extends OperatorDesc>>();
        dummyChildren.add(mapJoinOp);
        dummyOp.setChildOperators(dummyChildren);
        // add this dummy op to the dummp operator list
        dummyOperators.add(dummyOp);
      }
      hashTableSinkOp.setParentOperators(smallTablesParentOp);
      for (Operator<? extends OperatorDesc> op : dummyOperators) {
        context.addDummyParentOp(op);
      }
      if (hasAnyDirectFetch(directOperators)) {
        context.addDirectWorks(mapJoinOp, directOperators);
      }
      return null;
    }

    private boolean hasAnyDirectFetch(List<Operator<?>> directOperators) {
      for (Operator<?> operator : directOperators) {
        if (operator != null) {
          return true;
        }
      }
      return false;
    }

    public void hasGroupBy(Operator<? extends OperatorDesc> mapJoinOp,
        LocalMapJoinProcCtx localMapJoinProcCtx) throws Exception {
      List<Operator<? extends OperatorDesc>> childOps = mapJoinOp.getChildOperators();
      Map<Rule, NodeProcessor> opRules = new LinkedHashMap<Rule, NodeProcessor>();
      opRules.put(new RuleRegExp("R1", GroupByOperator.getOperatorName() + "%"),
        LocalMapJoinProcFactory.getGroupByProc());
      // The dispatcher fires the processor corresponding to the closest
      // matching rule and passes the context along
      Dispatcher disp = new DefaultRuleDispatcher(LocalMapJoinProcFactory.getDefaultProc(),
          opRules, localMapJoinProcCtx);
      GraphWalker ogw = new DefaultGraphWalker(disp);
      // iterator the reducer operator tree
      ArrayList<Node> topNodes = new ArrayList<Node>();
      topNodes.addAll(childOps);
      ogw.startWalking(topNodes, null);
    }
  }

  private LocalMapJoinProcFactory() {
    // prevent instantiation
  }
}
