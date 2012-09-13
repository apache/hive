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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

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
import org.apache.hadoop.hive.ql.plan.HashTableDummyDesc;
import org.apache.hadoop.hive.ql.plan.HashTableSinkDesc;
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

      HashTableSinkDesc hashTableSinkDesc = new HashTableSinkDesc(mapJoinOp.getConf());
      HashTableSinkOperator hashTableSinkOp = (HashTableSinkOperator) OperatorFactory
          .get(hashTableSinkDesc);

      // set hashtable memory usage
      float hashtableMemoryUsage;
      if (context.isFollowedByGroupBy()) {
        hashtableMemoryUsage = context.getParseCtx().getConf().getFloatVar(
            HiveConf.ConfVars.HIVEHASHTABLEFOLLOWBYGBYMAXMEMORYUSAGE);
      } else {
        hashtableMemoryUsage = context.getParseCtx().getConf().getFloatVar(
            HiveConf.ConfVars.HIVEHASHTABLEMAXMEMORYUSAGE);
      }
      hashTableSinkOp.getConf().setHashtableMemoryUsage(hashtableMemoryUsage);

      // get the last operator for processing big tables
      int bigTable = mapJoinOp.getConf().getPosBigTable();
      Byte[] order = mapJoinOp.getConf().getTagOrder();
      int bigTableAlias = (int) order[bigTable];

      // the parent ops for hashTableSinkOp
      List<Operator<? extends OperatorDesc>> smallTablesParentOp =
        new ArrayList<Operator<? extends OperatorDesc>>();
      List<Operator<? extends OperatorDesc>> dummyOperators =
        new ArrayList<Operator<? extends OperatorDesc>>();
      // get all parents
      List<Operator<? extends OperatorDesc>> parentsOp = mapJoinOp.getParentOperators();
      for (int i = 0; i < parentsOp.size(); i++) {
        if (i == bigTableAlias) {
          smallTablesParentOp.add(null);
          continue;
        }
        Operator<? extends OperatorDesc> parent = parentsOp.get(i);
        // let hashtable Op be the child of this parent
        parent.replaceChild(mapJoinOp, hashTableSinkOp);
        // keep the parent id correct
        smallTablesParentOp.add(parent);

        // create an new operator: HashTable DummyOpeator, which share the table desc
        HashTableDummyDesc desc = new HashTableDummyDesc();
        HashTableDummyOperator dummyOp = (HashTableDummyOperator) OperatorFactory.get(desc);
        TableDesc tbl;

        if (parent.getSchema() == null) {
          if (parent instanceof TableScanOperator) {
            tbl = ((TableScanOperator) parent).getTableDesc();
          } else {
            throw new SemanticException();
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
      return null;
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
