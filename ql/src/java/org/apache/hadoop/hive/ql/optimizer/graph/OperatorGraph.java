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


package org.apache.hadoop.hive.ql.optimizer.graph;

import java.io.File;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemiJoinBranchInfo;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import com.google.common.collect.Sets;

/**
 * Represents the Operator tree as a graph.
 *
 * The Operator-s already have graph like parent/child relationships.
 * However there are some information which might be skewed away in the configuration (like SJ connections).
 * And the actual execution time "vertex boundaries" are only there implicitly.
 *
 * The goal of this class is to parse in the different informations and provide some operations above it:
 *  - decompose the graph into "clusters" ; these are the runtime equvivalent of execution vertices.
 *  - ensures that the plan has a valid DAG property
 *  - hidden edges are also added - for ex: "semijoin"
 *  - connections to more easily consumable graph layout tools could help understand plans better
 */
public class OperatorGraph {

  DagGraph<Operator<?>, OpEdge> g;

  enum EdgeType {
    FLOW, SEMIJOIN, DPP, TEST,
  }

  static class OpEdge {

    private final EdgeType et;
    private final int index;

    public OpEdge(EdgeType et) {
      this(et, 0);
    }

    public OpEdge(EdgeType et, int index) {
      this.et = et;
      this.index = index;
    }

  }


  Map<Operator<?>, Cluster> nodeCluster = new HashMap<>();

  public class Cluster {

    Set<Operator<?>> members = new LinkedHashSet<>();

    public void merge(Cluster o) {
      for (Operator<?> node : o.members) {
        add(node);
      }
      o.members.clear();
    }

    public void add(Operator<?> curr) {
      nodeCluster.put(curr, this);
      members.add(curr);
    }

  }


  public OperatorGraph(ParseContext pctx) {
    g = new DagGraph<Operator<?>, OperatorGraph.OpEdge>();
    Set<Operator<?>> visited = Sets.newIdentityHashSet();
    Set<Operator<?>> seen = Sets.newIdentityHashSet();

    seen.addAll(pctx.getTopOps().values());
    while (!seen.isEmpty()) {
      Operator<?> curr = seen.iterator().next();
      seen.remove(curr);
      if (visited.contains(curr)) {
        continue;
      }

      visited.add(curr);

      Cluster currentCluster = nodeCluster.get(curr);
      if (currentCluster == null) {
        currentCluster=new Cluster();
        currentCluster.add(curr);
      }
      List<Operator<?>> parents = curr.getParentOperators();
      for (int i = 0; i < parents.size(); i++) {
        Operator<?> p = parents.get(i);
        g.putEdgeValue(p, curr, new OpEdge(EdgeType.FLOW, i));
        if (p instanceof ReduceSinkOperator) {
          // ignore cluster of parent RS
          continue;
        }
        Cluster cluster = nodeCluster.get(p);
        if (cluster != null) {
          currentCluster.merge(cluster);
        } else {
          currentCluster.add(p);
        }
      }

      SemiJoinBranchInfo sji = pctx.getRsToSemiJoinBranchInfo().get(curr);
      if (sji != null) {
        g.putEdgeValue(curr, sji.getTsOp(), new OpEdge(EdgeType.SEMIJOIN));
        seen.add(sji.getTsOp());
      }
      if (curr instanceof AppMasterEventOperator) {
        DynamicPruningEventDesc dped = (DynamicPruningEventDesc) curr.getConf();
        TableScanOperator ts = dped.getTableScan();
        g.putEdgeValue(curr, ts, new OpEdge(EdgeType.DPP));
        seen.add(ts);
      }

      List<Operator<?>> ccc = curr.getChildOperators();
      for (Operator<?> operator : ccc) {
        seen.add(operator);
      }
    }
  }

  public void toDot(File outFile) throws Exception {
    new DotExporter(this).write(outFile);
  }

  public boolean mayMerge(Operator<?> opA, Operator<?> opB) {
    try {
      g.putEdgeValue(opA, opB, new OpEdge(EdgeType.TEST));
      g.removeEdge(opA, opB);
      g.putEdgeValue(opB, opA, new OpEdge(EdgeType.TEST));
      g.removeEdge(opB, opA);
      return true;
    } catch (IllegalArgumentException iae) {
      return false;
    } finally {
      g.removeEdge(opA, opB);
      g.removeEdge(opB, opA);
    }
  }

  public int getDepth(Operator<?> o1) {
    return g.getDepth(o1);
  }

  public OperatorGraph implode() {
    Set<Operator<?>> nodes = new HashSet<Operator<?>>(g.nodes());
    for (Operator<?> n : nodes) {
      if (n instanceof TableScanOperator) {
        continue;
      }
      if (g.degree(n) == 2 && g.inDegree(n) == 1) {
        g.impode(n, new OpEdge(EdgeType.FLOW, -1));
      }
    }
    nodeCluster.clear();
    return this;

  }
}
