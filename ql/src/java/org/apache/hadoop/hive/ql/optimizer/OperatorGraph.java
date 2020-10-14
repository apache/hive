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

package org.apache.hadoop.hive.ql.optimizer;

import java.io.File;
import java.io.PrintWriter;
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
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HivePointLookupOptimizerRule.DiGraph;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemiJoinBranchInfo;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;

import com.google.common.collect.Sets;

public class OperatorGraph {

  /**
   * A directed graph extended with support to check dag property.
   */
  static class DagGraph<V, E> extends DiGraph<V, E> {

    static class DagNode<V, E> extends Node<V, E> {
      int dagIdx = 0;
      public DagNode(V v) {
        super(v);
      }

      @Override
      public void addEdge(Edge<V, E> edge) {
        if (edge.s == this) {
          DagNode<V, E> t = (DagNode<V, E>) edge.t;
          ensureDagIdxAtLeast(t.dagIdx + 1);
          if (t.dagIdx > dagIdx) {
            throw new IllegalArgumentException("adding this edge would violate dag properties");
          }
        }
        super.addEdge(edge);
      }

      void ensureDagIdxAtLeast(int min) {
        if (dagIdx >= min) {
          return;
        }
        dagIdx = min;
        for (Edge<V, E> e : edges) {
          if(e.t == this) {
            DagNode<V, E> s = (DagNode<V, E>) e.s;
            s.ensureDagIdxAtLeast(min + 1);
          }
        }
      }
    }

    @Override
    protected Node<V, E> newNode(V s) {
      return new DagNode<V, E>(s);
    }
  }

  DagGraph<Operator<?>, OpEdge> g;

  enum EdgeType {
    FLOW, SEMIJOIN, DPP, CLUSTER, TEST,
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

      Cluster currentCluster = null;
      List<Operator<?>> parents = curr.getParentOperators();
      for (int i = 0; i < parents.size(); i++) {
        Operator<?> p = parents.get(i);
        g.putEdgeValue(p, curr, new OpEdge(EdgeType.FLOW, i));
        if (p instanceof ReduceSinkOperator) {
          // ignore cluster of parent RS
          continue;
        }
        Cluster cluster = nodeCluster.get(p);
        if (currentCluster == null) {
          currentCluster = cluster;
        } else {
          currentCluster.merge(cluster);
        }
      }

      if (currentCluster == null) {
        currentCluster = new Cluster();
      }
      currentCluster.add(curr);

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
    PrintWriter writer = new PrintWriter(outFile);
    writer.println("digraph G");
    writer.println("{");
    HashSet<Cluster> clusters = new HashSet<>(nodeCluster.values());
    int idx = 0;
    for (Cluster cluster : clusters) {
      idx++;
      writer.printf("subgraph cluster_%d {", idx);
      for (Operator<?> member : cluster.members) {
        writer.printf("%s;", nodeName(member));
      }
      writer.printf("label = \"cluster %d\";", idx);
      writer.printf("}");
    }
    Set<Operator<?>> nodes = g.nodes();
    for (Operator<?> n : nodes) {
      Set<Operator<?>> succ = g.successors(n);
      for (Operator<?> s : succ) {
        writer.printf("%s->%s;", nodeName(n), nodeName(s));
      }
    }

    writer.println("}");
    writer.close();
  }


  private String nodeName(Operator<?> member) {
    return String.format("\"%s\"", member);
  }

  boolean mayMerge(Operator<?> opA, Operator<?> opB) {
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
}
