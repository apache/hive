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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.curator.shaded.com.google.common.base.Joiner;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.rules.HivePointLookupOptimizerRule.DiGraph;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemiJoinBranchInfo;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;

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

    public int getDepth(V o1) {
      Set<Node<V, E>> active = new HashSet<>();
      active.add(nodeOf(o1));
      int depth = 0;
      while(!active.isEmpty()) {
        Set<Node<V, E>> newActive = new HashSet<>();
        for (Node<V, E> node : active) {
          Set<V> p = successors(node.v);
          for (V n : p) {
            newActive.add(nodeOf(n));
          }
        }
        depth++;
        active = newActive;
      }
      return depth;
    }

  }

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
      writer.printf("%s[shape=record,label=\"%s\",%s];", nodeName(n), nodeLabel(n), style(n));
      Set<Operator<?>> succ = g.successors(n);
      for (Operator<?> s : succ) {
        writer.printf("%s->%s;", nodeName(n), nodeName(s));
      }
    }

    writer.println("}");
    writer.close();
  }


  private String style(Operator<?> n) {
    String fillColor = "white";
    OperatorDesc c = n.getConf();
    if (n instanceof TableScanOperator) {
      fillColor = "#ccffcc";
    }
    if (c instanceof JoinDesc) {
      fillColor = "#ffcccc";
    }
    return String.format("style=filled,fillcolor=\"%s\"", fillColor);
  }

  private String nodeLabel(Operator<?> n) {
    List<String> rows = new ArrayList<String>();

    rows.add(nodeName0(n));
    if ((n instanceof TableScanOperator)) {
      TableScanOperator ts = (TableScanOperator) n;
      TableScanDesc conf = ts.getConf();
      rows.add(vBox(conf.getTableName(), conf.getAlias()));
    }
    return vBox(rows);
  }

  private String hBox(List<String> rows) {
    return "" + Joiner.on("|").join(rows) + "";
  }

  private String hBox(String... rows) {
    return "" + Joiner.on("|").join(rows) + "";
  }
  private String vBox(List<String> rows) {
    return "{ " + hBox(rows) + "}";
  }

  private String vBox(String... strings) {
    return "{ " + hBox(strings) + "}";
  }


  private String nodeName(Operator<?> member) {
    return String.format("\"%s\"", member);
  }

  private String nodeName0(Operator<?> member) {
    return member.toString();
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

  public int getDepth(Operator o1) {
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
