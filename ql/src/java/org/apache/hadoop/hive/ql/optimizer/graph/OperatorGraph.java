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

import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.CommonJoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemiJoinBranchInfo;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;
import org.apache.hadoop.hive.ql.plan.MapJoinDesc;

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

  private final DagGraph<Operator<?>, OpEdge> dagGraph;
  private final Set<Cluster> clusterSet;
  private final Map<Operator<?>, Set<Cluster>> operatorToCluster;

  public enum EdgeType {
    FLOW, SEMIJOIN, DPP, TEST, BROADCAST
  }

  public static class OpEdge {

    private final EdgeType et;

    public OpEdge(EdgeType et) {
      this.et = et;
    }

    public EdgeType getEdgeType() {
      return et;
    }

  }

  public interface OperatorEdgePredicate {

    boolean accept(Operator<?> s, Operator<?> t, OpEdge opEdge);

  }

  public class Cluster {

    private final Set<Operator<?>> members = new HashSet<>();

    public void add(Operator<?> operator) {
      members.add(operator);
    }

    public void merge(Cluster cluster) {
      members.addAll(cluster.getMembers());
    }

    public Set<Cluster> parentClusters(OperatorEdgePredicate traverseEdge) {
      Set<Cluster> ret = new HashSet<Cluster>();
      for (Operator<?> operator : members) {
        Stream<Operator<?>> foreignParentOperators =
            dagGraph.predecessors(operator).stream()
                .filter(pOp -> !members.contains(pOp))
                .filter(pOp -> traverseEdge.accept(pOp, operator, dagGraph.getEdge(pOp, operator).get()));
        foreignParentOperators.forEach(parentOperator -> {
          for (Cluster parentCluster: operatorToCluster.get(parentOperator)) {
            if (!parentCluster.getMembers().contains(operator)) {
              ret.add(parentCluster);
            }
          }
        });
      }
      return ret;
    }

    public Set<Cluster> childClusters(OperatorEdgePredicate traverseEdge) {
      Set<Cluster> ret = new HashSet<Cluster>();
      for (Operator<?> operator : members) {
        Stream<Operator<?>> foreignChildOperators =
            dagGraph.successors(operator).stream()
                .filter(cOp -> !members.contains(cOp))
                .filter(cOp -> traverseEdge.accept(operator, cOp, dagGraph.getEdge(operator, cOp).get()));
        foreignChildOperators.forEach(childOperator -> {
          for (Cluster childCluster: operatorToCluster.get(childOperator)) {
            if (!childCluster.getMembers().contains(operator)) {
              ret.add(childCluster);
            }
          }
        });
      }
      return ret;
    }

    public Set<Operator<?>> getMembers() {
      return Collections.unmodifiableSet(members);
    }

  }

  private Cluster createCluster(Operator<?> rootOperator) {
    Cluster cluster = new Cluster();
    Queue<Operator<?>> remainingOperators = new LinkedList<>();
    remainingOperators.add(rootOperator);

    while (!remainingOperators.isEmpty()) {
      Operator<?> currentOperator = remainingOperators.poll();
      if (!cluster.getMembers().contains(currentOperator)) {
        cluster.add(currentOperator);

        if (!(currentOperator instanceof ReduceSinkOperator)) {
          remainingOperators.addAll(currentOperator.getChildOperators());
        }
      }
    }

    return cluster;
  }

  private Set<Cluster> createClusterSet(ParseContext pctx) {
    Set<Operator<?>> rootOperators = new HashSet<>(pctx.getTopOps().values());
    Set<Operator<?>> joinOperators = new HashSet<>();
    for (Operator<?> operator: pctx.getAllOps()) {
      if (operator instanceof CommonJoinOperator) {
        if (operator instanceof MapJoinOperator) {
          MapJoinDesc desc = (MapJoinDesc) operator.getConf();
          if (desc.isDynamicPartitionHashJoin()) {
            joinOperators.add(operator);
          }
        } else {
          joinOperators.add(operator);
        }
      }

      if (operator instanceof ReduceSinkOperator) {
        for (Operator<?> childOperator: operator.getChildOperators()) {
          if (childOperator instanceof MapJoinOperator) {
            MapJoinDesc conf = (MapJoinDesc) childOperator.getConf();
            if (conf.isDynamicPartitionHashJoin()) {
              rootOperators.add(childOperator);
            }
          } else {
            rootOperators.add(childOperator);
          }
        }
      }
    }

    Set<Cluster> clusters = new HashSet<>();
    for (Operator<?> rootOperator: rootOperators) {
      clusters.add(createCluster(rootOperator));
    }

    for (Operator<?> joinOp: joinOperators) {
      Set<Cluster> joinOpIncludingClusters = new HashSet<>();
      for (Cluster cluster: clusters) {
        if (cluster.getMembers().contains(joinOp)) {
          joinOpIncludingClusters.add(cluster);
        }
      }

      if (joinOpIncludingClusters.size() > 1) {
        Cluster mergedCluster = new Cluster();
        for (Cluster cluster: joinOpIncludingClusters) {
          mergedCluster.merge(cluster);
        }

        clusters.add(mergedCluster);
        clusters.removeAll(joinOpIncludingClusters);
      }
    }

    return clusters;
  }

  private DagGraph<Operator<?>, OperatorGraph.OpEdge> createDagGraph(ParseContext pctx) {
    DagGraph<Operator<?>, OperatorGraph.OpEdge> graph = new DagGraph<>();
    for (Operator<?> operator: pctx.getAllOps()) {
      List<Operator<?>> parents = operator.getParentOperators();
      for (Operator<?> parentOperator: parents) {
        if (operator instanceof MapJoinOperator && parentOperator instanceof ReduceSinkOperator) {
          graph.putEdgeValue(parentOperator, operator, new OpEdge(EdgeType.BROADCAST));
        } else {
          graph.putEdgeValue(parentOperator, operator, new OpEdge(EdgeType.FLOW));
        }
      }

      SemiJoinBranchInfo sji = pctx.getRsToSemiJoinBranchInfo().get(operator);
      if (sji != null) {
        graph.putEdgeValue(operator, sji.getTsOp(), new OpEdge(EdgeType.SEMIJOIN));
      }
      if (operator instanceof AppMasterEventOperator) {
        DynamicPruningEventDesc dped = (DynamicPruningEventDesc) operator.getConf();
        TableScanOperator ts = dped.getTableScan();
        graph.putEdgeValue(operator, ts, new OpEdge(EdgeType.DPP));
      }
    }
    return graph;
  }

  public OperatorGraph(ParseContext pctx) {
    dagGraph = createDagGraph(pctx);
    clusterSet = Collections.unmodifiableSet(createClusterSet(pctx));
    operatorToCluster = new HashMap<>();

    for (Cluster cluster: clusterSet) {
      for (Operator<?> operator: cluster.getMembers()) {
        operatorToCluster
            .computeIfAbsent(operator, o -> new HashSet<>())
            .add(cluster);
      }
    }
  }

  public boolean mayMerge(Operator<?> opA, Operator<?> opB) {
    try {
      dagGraph.putEdgeValue(opA, opB, new OpEdge(EdgeType.TEST));
      dagGraph.removeEdge(opA, opB);
      dagGraph.putEdgeValue(opB, opA, new OpEdge(EdgeType.TEST));
      dagGraph.removeEdge(opB, opA);
      return true;
    } catch (IllegalArgumentException iae) {
      return false;
    } finally {
      dagGraph.removeEdge(opA, opB);
      dagGraph.removeEdge(opB, opA);
    }
  }

  public Set<Cluster> clusterOf(Operator<?> operator) {
    return Collections.unmodifiableSet(operatorToCluster.get(operator));
  }

  public Set<Cluster> getClusters() {
    return clusterSet;
  }

  public DagGraph<Operator<?>, OpEdge> getDagGraph() {
    return dagGraph;
  }

}
