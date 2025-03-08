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

import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Joiner;

import org.apache.hadoop.hive.ql.exec.FilterOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.optimizer.graph.OperatorGraph.Cluster;
import org.apache.hadoop.hive.ql.optimizer.graph.OperatorGraph.OpEdge;
import org.apache.hadoop.hive.ql.plan.FilterDesc;
import org.apache.hadoop.hive.ql.plan.JoinDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import java.util.Optional;

public class DotExporter {

  private OperatorGraph operatorGraph;

  public DotExporter(OperatorGraph operatorGraph) {
    this.operatorGraph = operatorGraph;
  }

  public void write(File outFile) throws Exception {
    DagGraph<Operator<?>, OpEdge> g = operatorGraph.getDagGraph();
    PrintWriter writer = new PrintWriter(outFile);
    writer.println("digraph G");
    writer.println("{%n");
    Set<Cluster> clusters = operatorGraph.getClusters();
    Map<Cluster, Integer> clusterIndicesMap = new HashMap<>();
    int idx = 0;
    for (Cluster cluster : clusters) {
      idx++;
      clusterIndicesMap.put(cluster, idx);
      writer.printf("subgraph cluster_%d {%n", idx);
      for (Operator<?> member : cluster.getMembers()) {
        writer.printf("%s;%n", nodeName(member, idx));
      }
      writer.printf("label = \"cluster %d\";%n", idx);
      writer.printf("}%n");
    }
    Set<Operator<?>> nodes = g.nodes();
    for (Operator<?> n : nodes) {
      for (Cluster curCluster: operatorGraph.clusterOf(n)) {
        int curClusterIdx = clusterIndicesMap.get(curCluster);
        writer.printf("%s[shape=record,label=\"%s\",%s];%n",
            nodeName(n, curClusterIdx), nodeLabel(n), style(n));
        Set<Operator<?>> succ = g.successors(n);
        for (Operator<?> s : succ) {
          Optional<OpEdge> e = g.getEdge(n, s);
          String style = "";
          switch(e.get().getEdgeType()) {
            case BROADCAST:
              style = "[color=blue,label=\"BROADCAST\"]";
              break;
            case DPP:
              style = "[color=green,label=\"DPP\"]";
              break;
            case SEMIJOIN:
              style = "[color=red,label=\"SEMIJOIN\"]";
              break;
          }

          Set<Cluster> childClusters = operatorGraph.clusterOf(s);
          if (childClusters.contains(curCluster)) {
            writer.printf("%s->%s%s;%n", nodeName(n, curClusterIdx), nodeName(s, curClusterIdx), style);
          } else {
            for (Cluster childCluster: childClusters) {
              int childClusterIdx = clusterIndicesMap.get(childCluster);
              writer.printf("%s->%s%s;%n", nodeName(n, curClusterIdx), nodeName(s, childClusterIdx), style);
            }
          }
        }
      }
    }

    writer.println("}%n");
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
    if ((n instanceof FilterOperator)) {
      FilterOperator fil = (FilterOperator) n;
      FilterDesc conf = fil.getConf();
      rows.add(vBox("filter:", escape(conf.getPredicateString())));
    }
    return vBox(rows);
  }

  private String escape(String str) {
    StringWriter writer = new StringWriter(str.length() * 2);
    for (int i = 0; i < str.length(); i++) {
      char c = str.charAt(i);
      switch (c) {
      case '\\':
      case '<':
      case '>':
      case '|':
        writer.write("\\");
      default:
        writer.write(c);
      }
    }
    return writer.toString();

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

  private String nodeName(Operator<?> member, int clusterIndex) {
    return String.format("\"%s-cluster%d\"", member, clusterIndex);
  }

  private String nodeName0(Operator<?> member) {
    return member.toString();
  }

}
