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

import java.util.ArrayList;
import java.util.Comparator;
import java.util.Iterator;
import java.util.List;
import java.util.Map.Entry;
import org.apache.calcite.util.Pair;
import org.apache.commons.collections4.ListValuedMap;
import org.apache.commons.collections4.multimap.ArrayListValuedHashMap;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.RowSchema;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.optimizer.graph.OperatorGraph;
import org.apache.hadoop.hive.ql.optimizer.graph.OperatorGraph.Cluster;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;
import org.apache.hadoop.hive.ql.plan.SelectDesc;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;

/**
 * Inserts an extra RS to avoid parallel edges.
 *
 * For mapjoins, semijoins its less costly to sometimes reshuffle the existing data - than computing it from scratch.
 * Parallel edges are introduced by the {@link SharedWorkOptimizer} in case this fixer could patch them up.
 *
 * +--------------------+           +--------------------+
 * |                    |           |                    |
 * |   [...]    [...]   |           |   [...]    [...]   |
 * |     |        |     |           |     |        |     |
 * |  +-----+  +-----+  |           |  +-----+  +-----+  |
 * |  |RS_1 |  |RS_2 |  |           |  |RS_1 |  |RS_2 |  |
 * |  +-----+  +-----+  |           |  +-----+  +-----+  |
 * |     |        |     |           |     |        |     |
 * +---- | ------ | ----+           +---- | ------ | ----+
 *       |        |                       |        |
 *       |        |                       |   +--- | ---+
 *       |        |                       |   | +-----+ |
 *       |        |         >>>>          |   | |RS_T | |
 *       |        |                       |   | +-----+ |
 *       |        |                       |   +--- | ---+
 *       |        |                       |        |
 * +---- | ------ | ----+           +---- | ------ | ----+
 * |  +-----+  +-----+  |           |  +-----+  +-----+  |
 * |  |OP_1 |  |OP_2 |  |           |  |OP_1 |  |OP_2 |  |
 * |  +-----+  +-----+  |           |  +-----+  +-----+  |
 * |     |        |     |           |     |        |     |
 * |   [...]    [...]   |           |   [...]    [...]   |
 * |                    |           |                    |
 * +--------------------+           +--------------------+
 *
 */
public class ParallelEdgeFixer extends Transform {

  protected static final Logger LOG = LoggerFactory.getLogger(ParallelEdgeFixer.class);

  @Override
  public ParseContext transform(ParseContext pctx) throws SemanticException {
    OperatorGraph og = new OperatorGraph(pctx);
    fixParallelEdges(og);
    return pctx;
  }

  /**
   * To achieve reproducible plans - the operators must be placed in some stable order.
   */
  private static class OperatorPairComparator implements Comparator<Pair<Operator<?>, Operator<?>>> {
    @Override
    public int compare(Pair<Operator<?>, Operator<?>> o1, Pair<Operator<?>, Operator<?>> o2) {
      return sig(o1).compareTo(sig(o2));
    }

    private String sig(Pair<Operator<?>, Operator<?>> o1) {
      return o1.left.toString() + o1.right.toString();
    }
  }

  private void fixParallelEdges(OperatorGraph og) {

    // Identify edge operators
    ListValuedMap<Pair<Cluster, Cluster>, Pair<Operator<?>, Operator<?>>> edgeOperators =
        new ArrayListValuedHashMap<>();
    for (Cluster c : og.getClusters()) {
      for (Operator<?> o : c.getMembers()) {
        for (Operator<? extends OperatorDesc> p : o.getParentOperators()) {
          Cluster parentCluster = og.clusterOf(p);
          if (parentCluster == c) {
            continue;
          }
          edgeOperators.put(new Pair<>(parentCluster, c), new Pair<>(p, o));
        }
      }
    }

    // process all edges and fix parallel edges if there are any
    for (Pair<Cluster, Cluster> key : edgeOperators.keySet()) {
      List<Pair<Operator<?>, Operator<?>>> values = edgeOperators.get(key);
      if (values.size() <= 1) {
        continue;
      }
      // operator order must in stabile order - or we end up with falky plans causing flaky tests...
      values.sort(new OperatorPairComparator());

      // remove one optionally unsupported edge (it will be kept as is)
      removeOneEdge(values);

      Iterator<Pair<Operator<?>, Operator<?>>> it = values.iterator();
      while (it.hasNext()) {
        Pair<Operator<?>, Operator<?>> pair = it.next();
        fixParallelEdge(pair.left, pair.right);
      }
    }
  }

  private void removeOneEdge(List<Pair<Operator<?>, Operator<?>>> values) {
    Pair<Operator<?>, Operator<?>> toKeep = null;
    for (Pair<Operator<?>, Operator<?>> pair : values) {
      if (!isParallelEdgeSupported(pair)) {
        if (toKeep != null) {
          throw new RuntimeException("More than one operators which may not reshuffled!");
        }
        toKeep = pair;
      }
    }
    if (toKeep == null) {
      toKeep = values.get(values.size() - 1);
    }
    values.remove(toKeep);
  }

  private boolean isParallelEdgeSupported(Pair<Operator<?>, Operator<?>> pair) {
    Operator<?> child = pair.right;
    if (child instanceof MapJoinOperator) {
      return true;
    }
    if (child instanceof TableScanOperator) {
      return true;
    }
    return false;
  }

  /**
   * Fixes a parallel edge going into a mapjoin by introducing a concentrator RS.
   */
  private void fixParallelEdge(Operator<? extends OperatorDesc> p, Operator<?> o) {
    LOG.info("Fixing parallel by adding a concentrator RS between {} -> {}", p, o);

    ReduceSinkDesc conf = (ReduceSinkDesc) p.getConf();
    ReduceSinkDesc newConf = (ReduceSinkDesc) conf.clone();

    Operator<SelectDesc> newSEL = buildSEL(p, conf);

    Operator<ReduceSinkDesc> newRS =
        OperatorFactory.getAndMakeChild(p.getCompilationOpContext(), newConf, new ArrayList<>());

    conf.setOutputName("forward_to_" + newRS);
    conf.setTag(0);

    newConf.setKeyCols(new ArrayList<>(conf.getKeyCols()));
    newRS.setSchema(new RowSchema(p.getSchema()));

    p.replaceChild(o, newSEL);

    newSEL.setParentOperators(Lists.<Operator<?>> newArrayList(p));
    newSEL.setChildOperators(Lists.<Operator<?>> newArrayList(newRS));
    newRS.setParentOperators(Lists.<Operator<?>> newArrayList(newSEL));
    newRS.setChildOperators(Lists.<Operator<?>> newArrayList(o));

    o.replaceParent(p, newRS);

  }

  private Operator<SelectDesc> buildSEL(Operator<? extends OperatorDesc> p, ReduceSinkDesc conf) {
    List<ExprNodeDesc> colList = new ArrayList<>();
    List<String> outputColumnNames = new ArrayList<>();
    List<ColumnInfo> newColumns = new ArrayList<>();

    for (Entry<String, ExprNodeDesc> e : conf.getColumnExprMap().entrySet()) {

      String colName = e.getKey();
      ExprNodeDesc expr = e.getValue();

      ExprNodeDesc colRef = new ExprNodeColumnDesc(expr.getTypeInfo(), colName, colName, false);

      colList.add(colRef);
      String newColName = extractColumnName(expr);
      outputColumnNames.add(newColName);
      ColumnInfo newColInfo = new ColumnInfo(p.getSchema().getColumnInfo(colName));
      newColInfo.setInternalName(newColName);
      newColumns.add(newColInfo);
    }
    SelectDesc selConf = new SelectDesc(colList, outputColumnNames);
    Operator<SelectDesc> newSEL =
        OperatorFactory.getAndMakeChild(p.getCompilationOpContext(), selConf, new ArrayList<>());

    newSEL.setSchema(new RowSchema(newColumns));

    return newSEL;
  }

  private String extractColumnName(ExprNodeDesc expr) {
    if (expr instanceof ExprNodeColumnDesc) {
      ExprNodeColumnDesc exprNodeColumnDesc = (ExprNodeColumnDesc) expr;
      return exprNodeColumnDesc.getColumn();

    }
    if (expr instanceof ExprNodeConstantDesc) {
      ExprNodeConstantDesc exprNodeConstantDesc = (ExprNodeConstantDesc) expr;
      return exprNodeConstantDesc.getFoldedFromCol();
    }
    throw new RuntimeException("unexpected mapping expression!");
  }
}
