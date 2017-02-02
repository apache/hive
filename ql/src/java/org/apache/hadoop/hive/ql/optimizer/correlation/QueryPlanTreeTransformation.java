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

package org.apache.hadoop.hive.ql.optimizer.correlation;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.DemuxOperator;
import org.apache.hadoop.hive.ql.exec.GroupByOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorFactory;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.optimizer.correlation.CorrelationOptimizer.CorrelationNodeProcCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.DemuxDesc;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.plan.MuxDesc;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;

/**
 * QueryPlanTreeTransformation contains static methods used to transform
 * the query plan tree (operator tree) based on the correlation we have
 * detected by Correlation Optimizer.
 */
public class QueryPlanTreeTransformation {
  private static final Logger LOG = LoggerFactory.getLogger(QueryPlanTreeTransformation.class.getName());

  private static void setNewTag(IntraQueryCorrelation correlation,
      List<Operator<? extends OperatorDesc>> childrenOfDemux,
      ReduceSinkOperator rsop, Map<ReduceSinkOperator, Integer> bottomRSToNewTag)
          throws SemanticException {
    int newTag = bottomRSToNewTag.get(rsop);
    int oldTag = rsop.getConf().getTag();
    if (oldTag == -1) {
      // if this child of DemuxOperator does not use tag, we just set the oldTag to 0.
      oldTag = 0;
    }
    Operator<? extends OperatorDesc> child = CorrelationUtilities.getSingleChild(rsop, true);
    if (!childrenOfDemux.contains(child)) {
      childrenOfDemux.add(child);
    }
    int childIndex = childrenOfDemux.indexOf(child);
    correlation.setNewTag(newTag, oldTag, childIndex);
    rsop.getConf().setTag(newTag);
  }

  /**
   * Based on the correlation, we transform the query plan tree (operator tree).
   * In here, we first create DemuxOperator and all bottom ReduceSinkOperators
   * (bottom means near TableScanOperaotr) in the correlation will be be
   * the parents of the DemuxOperaotr. We also reassign tags to those
   * ReduceSinkOperators. Then, we use MuxOperators to replace ReduceSinkOperators
   * which are not bottom ones in this correlation.
   * Example: The original operator tree is ...
   *      JOIN2
   *      /    \
   *     RS4   RS5
   *    /        \
   *   GBY1     JOIN1
   *    |       /    \
   *   RS1     RS2   RS3
   * If GBY1, JOIN1, and JOIN2 can be executed in the same reducer
   * (optimized by Correlation Optimizer).
   * The new operator tree will be ...
   *      JOIN2
   *        |
   *       MUX
   *      /   \
   *    GBY1  JOIN1
   *      \    /
   *       DEMUX
   *      /  |  \
   *     /   |   \
   *    /    |    \
   *   RS1   RS2   RS3
   * @param pCtx
   * @param corrCtx
   * @param correlation
   * @throws SemanticException
   */
  protected static void applyCorrelation(
      ParseContext pCtx,
      CorrelationNodeProcCtx corrCtx,
      IntraQueryCorrelation correlation)
      throws SemanticException {

    final List<ReduceSinkOperator> bottomReduceSinkOperators =
        correlation.getBottomReduceSinkOperators();
    final int numReducers = correlation.getNumReducers();
    List<Operator<? extends OperatorDesc>> childrenOfDemux =
        new ArrayList<Operator<? extends OperatorDesc>>();
    List<Operator<? extends OperatorDesc>> parentRSsOfDemux =
        new ArrayList<Operator<? extends OperatorDesc>>();
    Map<Integer, Integer> childIndexToOriginalNumParents =
        new HashMap<Integer, Integer>();
    List<TableDesc> keysSerializeInfos = new ArrayList<TableDesc>();
    List<TableDesc> valuessSerializeInfos = new ArrayList<TableDesc>();
    Map<ReduceSinkOperator, Integer> bottomRSToNewTag =
        new HashMap<ReduceSinkOperator, Integer>();
    int newTag = 0;
    CompilationOpContext opCtx = null;
    for (ReduceSinkOperator rsop: bottomReduceSinkOperators) {
      if (opCtx == null) {
        opCtx = rsop.getCompilationOpContext();
      }
      rsop.getConf().setNumReducers(numReducers);
      bottomRSToNewTag.put(rsop, newTag);
      parentRSsOfDemux.add(rsop);
      keysSerializeInfos.add(rsop.getConf().getKeySerializeInfo());
      valuessSerializeInfos.add(rsop.getConf().getValueSerializeInfo());
      Operator<? extends OperatorDesc> child = CorrelationUtilities.getSingleChild(rsop, true);
      if (!childrenOfDemux.contains(child)) {
        childrenOfDemux.add(child);
        int childIndex = childrenOfDemux.size() - 1;
        childIndexToOriginalNumParents.put(childIndex, child.getNumParent());
      }
      newTag++;
    }

    for (ReduceSinkOperator rsop: bottomReduceSinkOperators) {
      setNewTag(correlation, childrenOfDemux, rsop, bottomRSToNewTag);
    }

    // Create the DemuxOperaotr
    DemuxDesc demuxDesc =
        new DemuxDesc(
            correlation.getNewTagToOldTag(),
            correlation.getNewTagToChildIndex(),
            childIndexToOriginalNumParents,
            keysSerializeInfos,
            valuessSerializeInfos);
    Operator<? extends OperatorDesc> demuxOp = OperatorFactory.get(opCtx, demuxDesc);
    demuxOp.setChildOperators(childrenOfDemux);
    demuxOp.setParentOperators(parentRSsOfDemux);
    for (Operator<? extends OperatorDesc> child: childrenOfDemux) {
      List<Operator<? extends OperatorDesc>> parentsWithMultipleDemux =
          new ArrayList<Operator<? extends OperatorDesc>>();
      boolean hasBottomReduceSinkOperators = false;
      boolean hasNonBottomReduceSinkOperators = false;
      for (int i = 0; i < child.getParentOperators().size(); i++) {
        Operator<? extends OperatorDesc> p = child.getParentOperators().get(i);
        assert p instanceof ReduceSinkOperator;
        ReduceSinkOperator rsop = (ReduceSinkOperator)p;
        if (bottomReduceSinkOperators.contains(rsop)) {
          hasBottomReduceSinkOperators = true;
          parentsWithMultipleDemux.add(demuxOp);
        } else {
          hasNonBottomReduceSinkOperators = true;
          parentsWithMultipleDemux.add(rsop);
        }
      }
      if (hasBottomReduceSinkOperators && hasNonBottomReduceSinkOperators) {
        child.setParentOperators(parentsWithMultipleDemux);
      } else {
        child.setParentOperators(Utilities.makeList(demuxOp));
      }
    }
    for (Operator<? extends OperatorDesc> parent: parentRSsOfDemux) {
      parent.setChildOperators(Utilities.makeList(demuxOp));
    }

    // replace all ReduceSinkOperators which are not at the bottom of
    // this correlation to MuxOperators
    Set<ReduceSinkOperator> handledRSs = new HashSet<ReduceSinkOperator>();
    for (ReduceSinkOperator rsop : correlation.getAllReduceSinkOperators()) {
      if (!bottomReduceSinkOperators.contains(rsop)) {
        if (handledRSs.contains(rsop)) {
          continue;
        }
        Operator<? extends OperatorDesc> childOP =
            CorrelationUtilities.getSingleChild(rsop, true);
        if (childOP instanceof GroupByOperator) {
          CorrelationUtilities.removeReduceSinkForGroupBy(
              rsop, (GroupByOperator)childOP, pCtx, corrCtx);
          List<Operator<? extends OperatorDesc>> parentsOfMux =
              new ArrayList<Operator<? extends OperatorDesc>>();
          Operator<? extends OperatorDesc> parentOp =
              CorrelationUtilities.getSingleParent(childOP, true);
          parentsOfMux.add(parentOp);
          Operator<? extends OperatorDesc> mux = OperatorFactory.get(
              childOP.getCompilationOpContext(), new MuxDesc(parentsOfMux));
          mux.setChildOperators(Utilities.makeList(childOP));
          mux.setParentOperators(parentsOfMux);
          childOP.setParentOperators(Utilities.makeList(mux));
          parentOp.setChildOperators(Utilities.makeList(mux));
        } else {
          List<Operator<? extends OperatorDesc>> parentsOfMux =
              new ArrayList<Operator<? extends OperatorDesc>>();
          List<Operator<? extends OperatorDesc>> siblingOPs =
              CorrelationUtilities.findSiblingOperators(rsop);
          for (Operator<? extends OperatorDesc> op: siblingOPs) {
            if (op instanceof DemuxOperator) {
              parentsOfMux.add(op);
            } else if (op instanceof ReduceSinkOperator){
              GroupByOperator pGBYm =
                  CorrelationUtilities.getSingleParent(op, GroupByOperator.class);
              if (pGBYm != null && pGBYm.getConf().getMode() == GroupByDesc.Mode.HASH) {
                // We get a semi join at here.
                // This map-side GroupByOperator needs to be removed
                CorrelationUtilities.removeOperator(
                    pGBYm, op, CorrelationUtilities.getSingleParent(pGBYm, true), pCtx);
              }
              handledRSs.add((ReduceSinkOperator)op);
              parentsOfMux.add(CorrelationUtilities.getSingleParent(op, true));
            } else {
              throw new SemanticException("A sibling of ReduceSinkOperator is neither a " +
                  "DemuxOperator nor a ReduceSinkOperator");
            }
          }
          MuxDesc muxDesc = new MuxDesc(siblingOPs);
          Operator<? extends OperatorDesc> mux = OperatorFactory.get(
              rsop.getCompilationOpContext(), muxDesc);
          mux.setChildOperators(Utilities.makeList(childOP));
          mux.setParentOperators(parentsOfMux);

          for (Operator<? extends OperatorDesc> op: parentsOfMux) {
            if (op instanceof DemuxOperator) {
              // op is a DemuxOperator and it directly connects to childOP.
              // We will add this MuxOperator between DemuxOperator
              // and childOP.
              if (op.getChildOperators().contains(childOP)) {
                op.replaceChild(childOP, mux);
              }
            } else {
              // op is not a DemuxOperator, so it should have
              // a single child.
              op.setChildOperators(Utilities.makeList(mux));
            }
          }
          childOP.setParentOperators(Utilities.makeList(mux));
        }
      }
    }
    for (ReduceSinkOperator rsop: handledRSs) {
      rsop.setChildOperators(null);
      rsop.setParentOperators(null);
    }
  }
}
