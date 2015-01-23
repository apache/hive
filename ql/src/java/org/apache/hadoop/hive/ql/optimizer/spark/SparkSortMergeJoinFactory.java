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
package org.apache.hadoop.hive.ql.optimizer.spark;

import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.GenSparkProcContext;
import org.apache.hadoop.hive.ql.plan.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
* Operator factory for Spark SMBJoin processing.
*/
public final class SparkSortMergeJoinFactory {

  private SparkSortMergeJoinFactory() {
    // prevent instantiation
  }

  /**
   * Get the branch on which we are invoked (walking) from.  See diagram below.
   * We are at the SMBJoinOp and could have come from TS of any of the input tables.
   */
  public static int getPositionParent(SMBMapJoinOperator op,
      Stack<Node> stack) {
    int size = stack.size();
    assert size >= 2 && stack.get(size - 1) == op;
    @SuppressWarnings("unchecked")
    Operator<? extends OperatorDesc> parent =
        (Operator<? extends OperatorDesc>) stack.get(size - 2);
    List<Operator<? extends OperatorDesc>> parOp = op.getParentOperators();
    int pos = parOp.indexOf(parent);
    return pos;
  }

  /**
   * SortMergeMapJoin processor, input is a SMBJoinOp that is part of a MapWork:
   *
   *  MapWork:
   *
   *   (Big)   (Small)  (Small)
   *    TS       TS       TS
   *     \       |       /
   *       \     DS     DS
   *         \   |    /
   *          SMBJoinOP
   *
   * 1. Initializes the MapWork's aliasToWork, pointing to big-table's TS.
   * 2. Adds the bucketing information to the MapWork.
   * 3. Adds localwork to the MapWork, with localWork's aliasToWork pointing to small-table's TS.
   */
  private static class SortMergeJoinProcessor implements NodeProcessor {

    public static void setupBucketMapJoinInfo(MapWork plan, SMBMapJoinOperator currMapJoinOp) {
      if (currMapJoinOp != null) {
        Map<String, Map<String, List<String>>> aliasBucketFileNameMapping =
            currMapJoinOp.getConf().getAliasBucketFileNameMapping();
        if (aliasBucketFileNameMapping != null) {
          MapredLocalWork localPlan = plan.getMapRedLocalWork();
          if (localPlan == null) {
            localPlan = currMapJoinOp.getConf().getLocalWork();
          } else {
            // local plan is not null, we want to merge it into SMBMapJoinOperator's local work
            MapredLocalWork smbLocalWork = currMapJoinOp.getConf().getLocalWork();
            if (smbLocalWork != null) {
              localPlan.getAliasToFetchWork().putAll(smbLocalWork.getAliasToFetchWork());
              localPlan.getAliasToWork().putAll(smbLocalWork.getAliasToWork());
            }
          }

          if (localPlan == null) {
            return;
          }
          plan.setMapRedLocalWork(null);
          currMapJoinOp.getConf().setLocalWork(localPlan);

          BucketMapJoinContext bucketMJCxt = new BucketMapJoinContext();
          localPlan.setBucketMapjoinContext(bucketMJCxt);
          bucketMJCxt.setAliasBucketFileNameMapping(aliasBucketFileNameMapping);
          bucketMJCxt.setBucketFileNameMapping(
              currMapJoinOp.getConf().getBigTableBucketNumMapping());
          localPlan.setInputFileChangeSensitive(true);
          bucketMJCxt.setMapJoinBigTableAlias(currMapJoinOp.getConf().getBigTableAlias());
          bucketMJCxt
              .setBucketMatcherClass(org.apache.hadoop.hive.ql.exec.DefaultBucketMatcher.class);
          bucketMJCxt.setBigTablePartSpecToFileMapping(
              currMapJoinOp.getConf().getBigTablePartSpecToFileMapping());

          plan.setUseBucketizedHiveInputFormat(true);

        }
      }
    }

    /**
     * Initialize the mapWork.
     *
     * @param opProcCtx
     *          processing context
     */
    private static void initSMBJoinPlan(MapWork mapWork,
                                        GenSparkProcContext opProcCtx, boolean local)
            throws SemanticException {
      TableScanOperator ts = (TableScanOperator) opProcCtx.currentRootOperator;
      String currAliasId = findAliasId(opProcCtx, ts);
      GenMapRedUtils.setMapWork(mapWork, opProcCtx.parseContext,
         opProcCtx.inputs, null, ts, currAliasId, opProcCtx.conf, local);
    }

    private static String findAliasId(GenSparkProcContext opProcCtx, TableScanOperator ts) {
      for (String alias : opProcCtx.topOps.keySet()) {
        if (opProcCtx.topOps.get(alias) == ts) {
          return alias;
        }
      }
      return null;
    }

    /**
     * 1. Initializes the MapWork's aliasToWork, pointing to big-table's TS.
     * 2. Adds the bucketing information to the MapWork.
     * 3. Adds localwork to the MapWork, with localWork's aliasToWork pointing to small-table's TS.
     */
    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
        Object... nodeOutputs) throws SemanticException {
      SMBMapJoinOperator mapJoin = (SMBMapJoinOperator) nd;
      GenSparkProcContext ctx = (GenSparkProcContext) procCtx;

      // find the branch on which this processor was invoked
      int pos = getPositionParent(mapJoin, stack);
      boolean local = pos != mapJoin.getConf().getPosBigTable();

      MapWork mapWork = ctx.smbJoinWorkMap.get(mapJoin);
      initSMBJoinPlan(mapWork, ctx, local);

      // find the associated mapWork that contains this processor.
      setupBucketMapJoinInfo(mapWork, mapJoin);

      // local aliases need not to hand over context further
      return false;
    }
  }

  public static NodeProcessor getTableScanMapJoin() {
    return new SortMergeJoinProcessor();
  }
}
