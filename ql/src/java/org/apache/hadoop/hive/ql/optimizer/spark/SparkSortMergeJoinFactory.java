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

import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
import org.apache.hadoop.hive.ql.optimizer.GenMapRedUtils;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.GenSparkProcContext;
import org.apache.hadoop.hive.ql.plan.BucketMapJoinContext;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;

/**
 * Operator factory for Spark SMBJoin processing.
 */
public final class SparkSortMergeJoinFactory {

  private SparkSortMergeJoinFactory() {
    // prevent instantiation
  }

  /**
   * Annotate MapWork, input is a SMBJoinOp that is part of a MapWork, and its root TS operator.
   *
   * 1. Initializes the MapWork's aliasToWork, pointing to big-table's TS.
   * 2. Adds the bucketing information to the MapWork.
   * 3. Adds localwork to the MapWork, with localWork's aliasToWork pointing to small-table's TS.
   * @param context proc walker context
   * @param mapWork mapwork to annotate
   * @param smbMapJoinOp SMB Map Join operator to get data
   * @param ts Table Scan operator to get data
   * @param local Whether ts is from a 'local' source (small-table that will be loaded by SMBJoin 'local' task)
   */
  public static void annotateMapWork(GenSparkProcContext context, MapWork mapWork,
    SMBMapJoinOperator smbMapJoinOp, TableScanOperator ts, boolean local)
    throws SemanticException {
    initSMBJoinPlan(context, mapWork, ts, local);
    setupBucketMapJoinInfo(mapWork, smbMapJoinOp);
  }

  private static void setupBucketMapJoinInfo(MapWork plan, SMBMapJoinOperator currMapJoinOp) {
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

  private static void initSMBJoinPlan(GenSparkProcContext opProcCtx,
    MapWork mapWork, TableScanOperator currentRootOperator, boolean local)
    throws SemanticException {
    String currAliasId = findAliasId(opProcCtx, currentRootOperator);
    GenMapRedUtils.setMapWork(mapWork, opProcCtx.parseContext,
      opProcCtx.inputs, null, currentRootOperator, currAliasId, opProcCtx.conf, local);
  }

  private static String findAliasId(GenSparkProcContext opProcCtx, TableScanOperator ts) {
    for (String alias : opProcCtx.topOps.keySet()) {
      if (opProcCtx.topOps.get(alias) == ts) {
        return alias;
      }
    }
    return null;
  }
}
