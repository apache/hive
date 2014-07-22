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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.optimizer.correlation.CorrelationOptimizer;
import org.apache.hadoop.hive.ql.optimizer.correlation.ReduceSinkDeDuplication;
import org.apache.hadoop.hive.ql.optimizer.index.RewriteGBUsingIndex;
import org.apache.hadoop.hive.ql.optimizer.lineage.Generator;
import org.apache.hadoop.hive.ql.optimizer.listbucketingpruner.ListBucketingPruner;
import org.apache.hadoop.hive.ql.optimizer.metainfo.annotation.AnnotateWithOpTraits;
import org.apache.hadoop.hive.ql.optimizer.pcr.PartitionConditionRemover;
import org.apache.hadoop.hive.ql.optimizer.ppr.PartitionPruner;
import org.apache.hadoop.hive.ql.optimizer.stats.annotation.AnnotateWithStatistics;
import org.apache.hadoop.hive.ql.optimizer.unionproc.UnionProcessor;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.ppd.PredicatePushDown;
import org.apache.hadoop.hive.ql.ppd.PredicateTransitivePropagate;

/**
 * Implementation of the optimizer.
 */
public class Optimizer {
  private ParseContext pctx;
  private List<Transform> transformations;

  /**
   * Create the list of transformations.
   *
   * @param hiveConf
   */
  public void initialize(HiveConf hiveConf) {
    transformations = new ArrayList<Transform>();
    // Add the transformation that computes the lineage information.
    transformations.add(new Generator());
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTPPD)) {
      transformations.add(new PredicateTransitivePropagate());
      transformations.add(new PredicatePushDown());
      transformations.add(new PartitionPruner());
      transformations.add(new PartitionConditionRemover());
      if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTLISTBUCKETING)) {
        /* Add list bucketing pruner. */
        transformations.add(new ListBucketingPruner());
      }
    }
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTGROUPBY) ||
        HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_MAP_GROUPBY_SORT)) {
      transformations.add(new GroupByOptimizer());
    }
    transformations.add(new ColumnPruner());
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPTIMIZE_SKEWJOIN_COMPILETIME)) {
      transformations.add(new SkewJoinOptimizer());
    }
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTGBYUSINGINDEX)) {
      transformations.add(new RewriteGBUsingIndex());
    }
    transformations.add(new SamplePruner());
    transformations.add(new MapJoinProcessor());
    boolean bucketMapJoinOptimizer = false;
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTBUCKETMAPJOIN)) {
      transformations.add(new BucketMapJoinOptimizer());
      bucketMapJoinOptimizer = true;
    }

    // If optimize hive.optimize.bucketmapjoin.sortedmerge is set, add both
    // BucketMapJoinOptimizer and SortedMergeBucketMapJoinOptimizer
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTSORTMERGEBUCKETMAPJOIN)) {
      if (!bucketMapJoinOptimizer) {
        // No need to add BucketMapJoinOptimizer twice
        transformations.add(new BucketMapJoinOptimizer());
      }
      transformations.add(new SortedMergeBucketMapJoinOptimizer());
    }

    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTIMIZEBUCKETINGSORTING)) {
      transformations.add(new BucketingSortingReduceSinkOptimizer());
    }

    transformations.add(new UnionProcessor());
    transformations.add(new JoinReorder());
    if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.DYNAMICPARTITIONING) &&
        HiveConf.getVar(hiveConf, HiveConf.ConfVars.DYNAMICPARTITIONINGMODE).equals("nonstrict") &&
        HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTSORTDYNAMICPARTITION) &&
        !HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTLISTBUCKETING)) {
      transformations.add(new SortedDynPartitionOptimizer());
    }
    if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTREDUCEDEDUPLICATION)) {
      transformations.add(new ReduceSinkDeDuplication());
    }
    transformations.add(new NonBlockingOpDeDupProc());
    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVELIMITOPTENABLE)) {
      transformations.add(new GlobalLimitOptimizer());
    }
    if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTCORRELATION) &&
        !HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEGROUPBYSKEW) &&
        !HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVE_OPTIMIZE_SKEWJOIN_COMPILETIME)) {
      transformations.add(new CorrelationOptimizer());
    }
    if (HiveConf.getFloatVar(hiveConf, HiveConf.ConfVars.HIVELIMITPUSHDOWNMEMORYUSAGE) > 0) {
      transformations.add(new LimitPushdownOptimizer());
    }
    if(HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEOPTIMIZEMETADATAQUERIES)) {
      transformations.add(new StatsOptimizer());
    }
    if (pctx.getContext().getExplain() ||
        HiveConf.getVar(hiveConf, HiveConf.ConfVars.HIVE_EXECUTION_ENGINE).equals("tez")) {
      transformations.add(new AnnotateWithStatistics());
      transformations.add(new AnnotateWithOpTraits());
    }

    transformations.add(new SimpleFetchOptimizer());  // must be called last

    if (HiveConf.getBoolVar(hiveConf, HiveConf.ConfVars.HIVEFETCHTASKAGGR)) {
      transformations.add(new SimpleFetchAggregation());
    }
  }

  /**
   * Invoke all the transformations one-by-one, and alter the query plan.
   *
   * @return ParseContext
   * @throws SemanticException
   */
  public ParseContext optimize() throws SemanticException {
    for (Transform t : transformations) {
        pctx = t.transform(pctx);
    }
    return pctx;
  }

  /**
   * @return the pctx
   */
  public ParseContext getPctx() {
    return pctx;
  }

  /**
   * @param pctx
   *          the pctx to set
   */
  public void setPctx(ParseContext pctx) {
    this.pctx = pctx;
  }

}
