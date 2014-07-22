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

package org.apache.hadoop.hive.ql.optimizer.physical;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * A hierarchy physical optimizer, which contains a list of
 * PhysicalPlanResolver. Each resolver has its own set of optimization rule.
 */
public class PhysicalOptimizer {
  private PhysicalContext pctx;
  private List<PhysicalPlanResolver> resolvers;

  public PhysicalOptimizer(PhysicalContext pctx, HiveConf hiveConf) {
    super();
    this.pctx = pctx;
    initialize(hiveConf);
  }

  /**
   * create the list of physical plan resolvers.
   *
   * @param hiveConf
   */
  private void initialize(HiveConf hiveConf) {
    resolvers = new ArrayList<PhysicalPlanResolver>();
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVESKEWJOIN)) {
      resolvers.add(new SkewJoinResolver());
    }
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVECONVERTJOIN)) {
      resolvers.add(new CommonJoinResolver());

      // The joins have been automatically converted to map-joins.
      // However, if the joins were converted to sort-merge joins automatically,
      // they should also be tried as map-joins.
      if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_AUTO_SORTMERGE_JOIN_TOMAPJOIN)) {
        resolvers.add(new SortMergeJoinResolver());
      }
    }

    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVEOPTINDEXFILTER)) {
      resolvers.add(new IndexWhereResolver());
    }
    resolvers.add(new MapJoinResolver());
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVEMETADATAONLYQUERIES)) {
      resolvers.add(new MetadataOnlyOptimizer());
    }
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVENULLSCANOPTIMIZE)) {
      resolvers.add(new NullScanOptimizer());
    }
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVESAMPLINGFORORDERBY)) {
      resolvers.add(new SamplingOptimizer());
    }

    // Physical optimizers which follow this need to be careful not to invalidate the inferences
    // made by this optimizer. Only optimizers which depend on the results of this one should
    // follow it.
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_INFER_BUCKET_SORT)) {
      resolvers.add(new BucketingSortingInferenceOptimizer());
    }

    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_CHECK_CROSS_PRODUCT)) {
      resolvers.add(new CrossProductCheck());
    }

    // Vectorization should be the last optimization, because it doesn't modify the plan
    // or any operators. It makes a very low level transformation to the expressions to
    // run in the vectorized mode.
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVE_VECTORIZATION_ENABLED)) {
      resolvers.add(new Vectorizer());
    }
    if (!"none".equalsIgnoreCase(hiveConf.getVar(HiveConf.ConfVars.HIVESTAGEIDREARRANGE))) {
      resolvers.add(new StageIDsRearranger());
    }
  }

  /**
   * invoke all the resolvers one-by-one, and alter the physical plan.
   *
   * @return PhysicalContext
   * @throws HiveException
   */
  public PhysicalContext optimize() throws SemanticException {
    for (PhysicalPlanResolver r : resolvers) {
      pctx = r.resolve(pctx);
    }
    return pctx;
  }
}
