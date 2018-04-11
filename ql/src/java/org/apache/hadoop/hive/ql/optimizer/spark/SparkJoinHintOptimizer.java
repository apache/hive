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

package org.apache.hadoop.hive.ql.optimizer.spark;

import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.BucketJoinProcCtx;
import org.apache.hadoop.hive.ql.optimizer.BucketMapjoinProc;
import org.apache.hadoop.hive.ql.optimizer.SortBucketJoinProcCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.OptimizeSparkProcContext;

/**
 * This processes joins in which user specified a hint to identify the small-table.
 * Currently it takes a mapjoin already converted from hints, and converts it further
 * to BucketMapJoin or SMBMapJoin using same small-table identification.
 *
 * The idea is eventually to process even hinted Mapjoin hints here,
 * but due to code complexity in refactoring, that is still in Optimizer.
 */
public class SparkJoinHintOptimizer implements NodeProcessor {

  private BucketMapjoinProc bucketMapJoinOptimizer;
  private SparkSMBJoinHintOptimizer smbMapJoinOptimizer;

  public SparkJoinHintOptimizer(ParseContext parseCtx) {
    bucketMapJoinOptimizer = new BucketMapjoinProc(parseCtx);
    smbMapJoinOptimizer = new SparkSMBJoinHintOptimizer(parseCtx);
  }

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
    Object... nodeOutputs) throws SemanticException {
    OptimizeSparkProcContext context = (OptimizeSparkProcContext) procCtx;
    HiveConf hiveConf = context.getParseContext().getConf();

    // Convert from mapjoin to bucket map join if enabled.
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVEOPTBUCKETMAPJOIN)
      || hiveConf.getBoolVar(HiveConf.ConfVars.HIVEOPTSORTMERGEBUCKETMAPJOIN)) {
      BucketJoinProcCtx bjProcCtx = new BucketJoinProcCtx(hiveConf);
      bucketMapJoinOptimizer.process(nd, stack, bjProcCtx, nodeOutputs);
    }

    // Convert from bucket map join to sort merge bucket map join if enabled.
    if (hiveConf.getBoolVar(HiveConf.ConfVars.HIVEOPTSORTMERGEBUCKETMAPJOIN)) {
      SortBucketJoinProcCtx smbJoinCtx = new SortBucketJoinProcCtx(hiveConf);
      smbMapJoinOptimizer.process(nd, stack, smbJoinCtx, nodeOutputs);
    }
    return null;
  }
}