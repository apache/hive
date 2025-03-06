/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.optimizer;

import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;

public class BucketMapjoinProc extends AbstractBucketJoinProc implements SemanticNodeProcessor {
  public BucketMapjoinProc(ParseContext pGraphContext) {
    super(pGraphContext);
  }

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
      Object... nodeOutputs) throws SemanticException {
    BucketJoinProcCtx context = (BucketJoinProcCtx) procCtx;
    MapJoinOperator mapJoinOperator = (MapJoinOperator) nd;

    // can the mapjoin present be converted to a bucketed mapjoin
    boolean convert = canConvertMapJoinToBucketMapJoin(
        mapJoinOperator, context);
    HiveConf conf = context.getConf();

    // Throw an error if the user asked for bucketed mapjoin to be enforced and
    // bucketed mapjoin cannot be performed
    if (!convert && conf.getBoolVar(HiveConf.ConfVars.HIVE_ENFORCE_BUCKET_MAPJOIN)) {
      throw new SemanticException(ErrorMsg.BUCKET_MAPJOIN_NOT_POSSIBLE.getMsg());
    }

    if (convert) {
      // convert the mapjoin to a bucketized mapjoin
      convertMapJoinToBucketMapJoin(mapJoinOperator, context);
    }

    return null;
  }

  /**
   * Check if a mapjoin can be converted to a bucket mapjoin,
   * and do the version if possible.
   */
  public static void checkAndConvertBucketMapJoin(ParseContext pGraphContext,
      MapJoinOperator mapJoinOp, String baseBigAlias,
      List<String> joinAliases) throws SemanticException {
    BucketJoinProcCtx ctx = new BucketJoinProcCtx(pGraphContext.getConf());
    BucketMapjoinProc proc = new BucketMapjoinProc(pGraphContext);
    Map<Byte, List<ExprNodeDesc>> keysMap = mapJoinOp.getConf().getKeys();
    if (proc.checkConvertBucketMapJoin(ctx, mapJoinOp.getConf().getAliasToOpInfo(),
        keysMap, baseBigAlias, joinAliases)) {
      proc.convertMapJoinToBucketMapJoin(mapJoinOp, ctx);
    }
  }
}
