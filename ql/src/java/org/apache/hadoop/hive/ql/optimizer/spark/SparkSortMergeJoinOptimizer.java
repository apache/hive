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
package org.apache.hadoop.hive.ql.optimizer.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.SMBMapJoinOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.AbstractSMBJoinProc;
import org.apache.hadoop.hive.ql.optimizer.SortBucketJoinProcCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.spark.OptimizeSparkProcContext;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

import java.util.Stack;

/**
 * Converts a common join operator to an SMB join if eligible.  Handles auto SMB conversion.
 */
public class SparkSortMergeJoinOptimizer extends AbstractSMBJoinProc implements NodeProcessor {

  public SparkSortMergeJoinOptimizer(ParseContext pctx) {
      super(pctx);
    }

  public SparkSortMergeJoinOptimizer() {
  }

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                        Object... nodeOutputs) throws SemanticException {

    JoinOperator joinOp = (JoinOperator) nd;
    HiveConf conf = ((OptimizeSparkProcContext) procCtx).getParseContext().getConf();

    if (!conf.getBoolVar(HiveConf.ConfVars.HIVE_AUTO_SORTMERGE_JOIN)) {
      return null;
    }

    SortBucketJoinProcCtx smbJoinContext = new SortBucketJoinProcCtx(conf);

    boolean convert =
            canConvertJoinToSMBJoin(
                    joinOp, smbJoinContext, pGraphContext, stack);

    if (convert) {
      return convertJoinToSMBJoinAndReturn(joinOp, smbJoinContext);
    }
    return null;
  }

  protected boolean canConvertJoinToSMBJoin(JoinOperator joinOperator,
    SortBucketJoinProcCtx smbJoinContext, ParseContext pGraphContext,
    Stack<Node> stack) throws SemanticException {
    if (!supportBucketMapJoin(stack)) {
      return false;
    }
    return canConvertJoinToSMBJoin(joinOperator, smbJoinContext);
  }

  //Preliminary checks.  In the MR version of the code, these used to be done via another walk,
  //here it is done inline.
  private boolean supportBucketMapJoin(Stack<Node> stack) {
    int size = stack.size();
    if (!(stack.get(size - 1) instanceof JoinOperator)
      || !(stack.get(size - 2) instanceof ReduceSinkOperator)) {
      return false;
    }

    // If any operator in the stack does not support a auto-conversion, this join should
    // not be converted.
    for (int pos = size - 3; pos >= 0; pos--) {
      @SuppressWarnings("unchecked")
      Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) stack.get(pos);
      if (!op.supportAutomaticSortMergeJoin()) {
        return false;
      }
    }
    return true;
  }

  protected SMBMapJoinOperator convertJoinToSMBJoinAndReturn(
          JoinOperator joinOp,
          SortBucketJoinProcCtx smbJoinContext) throws SemanticException {
    MapJoinOperator mapJoinOp = convertJoinToBucketMapJoin(joinOp, smbJoinContext);
    SMBMapJoinOperator smbMapJoinOp =
            convertBucketMapJoinToSMBJoin(mapJoinOp, smbJoinContext);
    smbMapJoinOp.setConvertedAutomaticallySMBJoin(true);
    return smbMapJoinOp;
  }
}
