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
import java.util.Stack;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.MapJoinOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.optimizer.AbstractSMBJoinProc;
import org.apache.hadoop.hive.ql.optimizer.SortBucketJoinProcCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.SMBJoinDesc;

import com.clearspring.analytics.util.Preconditions;

/**
 * Converts from a bucket-mapjoin created from hints to SMB mapjoin.
 */
public class SparkSMBJoinHintOptimizer extends AbstractSMBJoinProc implements NodeProcessor {

  public SparkSMBJoinHintOptimizer(ParseContext pctx) {
    super(pctx);
  }

  public SparkSMBJoinHintOptimizer() {
  }

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
                        Object... nodeOutputs) throws SemanticException {
    MapJoinOperator mapJoinOp = (MapJoinOperator) nd;
    SortBucketJoinProcCtx smbJoinContext = (SortBucketJoinProcCtx) procCtx;

    boolean convert =
      canConvertBucketMapJoinToSMBJoin(mapJoinOp, stack, smbJoinContext, nodeOutputs);

    // Throw an error if the user asked for sort merge bucketed mapjoin to be enforced
    // and sort merge bucketed mapjoin cannot be performed
    if (!convert
      && pGraphContext.getConf().getBoolVar(
        HiveConf.ConfVars.HIVEENFORCESORTMERGEBUCKETMAPJOIN)) {
      throw new SemanticException(ErrorMsg.SORTMERGE_MAPJOIN_FAILED.getMsg());
    }

    if (convert) {
      removeSmallTableReduceSink(mapJoinOp);
      convertBucketMapJoinToSMBJoin(mapJoinOp, smbJoinContext);
    }
    return null;
  }

  /**
   * In bucket mapjoin, there are ReduceSinks that mark a small table parent (Reduce Sink are removed from big-table).
   * In SMB join these are not expected for any parents, either from small or big tables.
   * @param mapJoinOp
   */
  @SuppressWarnings("unchecked")
  private void removeSmallTableReduceSink(MapJoinOperator mapJoinOp) {
    SMBJoinDesc smbJoinDesc = new SMBJoinDesc(mapJoinOp.getConf());
    List<Operator<? extends OperatorDesc>> parentOperators = mapJoinOp.getParentOperators();
    for (int i = 0; i < parentOperators.size(); i++) {
      Operator<? extends OperatorDesc> par = parentOperators.get(i);
      if (i != smbJoinDesc.getPosBigTable()) {
        if (par instanceof ReduceSinkOperator) {
          List<Operator<? extends OperatorDesc>> grandParents = par.getParentOperators();
          Preconditions.checkArgument(grandParents.size() == 1,
            "AssertionError: expect # of parents to be 1, but was " + grandParents.size());
          Operator<? extends OperatorDesc> grandParent = grandParents.get(0);
          grandParent.removeChild(par);
          grandParent.setChildOperators(Utilities.makeList(mapJoinOp));
          mapJoinOp.getParentOperators().set(i, grandParent);
        }
      }
    }
  }
}
