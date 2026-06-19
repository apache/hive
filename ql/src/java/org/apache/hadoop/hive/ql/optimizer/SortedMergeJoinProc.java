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

import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.JoinOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.ParseContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;

public class SortedMergeJoinProc extends AbstractSMBJoinProc implements SemanticNodeProcessor {

  public SortedMergeJoinProc(ParseContext pctx) {
    super(pctx);
  }

  public SortedMergeJoinProc() {
  }

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx,
      Object... nodeOutputs) throws SemanticException {

    JoinOperator joinOp = (JoinOperator) nd;
    SortBucketJoinProcCtx smbJoinContext = (SortBucketJoinProcCtx) procCtx;
    boolean convert =
        canConvertJoinToSMBJoin(
            joinOp, smbJoinContext);

    if (convert) {
      convertJoinToSMBJoin(joinOp, smbJoinContext);
    }
    return null;
  }
}
