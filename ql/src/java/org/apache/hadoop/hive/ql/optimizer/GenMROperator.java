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

import java.io.Serializable;
import java.util.Map;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.OperatorProcessor;
import org.apache.hadoop.hive.ql.optimizer.GenMRProcContext.GenMapRedCtx;

/**
 * Processor for the rule - no specific rule fired
 */
public class GenMROperator implements OperatorProcessor {

  public GenMROperator() {
  }

  /**
   * Reduce Scan encountered 
   * @param op the reduce sink operator encountered
   * @param opProcCtx context
   */
  public void process(Operator<? extends Serializable> op, OperatorProcessorContext opProcCtx) throws SemanticException {
    GenMRProcContext ctx = (GenMRProcContext)opProcCtx;

    Map<Operator<? extends Serializable>, GenMapRedCtx> mapCurrCtx = ctx.getMapCurrCtx();
    mapCurrCtx.put(op, new GenMapRedCtx(ctx.getCurrTask(), ctx.getCurrTopOp(), ctx.getCurrAliasId()));
  }
}
