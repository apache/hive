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

import java.util.Stack;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.OptimizeTezProcContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceSinkDesc;

/**
 * SetReducerParallelism determines how many reducers should
 * be run for a given reduce sink.
 */
public class SetReducerParallelism implements NodeProcessor {

  static final private Log LOG = LogFactory.getLog(SetReducerParallelism.class.getName());

  @SuppressWarnings("unchecked")
  @Override
  public Object process(Node nd, Stack<Node> stack,
      NodeProcessorCtx procContext, Object... nodeOutputs)
      throws SemanticException {

    OptimizeTezProcContext context = (OptimizeTezProcContext) procContext;

    ReduceSinkOperator sink = (ReduceSinkOperator) nd;
    ReduceSinkDesc desc = sink.getConf();

    long bytesPerReducer = context.conf.getLongVar(HiveConf.ConfVars.BYTESPERREDUCER);
    int maxReducers = context.conf.getIntVar(HiveConf.ConfVars.MAXREDUCERS);
    int constantReducers = context.conf.getIntVar(HiveConf.ConfVars.HADOOPNUMREDUCERS);

    if (context.visitedReduceSinks.contains(sink)) {
      // skip walking the children
      LOG.debug("Already processed reduce sink: " + sink.getName());
      return true;
    }

    context.visitedReduceSinks.add(sink);

    if (desc.getNumReducers() <= 0) {
      if (constantReducers > 0) {
        LOG.info("Parallelism for reduce sink "+sink+" set by user to "+constantReducers);
        desc.setNumReducers(constantReducers);
      } else {
        long numberOfBytes = 0;

        // we need to add up all the estimates from the siblings of this reduce sink
        for (Operator<? extends OperatorDesc> sibling:
          sink.getChildOperators().get(0).getParentOperators()) {
          if (sibling.getStatistics() != null) {
            numberOfBytes += sibling.getStatistics().getDataSize();
          } else {
            LOG.warn("No stats available from: "+sibling);
          }
        }

        int numReducers = Utilities.estimateReducers(numberOfBytes, bytesPerReducer,
            maxReducers, false);
        LOG.info("Set parallelism for reduce sink "+sink+" to: "+numReducers);
        desc.setNumReducers(numReducers);
      }
    } else {
      LOG.info("Number of reducers determined to be: "+desc.getNumReducers());
    }

    return false;
  }

}
