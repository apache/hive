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
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.OptimizeTezProcContext;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.AppMasterEventDesc;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;

/**
 * If we expect the number of keys for dynamic pruning to be too large we
 * disable it.
 */
public class RemoveDynamicPruningBySize implements NodeProcessor {

  static final private Log LOG = LogFactory.getLog(RemoveDynamicPruningBySize.class.getName());

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procContext,
      Object... nodeOutputs)
      throws SemanticException {

    OptimizeTezProcContext context = (OptimizeTezProcContext) procContext;

    AppMasterEventOperator event = (AppMasterEventOperator) nd;
    AppMasterEventDesc desc = event.getConf();

    if (desc.getStatistics().getDataSize() > context.conf
        .getLongVar(ConfVars.TEZ_DYNAMIC_PARTITION_PRUNING_MAX_DATA_SIZE)) {
      Operator<?> child = event;
      Operator<?> curr = event;

      while (curr.getChildOperators().size() <= 1) {
        child = curr;
        curr = curr.getParentOperators().get(0);
      }
      // at this point we've found the fork in the op pipeline that has the
      // pruning as a child plan.
      LOG.info("Disabling dynamic pruning for: "
          + ((DynamicPruningEventDesc) desc).getTableScan().getName()
          + ". Expected data size is too big: " + desc.getStatistics().getDataSize());
      curr.removeChild(child);
    }
    return false;
  }
}
