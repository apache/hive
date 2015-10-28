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

package org.apache.hadoop.hive.ql.parse.spark;

import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * FileSinkProcessor handles addition of merge, move and stats tasks for filesinks.
 * Cloned from tez's FileSinkProcessor.
 */
public class SparkFileSinkProcessor implements NodeProcessor {
  private static final Logger LOGGER = LoggerFactory.getLogger(SparkFileSinkProcessor.class.getName());

  /*
   * (non-Javadoc)
   * we should ideally not modify the tree we traverse.
   * However, since we need to walk the tree at any time when we modify the operator,
   * we might as well do it here.
   */
  @Override
  public Object process(Node nd, Stack<Node> stack,
      NodeProcessorCtx procCtx, Object... nodeOutputs)
      throws SemanticException {

    GenSparkProcContext context = (GenSparkProcContext) procCtx;
    FileSinkOperator fileSink = (FileSinkOperator) nd;

    // just remember it for later processing
    context.fileSinkSet.add(fileSink);
    return true;
  }

}
