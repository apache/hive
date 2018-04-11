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

package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.List;
import java.util.Stack;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.hive.ql.exec.AppMasterEventOperator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.plan.DynamicPruningEventDesc;

/**
 * FileSinkProcessor is a simple rule to remember seen unions for later
 * processing.
 *
 */
public class AppMasterEventProcessor implements NodeProcessor {

  static final private Logger LOG = LoggerFactory.getLogger(AppMasterEventProcessor.class.getName());

  @Override
  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
      throws SemanticException {
    GenTezProcContext context = (GenTezProcContext) procCtx;
    AppMasterEventOperator event = (AppMasterEventOperator) nd;
    DynamicPruningEventDesc desc = (DynamicPruningEventDesc) event.getConf();

    // simply need to remember that we've seen an event operator.
    context.eventOperatorSet.add(event);

    // and remember link between event and table scan
    List<AppMasterEventOperator> events;
    if (context.tsToEventMap.containsKey(desc.getTableScan())) {
      events = context.tsToEventMap.get(desc.getTableScan());
    } else {
      events = new ArrayList<AppMasterEventOperator>();
    }
    events.add(event);
    context.tsToEventMap.put(desc.getTableScan(), events);
    return true;
  }
}
