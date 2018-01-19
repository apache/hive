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

import java.io.PrintStream;
import java.util.HashMap;
import java.util.Stack;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessor;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;

/**
 * PrintOpTreeProcessor.
 *
 */
public class PrintOpTreeProcessor implements NodeProcessor {

  private final PrintStream out;
  private final HashMap<Operator<? extends OperatorDesc>, Integer> opMap =
    new HashMap<Operator<? extends OperatorDesc>, Integer>();
  private Integer curNum = 0;

  public PrintOpTreeProcessor() {
    out = System.out;
  }

  public PrintOpTreeProcessor(PrintStream o) {
    out = o;
  }

  private String getParents(Operator<? extends OperatorDesc> op) {
    StringBuilder ret = new StringBuilder("[");
    boolean first = true;
    if (op.getParentOperators() != null) {
      for (Operator<? extends OperatorDesc> parent : op.getParentOperators()) {
        if (!first) {
          ret.append(",");
        }
        ret.append(opMap.get(parent));
        first = false;
      }
    }
    ret.append("]");
    return ret.toString();
  }

  private String getChildren(Operator<? extends OperatorDesc> op) {
    StringBuilder ret = new StringBuilder("[");
    boolean first = true;
    if (op.getChildOperators() != null) {
      for (Operator<? extends OperatorDesc> child : op.getChildOperators()) {
        if (!first) {
          ret.append(",");
        }
        ret.append(opMap.get(child));
        first = false;
      }
    }
    ret.append("]");
    return ret.toString();
  }

  public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx ctx,
      Object... nodeOutputs) throws SemanticException {
    Operator<? extends OperatorDesc> op = (Operator<? extends OperatorDesc>) nd;
    if (opMap.get(op) == null) {
      opMap.put(op, curNum++);
    }
    out.println("[" + opMap.get(op) + "] " + op.getClass().getName() + " =p=> "
        + getParents(op) + " =c=> " + getChildren(op));
    if (op.getConf() == null) {
      return null;
    }
    return null;
  }
}
