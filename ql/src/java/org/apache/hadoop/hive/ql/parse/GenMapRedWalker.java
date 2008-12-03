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

package org.apache.hadoop.hive.ql.parse;

import java.io.Serializable;
import java.util.List;
import java.util.Stack;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.exec.*;

/**
 * Walks the operator tree in pre order fashion
 */
public class GenMapRedWalker extends DefaultOpGraphWalker {
  private Stack<Operator<? extends Serializable>> opStack;

  /**
   * constructor of the walker - the dispatcher is passed
   * @param disp the dispatcher to be called for each node visited
   */
  public GenMapRedWalker(Dispatcher disp) {
    super(disp);
    opStack = new Stack<Operator<? extends Serializable>>();
  }
  
  /**
   * Walk the given operator
   * @param op operator being walked
   */
  @Override
  public void walk(Operator<? extends Serializable> op) throws SemanticException {
    List<Operator<? extends Serializable>> children = op.getChildOperators();
    
    // maintain the stack of operators encountered
    opStack.push(op);
    dispatch(op, opStack);

    // kids of reduce sink operator need not be traversed again
    if ((children == null) ||
        ((op instanceof ReduceSinkOperator) && (getDispatchedList().containsAll(children)))) {
      opStack.pop();
      return;
    }

    // move all the children to the front of queue
    for (Operator<? extends Serializable> ch : children)
      walk(ch);

    // done with this operator
    opStack.pop();
  }
}
