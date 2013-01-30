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

package org.apache.hadoop.hive.ql.lib;

import java.util.Stack;

import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Implentation of the Rule interface for Nodes Used in Node dispatching to dispatch
 * process/visitor functions for Nodes.  The cost method returns 1 if there is an exact
 * match between the expression and the stack, otherwise -1.
 */
public class RuleExactMatch implements Rule {

  private final String ruleName;
  private final String pattern;

  /**
   * The rule specified as operator names separated by % symbols, the left side represents the
   * bottom of the stack.
   *
   * E.g. TS%FIL%RS -> means
   * TableScan Node followed by Filter followed by ReduceSink in the tree, or, in terms of the
   * stack, ReduceSink on top followed by Filter followed by TableScan
   *
   * @param ruleName
   *          name of the rule
   * @param regExp
   *          string specification of the rule
   **/
  public RuleExactMatch(String ruleName, String pattern) {
    this.ruleName = ruleName;
    this.pattern = pattern;
  }

  /**
   * This function returns the cost of the rule for the specified stack. Returns 1 if there is
   * an exact match with the entire stack, otherwise -1
   *
   * If any proper substack of the stack matches it will return -1.  It only returns 1 if the
   * entire stack matches the rule exactly.
   *
   * @param stack
   *          Node stack encountered so far
   * @return cost of the function
   * @throws SemanticException
   */
  public int cost(Stack<Node> stack) throws SemanticException {
    int numElems = (stack != null ? stack.size() : 0);
    String name = new String();
    for (int pos = numElems - 1; pos >= 0; pos--) {
      name = stack.get(pos).getName() + "%" + name;
    }

    if (pattern.equals(name)) {
      return 1;
    }

    return -1;
  }

  /**
   * @return the name of the Node
   **/
  public String getName() {
    return ruleName;
  }
}
