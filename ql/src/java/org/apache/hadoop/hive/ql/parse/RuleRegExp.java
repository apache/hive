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

import java.util.Stack;
import java.util.Iterator;
import java.util.regex.Pattern;
import java.util.regex.Matcher;
import java.io.Serializable;

import org.apache.hadoop.hive.ql.exec.Operator;

/**
 * Rule interface for Operators
 * Used in operator dispatching to dispatch process/visitor functions for operators
 */
public class RuleRegExp implements Rule {
  
  private String    ruleName;
  private String    regExp;
  private Pattern   pattern;

  /**
   * The rule specified by the regular expression. Note that, the regular expression is specified in terms of operator
   * name. For eg: TS.*RS -> means TableScan operator followed by anything any number of times followed by ReduceSink
   * @param ruleName name of the rule
   * @param regExp regular expression for the rule
   **/
  public RuleRegExp(String ruleName, String regExp) {
    this.ruleName = ruleName;
    this.regExp   = regExp;
    pattern       = Pattern.compile(regExp);
  }

  /**
   * This function returns the cost of the rule for the specified stack. Lower the cost, the better the rule is matched
   * @param stacl operator stack encountered so far
   * @return cost of the function
   * @throws SemanticException
   */
  public int cost(Stack<Operator<? extends Serializable>> stack) throws SemanticException {
    int numElems = stack.size();
    String name = new String();
    for (int pos = numElems - 1; pos >= 0; pos--) {
      name = stack.get(pos).getOperatorName().concat(name);
      Matcher m = pattern.matcher(name);
      if (m.matches()) 
        return m.group().length();
    }

    return -1;
  }

  /**
   * @return the name of the operator
   **/
  public String getName() {
    return ruleName;
  }
}
