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

package org.apache.hadoop.hive.ql.lib;

import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Rule interface for Nodes Used in Node dispatching to dispatch process/visitor
 * functions for Nodes.
 */
public class RuleRegExp implements SemanticRule {

  private final String ruleName;
  private final Pattern patternWithWildCardChar;
  private final String patternWithoutWildCardChar;
  private String[] patternORWildChar;
  private static final Set<Character> wildCards = new HashSet<Character>(Arrays.asList(
    '[', '^', '$', '*', ']', '+', '|', '(', '\\', '.', '?', ')', '&'));

  /**
   * The function iterates through the list of wild card characters and sees if
   * this regular expression contains a wild card character.
   *
   * @param pattern
   *          pattern expressed as a regular Expression
   */
  private static boolean patternHasWildCardChar(String pattern) {
    if (pattern == null) {
      return false;
    }
    for (char pc : pattern.toCharArray()) {
      if (wildCards.contains(pc)) {
        return true;
      }
    }
    return false;
  }

  /**
   * The function iterates through the list of wild card characters and sees if
   * this regular expression contains  only the given char as wild card character.
   *
   * @param pattern
   *          pattern expressed as a regular Expression
   * @param wcc
   *          wild card character
   */
  private static boolean patternHasOnlyWildCardChar(String pattern, char wcc) {
    if (pattern == null) {
      return false;
    }
    boolean ret = true;
    boolean hasWildCard = false;
    for (char pc : pattern.toCharArray()) {
      if (wildCards.contains(pc)) {
        hasWildCard = true;
        ret = ret && (pc == wcc);
        if (!ret) {
          return false;
        }
      }
    }
    return ret && hasWildCard;
  }

  /**
   * The rule specified by the regular expression. Note that, the regular
   * expression is specified in terms of Node name. For eg: TS.*RS -&gt; means
   * TableScan Node followed by anything any number of times followed by
   * ReduceSink
   * 
   * @param ruleName
   *          name of the rule
   * @param regExp
   *          regular expression for the rule
   **/
  public RuleRegExp(String ruleName, String regExp) {
    this.ruleName = ruleName;

    if (patternHasWildCardChar(regExp)) {
      if (patternHasOnlyWildCardChar(regExp, '|')) {
          this.patternWithWildCardChar = null;
          this.patternWithoutWildCardChar = null;
          this.patternORWildChar = regExp.split("\\|");
      } else {
        this.patternWithWildCardChar = Pattern.compile(regExp);
        this.patternWithoutWildCardChar = null;
        this.patternORWildChar = null;
      }
    } else {
      this.patternWithWildCardChar = null;
      this.patternWithoutWildCardChar = regExp;
      this.patternORWildChar = null;
    }
  }

  /**
   * This function returns the cost of the rule for the specified stack when the pattern
   * matched for has no wildcard character in it. The function expects patternWithoutWildCardChar
   * to be not null.
   * @param stack
   *          Node stack encountered so far
   * @return cost of the function
   * @throws SemanticException
   */
  private int costPatternWithoutWildCardChar(Stack<Node> stack) throws SemanticException {
    int numElems = (stack != null ? stack.size() : 0);

    // No elements
    if (numElems == 0) {
      return -1;
    }

    int patLen = patternWithoutWildCardChar.length();
    StringBuilder name = new StringBuilder(patLen + numElems);
    for (int pos = numElems - 1; pos >= 0; pos--) {
      String nodeName = stack.get(pos).getName() + "%";
      name.insert(0, nodeName);
      if (name.length() >= patLen) {
        if (patternWithoutWildCardChar.contentEquals(name)) {
          return patLen;
        }
        break;
      }
    }
    return -1;
  }

  /**
   * This function returns the cost of the rule for the specified stack when the pattern
   * matched for has only OR wildcard character in it. The function expects patternORWildChar
   * to be not null.
   * @param stack
   *          Node stack encountered so far
   * @return cost of the function
   * @throws SemanticException
   */
  private int costPatternWithORWildCardChar(Stack<Node> stack) throws SemanticException {
    int numElems = (stack != null ? stack.size() : 0);

    // No elements
    if (numElems == 0) {
      return -1;
    }

    // These DS are used to cache previously created String
    Map<Integer,String> cachedNames = new HashMap<Integer,String>();
    int maxDepth = numElems;
    int maxLength = 0;

    // For every pattern
    for (String pattern : patternORWildChar) {
      int patLen = pattern.length();

      // If the stack has been explored already till that level,
      // obtained cached String
      if (cachedNames.containsKey(patLen)) {
        if (pattern.contentEquals(cachedNames.get(patLen))) {
          return patLen;
        }
      } else if (maxLength >= patLen) {
        // We have already explored the stack deep enough, but
        // we do not have a matching
        continue;
      } else {
        // We are going to build the name
        StringBuilder name = new StringBuilder(patLen + numElems);
        if (maxLength != 0) {
          name.append(cachedNames.get(maxLength));
        }
        for (int pos = maxDepth - 1; pos >= 0; pos--) {
          String nodeName = stack.get(pos).getName() + "%";
          name.insert(0, nodeName);

          // We cache the values
          cachedNames.put(name.length(), name.toString());
          maxLength = name.length();
          maxDepth--;

          if (name.length() >= patLen) {
            if (pattern.contentEquals(name)) {
              return patLen;
            }
            break;
          }
        }
        
      }
    }
    return -1;
  }

  /**
   * This function returns the cost of the rule for the specified stack when the pattern
   * matched for has wildcard character in it. The function expects patternWithWildCardChar
   * to be not null.
   *
   * @param stack
   *          Node stack encountered so far
   * @return cost of the function
   * @throws SemanticException
   */
  private int costPatternWithWildCardChar(Stack<Node> stack) throws SemanticException {
    int numElems = (stack != null ? stack.size() : 0);
    StringBuilder name = new StringBuilder();
    Matcher m = patternWithWildCardChar.matcher("");
    for (int pos = numElems - 1; pos >= 0; pos--) {
      String nodeName = stack.get(pos).getName() + "%";
      name.insert(0, nodeName);
      m.reset(name);
      if (m.matches()) {
        return name.length();
      }
    }
    return -1;
  }

  /**
   * Returns true if the rule pattern is valid and has wild character in it.
   */
  boolean rulePatternIsValidWithWildCardChar() {
    return patternWithoutWildCardChar == null && patternWithWildCardChar != null && this.patternORWildChar == null;
  }

  /**
   * Returns true if the rule pattern is valid and has wild character in it.
   */
  boolean rulePatternIsValidWithoutWildCardChar() {
    return patternWithWildCardChar == null && patternWithoutWildCardChar != null && this.patternORWildChar == null;
  }

  /**
   * Returns true if the rule pattern is valid and has wild character in it.
   */
  boolean rulePatternIsValidWithORWildCardChar() {
    return patternWithoutWildCardChar == null && patternWithWildCardChar == null && this.patternORWildChar != null;
  }

  /**
   * This function returns the cost of the rule for the specified stack. Lower
   * the cost, the better the rule is matched
   *
   * @param stack
   *          Node stack encountered so far
   * @return cost of the function
   * @throws SemanticException
   */
  @Override
  public int cost(Stack<Node> stack) throws SemanticException {
    if (rulePatternIsValidWithoutWildCardChar()) {
      return costPatternWithoutWildCardChar(stack);
    }
    if (rulePatternIsValidWithWildCardChar()) {
      return costPatternWithWildCardChar(stack);
    }
    if (rulePatternIsValidWithORWildCardChar()) {
      return costPatternWithORWildCardChar(stack);
    }
    // If we reached here, either :
    // 1. patternWithWildCardChar and patternWithoutWildCardChar are both nulls.
    // 2. patternWithWildCardChar and patternWithoutWildCardChar are both not nulls.
    // This is an internal error and we should not let this happen, so throw an exception.
    throw new SemanticException (
      "Rule pattern is invalid for " + getName() + " : patternWithWildCardChar = " +
      patternWithWildCardChar + " patternWithoutWildCardChar = " +
      patternWithoutWildCardChar);
  }

  /**
   * @return the name of the Node
   **/
  @Override
  public String getName() {
    return ruleName;
  }
}
