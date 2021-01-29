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

package org.apache.hadoop.hive.ql.io.sarg;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * The inner representation of the SearchArgument. Most users should not
 * need this interface, it is only for file formats that need to translate
 * the SearchArgument into an internal form.
 */
public class ExpressionTree {
  public enum Operator {OR, AND, NOT, LEAF, CONSTANT}
  private final Operator operator;
  private final List<ExpressionTree> children;
  private final PredicateLeaf leaf;
  private final SearchArgument.TruthValue constant;

  ExpressionTree() {
    operator = null;
    children = null;
    leaf = null;
    constant = null;
  }

  ExpressionTree(Operator op, ExpressionTree... kids) {
    operator = op;
    children = new ArrayList<>();
    leaf = null;
    this.constant = null;
    Collections.addAll(children, kids);
  }

  ExpressionTree(PredicateLeaf leaf) {
    operator = Operator.LEAF;
    children = null;
    this.leaf = leaf;
    this.constant = null;
  }

  ExpressionTree(SearchArgument.TruthValue constant) {
    operator = Operator.CONSTANT;
    children = null;
    this.leaf = null;
    this.constant = constant;
  }

  ExpressionTree(ExpressionTree other) {
    this.operator = other.operator;
    if (other.children == null) {
      this.children = null;
    } else {
      this.children = new ArrayList<>();
      for(ExpressionTree child: other.children) {
        children.add(new ExpressionTree(child));
      }
    }
    this.leaf = other.leaf;
    this.constant = other.constant;
  }

  public SearchArgument.TruthValue evaluate(SearchArgument.TruthValue[] leaves
                                            ) {
    SearchArgument.TruthValue result = null;
    switch (operator) {
      case OR:
        for(ExpressionTree child: children) {
          result = child.evaluate(leaves).or(result);
        }
        return result;
      case AND:
        for(ExpressionTree child: children) {
          result = child.evaluate(leaves).and(result);
        }
        return result;
      case NOT:
        return children.get(0).evaluate(leaves).not();
      case LEAF:
        return leaves[leaf.getId()];
      case CONSTANT:
        return constant;
      default:
        throw new IllegalStateException("Unknown operator: " + operator);
    }
  }

  private void buildString(boolean useLeafIds, StringBuilder output) {
    switch (operator) {
      case OR:
        output.append("(or");
        for(ExpressionTree child: children) {
          output.append(' ');
          child.buildString(useLeafIds, output);
        }
        output.append(')');
        break;
      case AND:
        output.append("(and");
        for(ExpressionTree child: children) {
          output.append(' ');
          child.buildString(useLeafIds, output);
        }
        output.append(')');
        break;
      case NOT:
        output.append("(not ");
        children.get(0).buildString(useLeafIds, output);
        output.append(')');
        break;
      case LEAF:
        output.append("leaf-");
        if (useLeafIds) {
          output.append(leaf.getId());
        } else {
          output.append(leaf);
        }
        break;
      case CONSTANT:
        output.append(constant);
        break;
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buildString(false, buffer);
    return buffer.toString();
  }

  /**
   * Generate the old string for the old test cases.
   * @return the expression as a string using the leaf ids
   */
  public String toOldString() {
    StringBuilder buffer = new StringBuilder();
    buildString(true, buffer);
    return buffer.toString();
  }

  public Operator getOperator() {
    return operator;
  }

  public List<ExpressionTree> getChildren() {
    return children;
  }

  public SearchArgument.TruthValue getConstant() {
    return constant;
  }

  public int getLeaf() {
    return leaf.getId();
  }

  public PredicateLeaf getPredicateLeaf() {
    return leaf;
  }
}
