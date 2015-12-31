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
  private int leaf;
  private final SearchArgument.TruthValue constant;

  ExpressionTree() {
    operator = null;
    children = null;
    leaf = 0;
    constant = null;
  }

  ExpressionTree(Operator op, ExpressionTree... kids) {
    operator = op;
    children = new ArrayList<ExpressionTree>();
    leaf = -1;
    this.constant = null;
    Collections.addAll(children, kids);
  }

  ExpressionTree(int leaf) {
    operator = Operator.LEAF;
    children = null;
    this.leaf = leaf;
    this.constant = null;
  }

  ExpressionTree(SearchArgument.TruthValue constant) {
    operator = Operator.CONSTANT;
    children = null;
    this.leaf = -1;
    this.constant = constant;
  }

  ExpressionTree(ExpressionTree other) {
    this.operator = other.operator;
    if (other.children == null) {
      this.children = null;
    } else {
      this.children = new ArrayList<ExpressionTree>();
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
        return leaves[leaf];
      case CONSTANT:
        return constant;
      default:
        throw new IllegalStateException("Unknown operator: " + operator);
    }
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    switch (operator) {
      case OR:
        buffer.append("(or");
        for(ExpressionTree child: children) {
          buffer.append(' ');
          buffer.append(child.toString());
        }
        buffer.append(')');
        break;
      case AND:
        buffer.append("(and");
        for(ExpressionTree child: children) {
          buffer.append(' ');
          buffer.append(child.toString());
        }
        buffer.append(')');
        break;
      case NOT:
        buffer.append("(not ");
        buffer.append(children.get(0));
        buffer.append(')');
        break;
      case LEAF:
        buffer.append("leaf-");
        buffer.append(leaf);
        break;
      case CONSTANT:
        buffer.append(constant);
        break;
    }
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
    return leaf;
  }

  public void setLeaf(int leaf) {
    this.leaf = leaf;
  }
}
