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
package org.apache.hadoop.hive.metastore.parser;

import java.io.IOException;
import java.util.Map;
import java.util.Stack;

import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.serde.Constants;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;

/**
 * The Class representing the filter as a  binary tree. The tree has TreeNode's
 * at intermediate level and the leaf level nodes are of type LeafNode.
 */
public class ExpressionTree {

  /** The logical operations supported. */
  public enum LogicalOperator {
    AND,
    OR
  }

  /** The operators supported. */
  public enum Operator {
    EQUALS  ("=", "=="),
    GREATERTHAN  (">"),
    LESSTHAN  ("<"),
    LESSTHANOREQUALTO ("<="),
    GREATERTHANOREQUALTO (">="),
    LIKE ("LIKE", "matches"),
    NOTEQUALS ("<>", "!=");

    private final String op;
    private final String jdoOp;

    // private constructor
    private Operator(String op){
      this.op = op;
      this.jdoOp = op;
    }

    private Operator(String op, String jdoOp){
      this.op = op;
      this.jdoOp = jdoOp;
    }

    public String getOp() {
      return op;
    }

    public String getJdoOp() {
      return jdoOp;
    }

    public static Operator fromString(String inputOperator) {
      for(Operator op : Operator.values()) {
        if(op.getOp().equals(inputOperator)){
          return op;
        }
      }

      throw new Error("Invalid value " + inputOperator +
          " for " + Operator.class.getSimpleName());
    }
  }


  /**
   * The Class representing a Node in the ExpressionTree.
   */
  public static class TreeNode {
    private TreeNode lhs;
    private LogicalOperator andOr;
    private TreeNode rhs;

    public TreeNode() {
    } 

    public TreeNode(TreeNode lhs, LogicalOperator andOr, TreeNode rhs) {
      this.lhs = lhs;
      this.andOr = andOr;
      this.rhs = rhs;
    }

    public String generateJDOFilter(Table table, Map<String, String> params)
    throws MetaException {
      StringBuilder filterBuffer = new StringBuilder();

      if ( lhs != null) {
        filterBuffer.append (" (");
        filterBuffer.append(lhs.generateJDOFilter(table, params));

        if (rhs != null) {
          if( andOr == LogicalOperator.AND ) {
            filterBuffer.append(" && ");
          } else {
            filterBuffer.append(" || ");
          }

          filterBuffer.append(rhs.generateJDOFilter(table, params));
        }
        filterBuffer.append (") ");
      }

      return filterBuffer.toString();
    }

  }

  /**
   * The Class representing the leaf level nodes in the ExpressionTree.
   */
  public static class LeafNode extends TreeNode {
    public String keyName;
    public Operator operator;
    public String value;
    public boolean isReverseOrder = false;
    private static final String PARAM_PREFIX = "hive_filter_param_";

    @Override
    public String generateJDOFilter(Table table, Map<String, String> params)
    throws MetaException {
      int partitionIndex;
      for(partitionIndex = 0;
      partitionIndex < table.getPartitionKeys().size();
      partitionIndex++ ) {
        if( table.getPartitionKeys().get(partitionIndex).getName().
            equalsIgnoreCase(keyName)) {
          break;
        }
      }

      if( partitionIndex == table.getPartitionKeys().size() ) {
        throw new MetaException("Specified key <" + keyName +
            "> is not a partitioning key for the table");
      }

      if( ! table.getPartitionKeys().get(partitionIndex).
          getType().equals(Constants.STRING_TYPE_NAME) ) {
        throw new MetaException
        ("Filtering is supported only on partition keys of type string");
      }

      String paramName = PARAM_PREFIX + params.size();
      params.put(paramName, value);
      String filter;

      //Handle "a > 10" and "10 > a" appropriately
      if (isReverseOrder){
        //For LIKE, the value should be on the RHS
        if( operator == Operator.LIKE ) {
          throw new MetaException(
              "Value should be on the RHS for LIKE operator : " +
              "Key <" + keyName + ">");
        }

        filter = paramName +
          " " + operator.getJdoOp() + " " +
          " this.values.get(" + partitionIndex + ")";
      } else {
        if( operator == Operator.LIKE ) {
          //generate this.values.get(i).matches("abc%")
          filter = " this.values.get(" + partitionIndex + ")."
              + operator.getJdoOp() + "(" + paramName + ") ";
        } else {
          filter = " this.values.get(" + partitionIndex + ") "
              + operator.getJdoOp() + " " + paramName;
        }
      }
      return filter;
    }
  }

  /**
   * The root node for the tree.
   */
  private TreeNode root = null;

  /**
   * The node stack used to keep track of the tree nodes during parsing.
   */
  private final Stack<TreeNode> nodeStack = new Stack<TreeNode>();

  /**
   * Adds a intermediate node of either type(AND/OR). Pops last two nodes from
   * the stack and sets them as children of the new node and pushes itself
   * onto the stack.
   * @param andOr the operator type
   */
  public void addIntermediateNode(LogicalOperator andOr) {

    TreeNode rhs = nodeStack.pop();
    TreeNode lhs = nodeStack.pop();
    TreeNode newNode = new TreeNode(lhs, andOr, rhs);
    nodeStack.push(newNode);
    root = newNode;
  }

  /**
   * Adds a leaf node, pushes the new node onto the stack.
   * @param newNode the new node
   */
  public void addLeafNode(LeafNode newNode) {
    if( root == null ) {
      root = newNode;
    }
    nodeStack.push(newNode);
  }

  /** Generate the JDOQL filter for the given expression tree
   * @param table the table being queried
   * @param params the input map which is updated with the
   *     the parameterized values. Keys are the parameter names and values
   *     are the parameter values
   * @return the string representation of the expression tree
   * @throws MetaException
   */
  public String generateJDOFilter(Table table,
        Map<String, String> params) throws MetaException {
    if( root == null ) {
      return "";
    }

    return root.generateJDOFilter(table, params);
  }

  /** Case insensitive ANTLR string stream */
  public static class ANTLRNoCaseStringStream extends ANTLRStringStream {
    public ANTLRNoCaseStringStream (String input) {
      super(input);
    }

    public int LA (int i) {
      int returnChar = super.LA (i);

      if (returnChar == CharStream.EOF) {
        return returnChar;
      }
      else if (returnChar == 0) {
        return returnChar;
      }

      return Character.toUpperCase ((char) returnChar);
    }
  }
}
