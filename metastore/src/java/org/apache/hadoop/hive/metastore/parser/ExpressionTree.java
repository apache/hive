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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;

import org.antlr.runtime.ANTLRStringStream;
import org.antlr.runtime.CharStream;
import org.apache.hadoop.hive.common.FileUtils;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.hive_metastoreConstants;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.Table;

import com.google.common.collect.Sets;

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
    NOTEQUALS2 ("!=", "!="),
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

    @Override
    public String toString() {
      return op;
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

    /**
     * Generates a JDO filter statement
     * @param table
     *        The table on which the filter is applied.  If table is not null,
     *        then this method generates a JDO statement to get all partitions
     *        of the table that match the filter.
     *        If table is null, then this method generates a JDO statement to get all
     *        tables that match the filter.
     * @param params
     *        A map of parameter key to values for the filter statement.
     * @return a JDO filter statement
     * @throws MetaException
     */
    public String generateJDOFilter(Table table, Map<String, Object> params)
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
    public Object value;
    public boolean isReverseOrder = false;
    private static final String PARAM_PREFIX = "hive_filter_param_";

    @Override
    public String generateJDOFilter(Table table,
        Map<String, Object> params)
        throws MetaException {
      if (table != null) {
        return generateJDOFilterOverPartitions(table, params);
      } else {
        return generateJDOFilterOverTables(params);
      }
    }

    //can only support "=" and "!=" for now, because our JDO lib is buggy when
    // using objects from map.get()
    private static final Set<Operator> TABLE_FILTER_OPS = Sets.newHashSet(
        Operator.EQUALS, Operator.NOTEQUALS, Operator.NOTEQUALS2);

    private String generateJDOFilterOverTables(Map<String, Object> params)
        throws MetaException {
      if (keyName.equals(hive_metastoreConstants.HIVE_FILTER_FIELD_OWNER)) {
        keyName = "this.owner";
      } else if (keyName.equals(hive_metastoreConstants.HIVE_FILTER_FIELD_LAST_ACCESS)) {
        //lastAccessTime expects an integer, so we cannot use the "like operator"
        if (operator == Operator.LIKE) {
          throw new MetaException("Like is not supported for HIVE_FILTER_FIELD_LAST_ACCESS");
        }
        keyName = "this.lastAccessTime";
      } else if (keyName.startsWith(hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS)) {
        if (!TABLE_FILTER_OPS.contains(operator)) {
          throw new MetaException("Only " + TABLE_FILTER_OPS + " are supported " +
            "operators for HIVE_FILTER_FIELD_PARAMS");
        }
        String paramKeyName = keyName.substring(hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS.length());
        keyName = "this.parameters.get(\"" + paramKeyName + "\")";
        //value is persisted as a string in the db, so make sure it's a string here
        // in case we get an integer.
        value = value.toString();
      } else {
        throw new MetaException("Invalid key name in filter.  " +
          "Use constants from org.apache.hadoop.hive.metastore.api");
      }
      return generateJDOFilterGeneral(params);
    }

    /**
     * Generates a general filter.  Given a map of <key, value>,
     * generates a statement of the form:
     * key1 operator value2 (&& | || ) key2 operator value2 ...
     *
     * Currently supported types for value are String and Integer.
     * The LIKE operator for Integers is unsupported.
     */
    private String generateJDOFilterGeneral(Map<String, Object> params)
        throws MetaException {
      String paramName = PARAM_PREFIX + params.size();
      params.put(paramName, value);
      String filter;

      if (isReverseOrder) {
        if (operator == Operator.LIKE) {
          throw new MetaException(
              "Value should be on the RHS for LIKE operator : " +
              "Key <" + keyName + ">");
        } else {
          filter = paramName + " " + operator.getJdoOp() + " " + keyName;
        }
      } else {
        if (operator == Operator.LIKE) {
          filter = " " + keyName + "." + operator.getJdoOp() + "(" + paramName + ") ";
        } else {
          filter = " " + keyName + " " + operator.getJdoOp() + " " + paramName;
        }
      }
      return filter;
    }

    private String generateJDOFilterOverPartitions(Table table, Map<String, Object> params)
    throws MetaException {

      int partitionColumnCount = table.getPartitionKeys().size();
      int partitionColumnIndex;
      for(partitionColumnIndex = 0;
      partitionColumnIndex < partitionColumnCount;
      partitionColumnIndex++ ) {
        if( table.getPartitionKeys().get(partitionColumnIndex).getName().
            equalsIgnoreCase(keyName)) {
          break;
        }
      }
      assert (table.getPartitionKeys().size() > 0);

      if( partitionColumnIndex == table.getPartitionKeys().size() ) {
        throw new MetaException("Specified key <" + keyName +
            "> is not a partitioning key for the table");
      }

      //Can only support partitions whose types are string
      if( ! table.getPartitionKeys().get(partitionColumnIndex).
          getType().equals(org.apache.hadoop.hive.serde.serdeConstants.STRING_TYPE_NAME) ) {
        throw new MetaException
        ("Filtering is supported only on partition keys of type string");
      }

      String valueParam = null;
      try {
        valueParam = (String) value;
      } catch (ClassCastException e) {
        throw new MetaException("Filtering is supported only on partition keys of type string");
      }

      String paramName = PARAM_PREFIX + params.size();
      params.put(paramName, valueParam);
      String filter;

      String keyEqual = FileUtils.escapePathName(keyName) + "=";
      int keyEqualLength = keyEqual.length();
      String valString;
      // partitionname ==>  (key=value/)*(key=value)
      if (partitionColumnIndex == (partitionColumnCount - 1)) {
        valString = "partitionName.substring(partitionName.indexOf(\"" + keyEqual + "\")+" + keyEqualLength + ")";
      }
      else {
        valString = "partitionName.substring(partitionName.indexOf(\"" + keyEqual + "\")+" + keyEqualLength + ").substring(0, partitionName.substring(partitionName.indexOf(\"" + keyEqual + "\")+" + keyEqualLength + ").indexOf(\"/\"))";
      }

      //Handle "a > 10" and "10 > a" appropriately
      if (isReverseOrder){
        //For LIKE, the value should be on the RHS
        if( operator == Operator.LIKE ) {
          throw new MetaException(
              "Value should be on the RHS for LIKE operator : " +
              "Key <" + keyName + ">");
        } else if (operator == Operator.EQUALS) {
          filter = makeFilterForEquals(keyName, valueParam, paramName, params,
              partitionColumnIndex, partitionColumnCount);
        } else {
          filter = paramName +
          " " + operator.getJdoOp() + " " + valString;
        }
      } else {
        if (operator == Operator.LIKE ) {
          //generate this.values.get(i).matches("abc%")
          filter = " " + valString + "."
              + operator.getJdoOp() + "(" + paramName + ") ";
        } else if (operator == Operator.EQUALS) {
          filter = makeFilterForEquals(keyName, valueParam, paramName, params,
              partitionColumnIndex, partitionColumnCount);
        } else {
          filter = " " + valString + " "
              + operator.getJdoOp() + " " + paramName;
        }
      }
      return filter;
    }
  }

  /**
   * For equals, we can make the JDO query much faster by filtering based on the
   * partition name. For a condition like ds="2010-10-01", we can see if there
   * are any partitions with a name that contains the substring "ds=2010-10-01/"
   * False matches aren't possible since "=" is escaped for partition names
   * and the trailing '/' ensures that we won't get a match with ds=2010-10-011
   *
   * Two cases to keep in mind: Case with only one partition column (no '/'s)
   * Case where the partition key column is at the end of the name. (no
   * tailing '/')
   *
   * @param keyName name of the partition col e.g. ds
   * @param value
   * @param paramName name of the parameter to use for JDOQL
   * @param params a map from the parameter name to their values
   * @return
   * @throws MetaException
   */
  private static String makeFilterForEquals(String keyName, String value,
      String paramName, Map<String, Object> params, int keyPos, int keyCount)
      throws MetaException {
    Map<String, String> partKeyToVal = new HashMap<String, String>();
    partKeyToVal.put(keyName, value);
    // If a partition has multiple partition keys, we make the assumption that
    // makePartName with one key will return a substring of the name made
    // with both all the keys.
    String escapedNameFragment = Warehouse.makePartName(partKeyToVal, false);
    StringBuilder fltr = new StringBuilder();
    if (keyCount == 1) {
      // Case where this is no other partition columns
      params.put(paramName, escapedNameFragment);
      fltr.append("partitionName == ").append(paramName);
    } else if (keyPos + 1 == keyCount) {
      // Case where the partition column is at the end of the name. There will
      // be a leading '/' but no trailing '/'
      params.put(paramName, "/" + escapedNameFragment);
      fltr.append("partitionName.endsWith(").append(paramName).append(')');
    } else if (keyPos == 0) {
      // Case where the parttion column is at the beginning of the name. There will
      // be a trailing '/' but no leading '/'
      params.put(paramName, escapedNameFragment + "/");
      fltr.append("partitionName.startsWith(").append(paramName).append(')');
    } else {
      // Case where the partition column is in the middle of the name. There will
      // be a leading '/' and an trailing '/'
      params.put(paramName, "/" + escapedNameFragment + "/");
      fltr.append("partitionName.indexOf(").append(paramName).append(") >= 0");
    }
    return fltr.toString();
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
        Map<String, Object> params) throws MetaException {
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

    @Override
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
