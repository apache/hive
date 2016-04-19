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

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Queue;
import java.util.Set;
import java.util.Stack;

import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.typeinfo.CharTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.DecimalTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeInfo;


/**
 * Library of utility functions used in the parse code.
 *
 */
public final class ParseUtils {

  /**
   * Tests whether the parse tree node is a join token.
   *
   * @param node
   *          The parse tree node
   * @return boolean
   */
  public static boolean isJoinToken(ASTNode node) {
    switch (node.getToken().getType()) {
    case HiveParser.TOK_JOIN:
    case HiveParser.TOK_LEFTOUTERJOIN:
    case HiveParser.TOK_RIGHTOUTERJOIN:
    case HiveParser.TOK_FULLOUTERJOIN:
      return true;
    default:
      return false;
    }
  }

  /**
   * Performs a descent of the leftmost branch of a tree, stopping when either a
   * node with a non-null token is found or the leaf level is encountered.
   *
   * @param tree
   *          candidate node from which to start searching
   *
   * @return node at which descent stopped
   */
  public static ASTNode findRootNonNullToken(ASTNode tree) {
    while ((tree.getToken() == null) && (tree.getChildCount() > 0)) {
      tree = (ASTNode) tree.getChild(0);
    }
    return tree;
  }

  private ParseUtils() {
    // prevent instantiation
  }

  public static List<String> validateColumnNameUniqueness(
      List<FieldSchema> fieldSchemas) throws SemanticException {

    // no duplicate column names
    // currently, it is a simple n*n algorithm - this can be optimized later if
    // need be
    // but it should not be a major bottleneck as the number of columns are
    // anyway not so big
    Iterator<FieldSchema> iterCols = fieldSchemas.iterator();
    List<String> colNames = new ArrayList<String>();
    while (iterCols.hasNext()) {
      String colName = iterCols.next().getName();
      Iterator<String> iter = colNames.iterator();
      while (iter.hasNext()) {
        String oldColName = iter.next();
        if (colName.equalsIgnoreCase(oldColName)) {
          throw new SemanticException(ErrorMsg.DUPLICATE_COLUMN_NAMES
              .getMsg(oldColName));
        }
      }
      colNames.add(colName);
    }
    return colNames;
  }

  /**
   * @param column  column expression to convert
   * @param tableFieldTypeInfo TypeInfo to convert to
   * @return Expression converting column to the type specified by tableFieldTypeInfo
   */
  public static ExprNodeDesc createConversionCast(ExprNodeDesc column, PrimitiveTypeInfo tableFieldTypeInfo)
      throws SemanticException {
    // Get base type, since type string may be parameterized
    String baseType = TypeInfoUtils.getBaseName(tableFieldTypeInfo.getTypeName());

    // If the type cast UDF is for a parameterized type, then it should implement
    // the SettableUDF interface so that we can pass in the params.
    // Not sure if this is the cleanest solution, but there does need to be a way
    // to provide the type params to the type cast.
    return TypeCheckProcFactory.DefaultExprProcessor.getFuncExprNodeDescWithUdfData(baseType,
        tableFieldTypeInfo, column);
  }

  public static VarcharTypeInfo getVarcharTypeInfo(ASTNode node)
      throws SemanticException {
    if (node.getChildCount() != 1) {
      throw new SemanticException("Bad params for type varchar");
    }

    String lengthStr = node.getChild(0).getText();
    return TypeInfoFactory.getVarcharTypeInfo(Integer.parseInt(lengthStr));
  }

  public static CharTypeInfo getCharTypeInfo(ASTNode node)
      throws SemanticException {
    if (node.getChildCount() != 1) {
      throw new SemanticException("Bad params for type char");
    }

    String lengthStr = node.getChild(0).getText();
    return TypeInfoFactory.getCharTypeInfo(Integer.parseInt(lengthStr));
  }

  static int getIndex(String[] list, String elem) {
    for(int i=0; i < list.length; i++) {
      if (list[i] != null && list[i].toLowerCase().equals(elem)) {
        return i;
      }
    }
    return -1;
  }

  /*
   * if the given filterCondn refers to only 1 table alias in the QBJoinTree,
   * we return that alias's position. Otherwise we return -1
   */
  static int checkJoinFilterRefersOneAlias(String[] tabAliases, ASTNode filterCondn) {

    switch(filterCondn.getType()) {
    case HiveParser.TOK_TABLE_OR_COL:
      String tableOrCol = SemanticAnalyzer.unescapeIdentifier(filterCondn.getChild(0).getText()
          .toLowerCase());
      return getIndex(tabAliases, tableOrCol);
    case HiveParser.Identifier:
    case HiveParser.Number:
    case HiveParser.StringLiteral:
    case HiveParser.BigintLiteral:
    case HiveParser.SmallintLiteral:
    case HiveParser.TinyintLiteral:
    case HiveParser.DecimalLiteral:
    case HiveParser.TOK_STRINGLITERALSEQUENCE:
    case HiveParser.TOK_CHARSETLITERAL:
    case HiveParser.TOK_DATELITERAL:
    case HiveParser.KW_TRUE:
    case HiveParser.KW_FALSE:
    case HiveParser.TOK_NULL:
      return -1;
    default:
      int idx = -1;
      int i = filterCondn.getType() == HiveParser.TOK_FUNCTION ? 1 : 0;
      for (; i < filterCondn.getChildCount(); i++) {
        int cIdx = checkJoinFilterRefersOneAlias(tabAliases, (ASTNode) filterCondn.getChild(i));
        if ( cIdx != idx ) {
          if ( idx != -1 && cIdx != -1 ) {
            return -1;
          }
          idx = idx == -1 ? cIdx : idx;
        }
      }
      return idx;
    }
  }

  public static DecimalTypeInfo getDecimalTypeTypeInfo(ASTNode node)
      throws SemanticException {
    if (node.getChildCount() > 2) {
        throw new SemanticException("Bad params for type decimal");
      }

      int precision = HiveDecimal.USER_DEFAULT_PRECISION;
      int scale = HiveDecimal.USER_DEFAULT_SCALE;

      if (node.getChildCount() >= 1) {
        String precStr = node.getChild(0).getText();
        precision = Integer.parseInt(precStr);
      }

      if (node.getChildCount() == 2) {
        String scaleStr = node.getChild(1).getText();
        scale = Integer.parseInt(scaleStr);
      }

      return TypeInfoFactory.getDecimalTypeInfo(precision, scale);
  }

  public static String ensureClassExists(String className)
      throws SemanticException {
    if (className == null) {
      return null;
    }
    try {
      Class.forName(className, true, Utilities.getSessionSpecifiedClassLoader());
    } catch (ClassNotFoundException e) {
      throw new SemanticException("Cannot find class '" + className + "'", e);
    }
    return className;
  }

  public static boolean containsTokenOfType(ASTNode root, Integer ... tokens) {
      final Set<Integer> tokensToMatch = new HashSet<Integer>();
      for (Integer tokenTypeToMatch : tokens) {
          tokensToMatch.add(tokenTypeToMatch);
        }

        return ParseUtils.containsTokenOfType(root, new PTFUtils.Predicate<ASTNode>() {
          @Override
          public boolean apply(ASTNode node) {
              return tokensToMatch.contains(node.getType());
            }
        });
    }

    public static boolean containsTokenOfType(ASTNode root, PTFUtils.Predicate<ASTNode> predicate) {
      Queue<ASTNode> queue = new ArrayDeque<ASTNode>();

      // BFS
      queue.add(root);
      while (!queue.isEmpty())  {
        ASTNode current = queue.remove();
        // If the predicate matches, then return true.
        // Otherwise visit the next set of nodes that haven't been seen.
        if (predicate.apply(current)) {
          return true;
        } else {
          // Guard because ASTNode.getChildren.iterator returns null if no children available (bug).
          if (current.getChildCount() > 0) {
            for (Node child : current.getChildren()) {
              queue.add((ASTNode) child);
            }
          }
        }
      }

      return false;
    }

    public static boolean sameTree(ASTNode node, ASTNode otherNode) {
      if (node == null && otherNode == null) {
        return true;
      }
      if ((node == null && otherNode != null) ||
              (node != null && otherNode == null)) {
        return false;
      }

      Stack<Tree> stack = new Stack<Tree>();
      stack.push(node);
      Stack<Tree> otherStack = new Stack<Tree>();
      otherStack.push(otherNode);

      while (!stack.empty() && !otherStack.empty()) {
        Tree p = stack.pop();
        Tree otherP = otherStack.pop();

        if (p.isNil() != otherP.isNil()) {
          return false;
        }
        if (!p.isNil()) {
          if (!p.toString().equals(otherP.toString())) {
            return false;
          }
        }
        if (p.getChildCount() != otherP.getChildCount()) {
          return false;
        }
        for (int i = p.getChildCount()-1; i >= 0; i--) {
          Tree t = p.getChild(i);
          stack.push(t);
          Tree otherT = otherP.getChild(i);
          otherStack.push(otherT);
        }
      }

      return stack.empty() && otherStack.empty();
    }
}
