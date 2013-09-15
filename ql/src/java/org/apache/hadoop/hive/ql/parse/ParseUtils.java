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

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.typeinfo.BaseTypeParams;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.hive.serde2.typeinfo.VarcharTypeParams;


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
  static ExprNodeDesc createConversionCast(ExprNodeDesc column, PrimitiveTypeInfo tableFieldTypeInfo)
      throws SemanticException {
    ExprNodeDesc ret;

    // Get base type, since type string may be parameterized
    String baseType = TypeInfoUtils.getBaseName(tableFieldTypeInfo.getTypeName());
    BaseTypeParams typeParams = null;
    // If TypeInfo is parameterized, provide the params to the UDF factory method.
    typeParams = tableFieldTypeInfo.getTypeParams();
    if (typeParams != null) {
      switch (tableFieldTypeInfo.getPrimitiveCategory()) {
        case VARCHAR:
          // Nothing to do here - the parameter will be passed to the UDF factory method below
          break;
        default:
          throw new SemanticException("Type cast for " + tableFieldTypeInfo.getPrimitiveCategory() +
              " does not take type parameters");
      }
    }

    // If the type cast UDF is for a parameterized type, then it should implement
    // the SettableUDF interface so that we can pass in the params.
    // Not sure if this is the cleanest solution, but there does need to be a way
    // to provide the type params to the type cast.
    ret = TypeCheckProcFactory.DefaultExprProcessor
        .getFuncExprNodeDescWithUdfData(baseType, typeParams, column);

    return ret;
  }

  public static VarcharTypeParams getVarcharParams(String typeName, ASTNode node)
      throws SemanticException {
    if (node.getChildCount() != 1) {
      throw new SemanticException("Bad params for type " + typeName);
    }

    try {
      VarcharTypeParams typeParams = new VarcharTypeParams();
      String lengthStr = node.getChild(0).getText();
      Integer length = Integer.valueOf(lengthStr);
      typeParams.setLength(length.intValue());
      typeParams.validateParams();
      return typeParams;
    } catch (SerDeException err) {
      throw new SemanticException(err);
    }
  }
}
