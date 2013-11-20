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
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.BaseCharTypeInfo;
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
  static ExprNodeDesc createConversionCast(ExprNodeDesc column, PrimitiveTypeInfo tableFieldTypeInfo)
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
    return TypeInfoFactory.getVarcharTypeInfo(Integer.valueOf(lengthStr));
  }

  public static CharTypeInfo getCharTypeInfo(ASTNode node)
      throws SemanticException {
    if (node.getChildCount() != 1) {
      throw new SemanticException("Bad params for type char");
    }

    String lengthStr = node.getChild(0).getText();
    return TypeInfoFactory.getCharTypeInfo(Integer.valueOf(lengthStr));
  }

  static int getIndex(String[] list, String elem) {
    for(int i=0; i < list.length; i++) {
      if (list[i].toLowerCase().equals(elem)) {
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
        precision = Integer.valueOf(precStr);
      }

      if (node.getChildCount() == 2) {
        String scaleStr = node.getChild(1).getText();
        scale = Integer.valueOf(scaleStr);
      }

      return TypeInfoFactory.getDecimalTypeInfo(precision, scale);
  }

}
