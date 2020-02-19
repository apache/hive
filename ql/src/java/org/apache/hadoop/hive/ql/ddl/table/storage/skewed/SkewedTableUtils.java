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

package org.apache.hadoop.hive.ql.ddl.table.storage.skewed;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * Utilities for skewed table related DDL.
 */
public final class SkewedTableUtils {

  private SkewedTableUtils() {
    throw new UnsupportedOperationException("SkewedTableUtils should not be instantiated!");
  }

  public static List<String> analyzeSkewedTableDDLColNames(ASTNode node) throws SemanticException {
    ASTNode child = (ASTNode) node.getChild(0);
    if (child == null) {
      throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_NAME.getMsg());
    } else {
      if (child.getToken().getType() != HiveParser.TOK_TABCOLNAME) {
        throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_NAME.getMsg());
      } else {
        return BaseSemanticAnalyzer.getColumnNames(child);
      }
    }
  }

  public static List<List<String>> analyzeDDLSkewedValues(ASTNode node) throws SemanticException {
    ASTNode child = (ASTNode) node.getChild(1);
    if (child == null) {
      throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
    }

    List<List<String>> skewedValues = new ArrayList<>();
    switch (child.getToken().getType()) {
    case HiveParser.TOK_TABCOLVALUE:
      for (String skewedValue : getSkewedValueFromASTNode(child)) {
        skewedValues.add(Arrays.asList(skewedValue));
      }
      break;
    case HiveParser.TOK_TABCOLVALUE_PAIR:
      for (Node cvNode : child.getChildren()) {
        ASTNode acvNode = (ASTNode) cvNode;
        if (acvNode.getToken().getType() != HiveParser.TOK_TABCOLVALUES) {
          throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
        } else {
          skewedValues.add(getSkewedValuesFromASTNode(acvNode));
        }
      }
      break;
    default:
      break;
    }

    return skewedValues;
  }

  public static List<String> getSkewedValuesFromASTNode(ASTNode node) throws SemanticException {
    ASTNode child = (ASTNode) node.getChild(0);
    if (child == null) {
      throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
    } else {
      if (child.getToken().getType() != HiveParser.TOK_TABCOLVALUE) {
        throw new SemanticException(ErrorMsg.SKEWED_TABLE_NO_COLUMN_VALUE.getMsg());
      } else {
        return new ArrayList<String>(getSkewedValueFromASTNode(child));
      }
    }
  }

  /**
   * Gets the skewed column list from the statement.
   *   create table xyz list bucketed (col1) with skew (1,2,5)
   *   AST Node is for (1,2,5)
   */
  private static List<String> getSkewedValueFromASTNode(ASTNode node) {
    List<String> colList = new ArrayList<String>();
    int numCh = node.getChildCount();
    for (int i = 0; i < numCh; i++) {
      ASTNode child = (ASTNode) node.getChild(i);
      colList.add(BaseSemanticAnalyzer.stripQuotes(child.getText()).toLowerCase());
    }
    return colList;
  }
}
