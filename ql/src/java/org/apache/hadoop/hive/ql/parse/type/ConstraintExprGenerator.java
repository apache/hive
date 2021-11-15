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
package org.apache.hadoop.hive.ql.parse.type;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.util.ImmutableBitSet;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.metadata.CheckConstraint;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.metadata.NotNullConstraint;
import org.apache.hadoop.hive.ql.metadata.Table;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTBuilder;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;

/**
 * This class generates enforce_constraint function expressions to validate constraints.
 * @param <T> Type of result expression:
 * <ul>
 * <li> {@link org.apache.hadoop.hive.ql.plan.ExprNodeDesc} </li>
 * <li> {@link org.apache.calcite.rex.RexNode} </li>
 * </ul>
 */
class ConstraintExprGenerator<T> {
  private final HiveConf conf;
  private final TypeCheckProcFactory<T> typeCheckProcFactory;
  private final TypeCheckProcFactory<T>.DefaultExprProcessor exprProcessor;

  ConstraintExprGenerator(HiveConf conf, TypeCheckProcFactory<T> typeCheckProcFactory) {
    this.conf = conf;
    this.typeCheckProcFactory = typeCheckProcFactory;
    this.exprProcessor = typeCheckProcFactory.getDefaultExprProcessor();
  }

  public T genConstraintsExpr(
      Table targetTable, boolean updateStatement, RowResolver inputRR)
      throws SemanticException {
    List<ColumnInfo> inputColumnInfoList = inputRR.getColumnInfos();
    T nullConstraintExpr = getNotNullConstraintExpr(targetTable, inputRR, updateStatement);
    T checkConstraintExpr = getCheckConstraintExpr(targetTable, inputColumnInfoList, inputRR, updateStatement);
    T combinedConstraintExpr = null;
    if (nullConstraintExpr != null && checkConstraintExpr != null) {
      combinedConstraintExpr = exprProcessor.getFuncExprNodeDesc("and", nullConstraintExpr, checkConstraintExpr);
    } else if (nullConstraintExpr != null) {
      combinedConstraintExpr = nullConstraintExpr;
    } else if (checkConstraintExpr != null) {
      combinedConstraintExpr = checkConstraintExpr;
    }

    if (combinedConstraintExpr == null) {
      return null;
    }
    return exprProcessor.getFuncExprNodeDesc("enforce_constraint", combinedConstraintExpr);
  }

  private T getNotNullConstraintExpr(
      Table targetTable, RowResolver inputRR, boolean isUpdateStatement)
      throws SemanticException {
    boolean forceNotNullConstraint = conf.getBoolVar(HiveConf.ConfVars.HIVE_ENFORCE_NOT_NULL_CONSTRAINT);
    if (!forceNotNullConstraint) {
      return null;
    }

    ImmutableBitSet nullConstraintBitSet;
    try {
      nullConstraintBitSet = getEnabledNotNullConstraints(targetTable);
    } catch (SemanticException e) {
      throw e;
    } catch (Exception e) {
      throw (new RuntimeException(e));
    }

    if (nullConstraintBitSet == null) {
      return null;
    }

    T currUDF = null;
    int constraintIdx = 0;
    List<ColumnInfo> inputColInfos = inputRR.getColumnInfos();
    for (int colExprIdx = 0; colExprIdx < inputColInfos.size(); colExprIdx++) {
      if (isUpdateStatement && colExprIdx == 0) {
        // for updates first column is _rowid
        continue;
      }
      if (nullConstraintBitSet.indexOf(constraintIdx) != -1) {
        T currExpr = typeCheckProcFactory.exprFactory.createColumnRefExpr(inputColInfos.get(colExprIdx), inputRR, 0);
        T isNotNullUDF = exprProcessor.getFuncExprNodeDesc("isnotnull", currExpr);
        if (currUDF != null) {
          currUDF = exprProcessor.getFuncExprNodeDesc("and", currUDF, isNotNullUDF);
        } else {
          currUDF = isNotNullUDF;
        }
      }
      constraintIdx++;
    }
    return currUDF;
  }

  private ImmutableBitSet getEnabledNotNullConstraints(Table tbl) throws HiveException {
    final NotNullConstraint nnc = Hive.get().getEnabledNotNullConstraints(
        tbl.getDbName(), tbl.getTableName());
    if (nnc == null || nnc.getNotNullConstraints().isEmpty()) {
      return null;
    }
    // Build the bitset with not null columns
    ImmutableBitSet.Builder builder = ImmutableBitSet.builder();
    for (String nnCol : nnc.getNotNullConstraints().values()) {
      int nnPos = -1;
      for (int i = 0; i < tbl.getCols().size(); i++) {
        if (tbl.getCols().get(i).getName().equals(nnCol)) {
          nnPos = i;
          builder.set(nnPos);
          break;
        }
      }
    }
    return builder.build();
  }

  private T getCheckConstraintExpr(
      Table tbl, List<ColumnInfo> inputColInfos, RowResolver inputRR, boolean isUpdateStatement)
      throws SemanticException {

    CheckConstraint cc = null;
    try {
      cc = Hive.get().getEnabledCheckConstraints(tbl.getDbName(), tbl.getTableName());
    } catch (HiveException e) {
      throw new SemanticException(e);
    }
    if (cc == null || cc.getCheckConstraints().isEmpty()) {
      return null;
    }

    // build a map which tracks the name of column in input's signature to corresponding table column name
    // this will be used to replace column references in CHECK expression AST with corresponding column name
    // in input
    Map<String, String> col2Cols = new HashMap<>();
    int colIdx = 0;
    if (isUpdateStatement) {
      // if this is an update we need to skip the first col since it is row id
      colIdx = 1;
    }
    for (FieldSchema fs : tbl.getCols()) {
      // since SQL is case insenstive just to make sure that the comparison b/w column names
      // and check expression's column reference work convert the key to lower case
      col2Cols.put(fs.getName().toLowerCase(), inputColInfos.get(colIdx).getInternalName());
      colIdx++;
    }

    List<String> checkExprStrs = cc.getCheckExpressionList();
    TypeCheckCtx typeCheckCtx = new TypeCheckCtx(inputRR);
    T checkAndExprs = null;
    for (String checkExprStr : checkExprStrs) {
      try {
        ParseDriver parseDriver = new ParseDriver();
        ASTNode checkExprAST = parseDriver.parseExpression(checkExprStr);
        //replace column references in checkExprAST with corresponding columns in input
        replaceColumnReference(checkExprAST, col2Cols, inputRR);
        Map<ASTNode, T> genExprs = typeCheckProcFactory.genExprNode(checkExprAST, typeCheckCtx);
        T checkExpr = genExprs.get(checkExprAST);
        // Check constraint fails only if it evaluates to false, NULL/UNKNOWN should evaluate to TRUE
        T notFalseCheckExpr = exprProcessor.getFuncExprNodeDesc("isnotfalse", checkExpr);
        if (checkAndExprs == null) {
          checkAndExprs = notFalseCheckExpr;
        } else {
          checkAndExprs = exprProcessor.getFuncExprNodeDesc("and", checkAndExprs, notFalseCheckExpr);
        }
      } catch (Exception e) {
        throw new SemanticException(e);
      }
    }
    return checkAndExprs;
  }

  private void replaceColumnReference(ASTNode checkExpr, Map<String, String> col2Col,
                                      RowResolver inputRR) {
    if (checkExpr.getType() == HiveParser.TOK_TABLE_OR_COL) {
      ASTNode oldColChild = (ASTNode) (checkExpr.getChild(0));
      String oldColRef = oldColChild.getText().toLowerCase();
      assert (col2Col.containsKey(oldColRef));
      String internalColRef = col2Col.get(oldColRef);
      String[] fullQualColRef = inputRR.reverseLookup(internalColRef);
      String newColRef = fullQualColRef[1];
      checkExpr.deleteChild(0);
      checkExpr.addChild(ASTBuilder.createAST(oldColChild.getType(), newColRef));
    } else {
      for (int i = 0; i < checkExpr.getChildCount(); i++) {
        replaceColumnReference((ASTNode) (checkExpr.getChild(i)), col2Col, inputRR);
      }
    }
  }
}
