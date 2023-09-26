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

package org.apache.hadoop.hive.ql.ddl.table.constraint;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import com.google.common.collect.HashMultimap;
import com.google.common.collect.SetMultimap;
import org.antlr.runtime.TokenRewriteStream;
import org.antlr.runtime.tree.Tree;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.TableName;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.SQLCheckConstraint;
import org.apache.hadoop.hive.metastore.api.SQLDefaultConstraint;
import org.apache.hadoop.hive.metastore.api.SQLForeignKey;
import org.apache.hadoop.hive.metastore.api.SQLNotNullConstraint;
import org.apache.hadoop.hive.metastore.api.SQLPrimaryKey;
import org.apache.hadoop.hive.metastore.api.SQLUniqueConstraint;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.lib.CostLessRuleDispatcher;
import org.apache.hadoop.hive.ql.lib.ExpressionWalker;
import org.apache.hadoop.hive.ql.lib.Node;
import org.apache.hadoop.hive.ql.lib.NodeProcessorCtx;
import org.apache.hadoop.hive.ql.lib.SemanticGraphWalker;
import org.apache.hadoop.hive.ql.lib.SemanticNodeProcessor;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.Quotation;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.parse.type.TypeCheckCtx;
import org.apache.hadoop.hive.ql.parse.UnparseTranslator;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde.serdeConstants;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import com.google.common.collect.ImmutableList;

/**
 * Utilities for constraints.
 */
public final class ConstraintsUtils {

  public static final String CHECK_CONSTRAINT_PROGRAM = "CHECK_CONSTRAINT_PROGRAM";

  private ConstraintsUtils() {
    throw new UnsupportedOperationException("ConstraintsUtils should not be instantiated!");
  }

  private static class ConstraintInfo {
    final String colName;
    final String constraintName;
    final boolean enable;
    final boolean validate;
    final boolean rely;
    final String defaultValue;

    ConstraintInfo(String colName, String constraintName, boolean enable, boolean validate, boolean rely,
        String defaultValue) {
      this.colName = colName;
      this.constraintName = constraintName;
      this.enable = enable;
      this.validate = validate;
      this.rely = rely;
      this.defaultValue = defaultValue;
    }
  }

  public static void processPrimaryKeys(TableName tableName, ASTNode child, List<SQLPrimaryKey> primaryKeys)
      throws SemanticException {
    List<ConstraintInfo> primaryKeyInfos = generateConstraintInfos(child);
    constraintInfosToPrimaryKeys(tableName, primaryKeyInfos, primaryKeys);
  }

  public static void processPrimaryKeys(TableName tableName, ASTNode child, List<String> columnNames,
      List<SQLPrimaryKey> primaryKeys) throws SemanticException {
    List<ConstraintInfo> primaryKeyInfos = generateConstraintInfos(child, columnNames, null, null);
    constraintInfosToPrimaryKeys(tableName, primaryKeyInfos, primaryKeys);
  }

  private static void constraintInfosToPrimaryKeys(TableName tableName, List<ConstraintInfo> primaryKeyInfos,
      List<SQLPrimaryKey> primaryKeys) {
    int i = 1;
    for (ConstraintInfo primaryKeyInfo : primaryKeyInfos) {
      primaryKeys.add(new SQLPrimaryKey(tableName.getDb(), tableName.getTable(), primaryKeyInfo.colName, i++,
          primaryKeyInfo.constraintName, primaryKeyInfo.enable, primaryKeyInfo.validate, primaryKeyInfo.rely));
    }
  }

  /**
   * Process the unique constraints from the ast node and populate the SQLUniqueConstraint list.
   */
  public static void processUniqueConstraints(TableName tableName, ASTNode child,
      List<SQLUniqueConstraint> uniqueConstraints) throws SemanticException {
    List<ConstraintInfo> uniqueInfos = generateConstraintInfos(child);
    constraintInfosToUniqueConstraints(tableName, uniqueInfos, uniqueConstraints);
  }

  public static void processUniqueConstraints(TableName tableName, ASTNode child, List<String> columnNames,
      List<SQLUniqueConstraint> uniqueConstraints) throws SemanticException {
    List<ConstraintInfo> uniqueInfos = generateConstraintInfos(child, columnNames, null, null);
    constraintInfosToUniqueConstraints(tableName, uniqueInfos, uniqueConstraints);
  }

  private static void constraintInfosToUniqueConstraints(TableName tableName, List<ConstraintInfo> uniqueInfos,
      List<SQLUniqueConstraint> uniqueConstraints) {
    int i = 1;
    for (ConstraintInfo uniqueInfo : uniqueInfos) {
      uniqueConstraints.add(new SQLUniqueConstraint(tableName.getCat(), tableName.getDb(), tableName.getTable(),
          uniqueInfo.colName, i++, uniqueInfo.constraintName, uniqueInfo.enable, uniqueInfo.validate, uniqueInfo.rely));
    }
  }

  public static void processCheckConstraints(TableName tableName, ASTNode child, List<String> columnNames,
      List<SQLCheckConstraint> checkConstraints, final ASTNode typeChild, final TokenRewriteStream tokenRewriteStream)
      throws SemanticException {
    List<ConstraintInfo> checkInfos = generateConstraintInfos(child, columnNames, typeChild, tokenRewriteStream);
    constraintInfosToCheckConstraints(tableName, checkInfos, checkConstraints);
  }

  private static void constraintInfosToCheckConstraints(TableName tableName, List<ConstraintInfo> checkInfos,
      List<SQLCheckConstraint> checkConstraints) {
    for (ConstraintInfo checkInfo : checkInfos) {
      checkConstraints.add(new SQLCheckConstraint(tableName.getCat(), tableName.getDb(), tableName.getTable(),
          checkInfo.colName, checkInfo.defaultValue, checkInfo.constraintName, checkInfo.enable, checkInfo.validate,
          checkInfo.rely));
    }
  }

  public static void processDefaultConstraints(TableName tableName, ASTNode child, List<String> columnNames,
      List<SQLDefaultConstraint> defaultConstraints, final ASTNode typeChild, TokenRewriteStream tokenRewriteStream)
          throws SemanticException {
    List<ConstraintInfo> defaultInfos = generateConstraintInfos(child, columnNames, typeChild, tokenRewriteStream);
    constraintInfosToDefaultConstraints(tableName, defaultInfos, defaultConstraints);
  }

  private static void constraintInfosToDefaultConstraints(TableName tableName, List<ConstraintInfo> defaultInfos,
      List<SQLDefaultConstraint> defaultConstraints) {
    for (ConstraintInfo defaultInfo : defaultInfos) {
      defaultConstraints.add(new SQLDefaultConstraint(tableName.getCat(), tableName.getDb(), tableName.getTable(),
          defaultInfo.colName, defaultInfo.defaultValue, defaultInfo.constraintName, defaultInfo.enable,
          defaultInfo.validate, defaultInfo.rely));
    }
  }

  public static void processNotNullConstraints(TableName tableName, ASTNode child, List<String> columnNames,
      List<SQLNotNullConstraint> notNullConstraints) throws SemanticException {
    List<ConstraintInfo> notNullInfos = generateConstraintInfos(child, columnNames, null, null);
    constraintInfosToNotNullConstraints(tableName, notNullInfos, notNullConstraints);
  }

  private static void constraintInfosToNotNullConstraints(TableName tableName, List<ConstraintInfo> notNullInfos,
      List<SQLNotNullConstraint> notNullConstraints) {
    for (ConstraintInfo notNullInfo : notNullInfos) {
      notNullConstraints.add(new SQLNotNullConstraint(tableName.getCat(), tableName.getDb(), tableName.getTable(),
          notNullInfo.colName, notNullInfo.constraintName, notNullInfo.enable, notNullInfo.validate, notNullInfo.rely));
    }
  }

  /**
   * Get the constraint from the AST and populate the cstrInfos with the required information.
   */
  private static List<ConstraintInfo> generateConstraintInfos(ASTNode child) throws SemanticException {
    ImmutableList.Builder<String> columnNames = ImmutableList.builder();
    for (int j = 0; j < child.getChild(0).getChildCount(); j++) {
      Tree columnName = child.getChild(0).getChild(j);
      BaseSemanticAnalyzer.checkColumnName(columnName.getText());
      columnNames.add(BaseSemanticAnalyzer.unescapeIdentifier(columnName.getText().toLowerCase()));
    }
    return generateConstraintInfos(child, columnNames.build(), null, null);
  }

  private static final int CONSTRAINT_MAX_LENGTH = 255;

  /**
   * Get the constraint from the AST and populate the cstrInfos with the required information.
   * @param child  The node with the constraint token
   * @param columnNames The name of the columns for the primary key
   * @param typeChildForDefault type of column used for default value type check
   */
  private static List<ConstraintInfo> generateConstraintInfos(ASTNode child, List<String> columnNames,
      ASTNode typeChildForDefault, TokenRewriteStream tokenRewriteStream) throws SemanticException {
    // The ANTLR grammar looks like :
    // 1. KW_CONSTRAINT idfr=identifier KW_PRIMARY KW_KEY pkCols=columnParenthesesList
    //  constraintOptsCreate?
    // -> ^(TOK_PRIMARY_KEY $pkCols $idfr constraintOptsCreate?)
    // when the user specifies the constraint name.
    // 2.  KW_PRIMARY KW_KEY columnParenthesesList
    // constraintOptsCreate?
    // -> ^(TOK_PRIMARY_KEY columnParenthesesList constraintOptsCreate?)
    // when the user does not specify the constraint name.
    // Default values
    String constraintName = null;
    //by default if user hasn't provided any optional constraint properties
    // it will be considered ENABLE and NOVALIDATE and RELY=true
    boolean enable = true;
    boolean validate = false;
    boolean rely = true;
    String checkOrDefaultValue = null;
    int childType = child.getToken().getType();
    for (int i = 0; i < child.getChildCount(); i++) {
      ASTNode grandChild = (ASTNode) child.getChild(i);
      int type = grandChild.getToken().getType();
      if (type == HiveParser.TOK_CONSTRAINT_NAME) {
        constraintName = BaseSemanticAnalyzer.unescapeIdentifier(grandChild.getChild(0).getText().toLowerCase());
      } else if (type == HiveParser.TOK_ENABLE) {
        enable = true;
        // validate is false by default if we enable the constraint
        // TODO: A constraint like NOT NULL could be enabled using ALTER but VALIDATE remains
        //  false in such cases. Ideally VALIDATE should be set to true to validate existing data
        validate = false;
      } else if (type == HiveParser.TOK_DISABLE) {
        enable = false;
        // validate is false by default if we disable the constraint
        validate = false;
        rely = false;
      } else if (type == HiveParser.TOK_VALIDATE) {
        validate = true;
      } else if (type == HiveParser.TOK_NOVALIDATE) {
        validate = false;
      } else if (type == HiveParser.TOK_RELY) {
        rely = true;
      } else if (type == HiveParser.TOK_NORELY) {
        rely = false;
      } else if (childType == HiveParser.TOK_DEFAULT_VALUE) {
        // try to get default value only if this is DEFAULT constraint
        checkOrDefaultValue = getDefaultValue(grandChild, typeChildForDefault, tokenRewriteStream);
      } else if (childType == HiveParser.TOK_CHECK_CONSTRAINT) {
        UnparseTranslator unparseTranslator = collectUnescapeIdentifierTranslations(grandChild);
        unparseTranslator.applyTranslations(tokenRewriteStream, CHECK_CONSTRAINT_PROGRAM);
        checkOrDefaultValue = tokenRewriteStream.toString(CHECK_CONSTRAINT_PROGRAM, grandChild.getTokenStartIndex(),
            grandChild.getTokenStopIndex());
      }
    }

    // metastore schema only allows maximum 255 for constraint name column
    if (constraintName != null && constraintName.length() > CONSTRAINT_MAX_LENGTH) {
      throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Constraint name: " + constraintName +
          " exceeded maximum allowed length: " + CONSTRAINT_MAX_LENGTH));
    }

    // metastore schema only allows maximum 255 for constraint value column
    if (checkOrDefaultValue!= null && checkOrDefaultValue.length() > CONSTRAINT_MAX_LENGTH) {
      throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Constraint value: " + checkOrDefaultValue +
          " exceeded maximum allowed length: " + CONSTRAINT_MAX_LENGTH));
    }

    // NOT NULL constraint could be enforced/enabled
    if (enable && childType != HiveParser.TOK_NOT_NULL && childType != HiveParser.TOK_DEFAULT_VALUE &&
        childType != HiveParser.TOK_CHECK_CONSTRAINT) {
      throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("ENABLE/ENFORCED feature not supported yet. " +
          "Please use DISABLE/NOT ENFORCED instead."));
    }
    if (validate) {
      throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("VALIDATE feature not supported yet. " +
          "Please use NOVALIDATE instead."));
    }

    List<ConstraintInfo> constraintInfos = new ArrayList<>();
    if (columnNames == null) {
      constraintInfos.add(new ConstraintInfo(null, constraintName, enable, validate, rely, checkOrDefaultValue));
    } else {
      for (String columnName : columnNames) {
        constraintInfos.add(new ConstraintInfo(columnName, constraintName, enable, validate, rely,
            checkOrDefaultValue));
      }
    }

    return constraintInfos;
  }

  static class ConstraintExpressionContext implements NodeProcessorCtx {
    private UnparseTranslator unparseTranslator;

    public ConstraintExpressionContext(UnparseTranslator unparseTranslator) {
      this.unparseTranslator = unparseTranslator;
    }

    public UnparseTranslator getUnparseTranslator() {
      return unparseTranslator;
    }
  }

  private static UnparseTranslator collectUnescapeIdentifierTranslations(ASTNode node)
      throws SemanticException {
    UnparseTranslator unparseTranslator = new UnparseTranslator(Quotation.BACKTICKS);
    unparseTranslator.enable();

    SetMultimap<Integer, SemanticNodeProcessor> astNodeToProcessor = HashMultimap.create();
    astNodeToProcessor.put(HiveParser.TOK_TABLE_OR_COL, new ColumnExprProcessor());
    astNodeToProcessor.put(HiveParser.DOT, new ColumnExprProcessor());
    NodeProcessorCtx nodeProcessorCtx = new ConstraintExpressionContext(unparseTranslator);

    CostLessRuleDispatcher costLessRuleDispatcher = new CostLessRuleDispatcher(
        (nd, stack, procCtx, nodeOutputs) -> null, astNodeToProcessor, nodeProcessorCtx);
    SemanticGraphWalker walker = new ExpressionWalker(costLessRuleDispatcher);
    walker.startWalking(Collections.singletonList(node), null);
    return unparseTranslator;
  }

  static class ColumnExprProcessor implements SemanticNodeProcessor {

    @Override
    public Object process(Node nd, Stack<Node> stack, NodeProcessorCtx procCtx, Object... nodeOutputs)
        throws SemanticException {
      UnparseTranslator unparseTranslator = ((ConstraintExpressionContext)procCtx).getUnparseTranslator();
      ASTNode tokTableOrColNode = (ASTNode) nd;
      for (int i = 0; i < tokTableOrColNode.getChildCount(); ++i) {
        ASTNode child = (ASTNode) tokTableOrColNode.getChild(i);
        if (child.getType() == HiveParser.Identifier) {
          unparseTranslator.addIdentifierTranslation(child);
        }
      }
      return null;
    }
  }

  private static final int DEFAULT_MAX_LEN = 255;

  /**
   * Validate and get the default value from the AST.
   * @param node AST node corresponding to default value
   * @return retrieve the default value and return it as string
   */
  private static String getDefaultValue(ASTNode node, ASTNode typeChild, TokenRewriteStream tokenStream)
      throws SemanticException{
    // first create expression from defaultValueAST
    TypeCheckCtx typeCheckCtx = new TypeCheckCtx(null);
    ExprNodeDesc defaultValExpr = ExprNodeTypeCheck.genExprNode(node, typeCheckCtx).get(node);

    if (defaultValExpr == null) {
      throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Invalid Default value!"));
    }

    //get default value to be be stored in metastore
    String defaultValueText  = tokenStream.toOriginalString(node.getTokenStartIndex(), node.getTokenStopIndex());

    if (defaultValueText.length() > DEFAULT_MAX_LEN) {
      throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Invalid Default value:  " + defaultValueText +
          " .Maximum character length allowed is " + DEFAULT_MAX_LEN +" ."));
    }

    // Make sure the default value expression type is exactly same as column's type.
    TypeInfo defaultValTypeInfo = defaultValExpr.getTypeInfo();
    TypeInfo colTypeInfo =
        TypeInfoUtils.getTypeInfoFromTypeString(BaseSemanticAnalyzer.getTypeStringFromAST(typeChild));
    if (!defaultValTypeInfo.equals(colTypeInfo)) {
      throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Invalid type: " +
          defaultValTypeInfo.getTypeName() + " for default value: " + defaultValueText + ". Please make sure that " +
          "the type is compatible with column type: " + colTypeInfo.getTypeName()));
    }

    // throw an error if default value isn't what hive allows
    if (!isDefaultValueAllowed(defaultValExpr)) {
      throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Invalid Default value: " + defaultValueText +
          ". DEFAULT only allows constant or function expressions"));
    }
    return defaultValueText;
  }


  private static boolean isDefaultValueAllowed(ExprNodeDesc defaultValExpr) {
    while (FunctionRegistry.isOpCast(defaultValExpr)) {
      defaultValExpr = defaultValExpr.getChildren().get(0);
    }

    if (defaultValExpr instanceof ExprNodeConstantDesc) {
      return true;
    }

    if (defaultValExpr instanceof ExprNodeGenericFuncDesc) {
      for (ExprNodeDesc argument : defaultValExpr.getChildren()) {
        if (!isDefaultValueAllowed(argument)) {
          return false;
        }
      }
      return true;
    }

    return false;
  }

  public static void processForeignKeys(TableName tableName, ASTNode node, List<SQLForeignKey> foreignKeys)
      throws SemanticException {
    // The ANTLR grammar looks like :
    // 1.  KW_CONSTRAINT idfr=identifier KW_FOREIGN KW_KEY fkCols=columnParenthesesList
    // KW_REFERENCES tabName=tableName parCols=columnParenthesesList
    // enableSpec=enableSpecification validateSpec=validateSpecification relySpec=relySpecification
    // -> ^(TOK_FOREIGN_KEY $idfr $fkCols $tabName $parCols $relySpec $enableSpec $validateSpec)
    // when the user specifies the constraint name (i.e. child.getChildCount() == 7)
    // 2.  KW_FOREIGN KW_KEY fkCols=columnParenthesesList
    // KW_REFERENCES tabName=tableName parCols=columnParenthesesList
    // enableSpec=enableSpecification validateSpec=validateSpecification relySpec=relySpecification
    // -> ^(TOK_FOREIGN_KEY $fkCols  $tabName $parCols $relySpec $enableSpec $validateSpec)
    // when the user does not specify the constraint name (i.e. child.getChildCount() == 6)
    String constraintName = null;
    boolean enable = true;
    boolean validate = true;
    boolean rely = false;
    int fkIndex = -1;
    for (int i = 0; i < node.getChildCount(); i++) {
      ASTNode grandChild = (ASTNode) node.getChild(i);
      int type = grandChild.getToken().getType();
      if (type == HiveParser.TOK_CONSTRAINT_NAME) {
        constraintName = BaseSemanticAnalyzer.unescapeIdentifier(grandChild.getChild(0).getText().toLowerCase());
      } else if (type == HiveParser.TOK_ENABLE) {
        enable = true;
        // validate is true by default if we enable the constraint
        validate = true;
      } else if (type == HiveParser.TOK_DISABLE) {
        enable = false;
        // validate is false by default if we disable the constraint
        validate = false;
      } else if (type == HiveParser.TOK_VALIDATE) {
        validate = true;
      } else if (type == HiveParser.TOK_NOVALIDATE) {
        validate = false;
      } else if (type == HiveParser.TOK_RELY) {
        rely = true;
      } else if (type == HiveParser.TOK_TABCOLNAME && fkIndex == -1) {
        fkIndex = i;
      }
    }
    if (enable) {
      throw new SemanticException(ErrorMsg.INVALID_FK_SYNTAX.getMsg("ENABLE feature not supported yet. " +
          "Please use DISABLE instead."));
    }
    if (validate) {
      throw new SemanticException(ErrorMsg.INVALID_FK_SYNTAX.getMsg("VALIDATE feature not supported yet. " +
          "Please use NOVALIDATE instead."));
    }

    int ptIndex = fkIndex + 1;
    int pkIndex = ptIndex + 1;
    if (node.getChild(fkIndex).getChildCount() != node.getChild(pkIndex).getChildCount()) {
      throw new SemanticException(ErrorMsg.INVALID_FK_SYNTAX.getMsg(
        " The number of foreign key columns should be same as number of parent key columns "));
    }

    TableName parentTblName = BaseSemanticAnalyzer.getQualifiedTableName((ASTNode) node.getChild(ptIndex));
    for (int j = 0; j < node.getChild(fkIndex).getChildCount(); j++) {
      SQLForeignKey sqlForeignKey = new SQLForeignKey();
      sqlForeignKey.setFktable_db(tableName.getDb());
      sqlForeignKey.setFktable_name(tableName.getTable());
      Tree fkgrandChild = node.getChild(fkIndex).getChild(j);
      BaseSemanticAnalyzer.checkColumnName(fkgrandChild.getText());
      sqlForeignKey.setFkcolumn_name(BaseSemanticAnalyzer.unescapeIdentifier(fkgrandChild.getText().toLowerCase()));
      sqlForeignKey.setPktable_db(parentTblName.getDb());
      sqlForeignKey.setPktable_name(parentTblName.getTable());
      Tree pkgrandChild = node.getChild(pkIndex).getChild(j);
      sqlForeignKey.setPkcolumn_name(BaseSemanticAnalyzer.unescapeIdentifier(pkgrandChild.getText().toLowerCase()));
      sqlForeignKey.setKey_seq(j+1);
      sqlForeignKey.setFk_name(constraintName);
      sqlForeignKey.setEnable_cstr(enable);
      sqlForeignKey.setValidate_cstr(validate);
      sqlForeignKey.setRely_cstr(rely);

      foreignKeys.add(sqlForeignKey);
    }
  }

  public static void validateCheckConstraint(List<FieldSchema> columns, List<SQLCheckConstraint> checkConstraints,
      Configuration conf) throws SemanticException{
    // create colinfo and then row resolver
    RowResolver rr = new RowResolver();
    for (FieldSchema column : columns) {
      ColumnInfo ci = new ColumnInfo(column.getName(),
          TypeInfoUtils.getTypeInfoFromTypeString(column.getType()), null, false);
      rr.put(null, column.getName(), ci);
    }

    TypeCheckCtx typeCheckCtx = new TypeCheckCtx(rr);
    // TypeCheckProcFactor expects typecheckctx to have unparse translator
    UnparseTranslator unparseTranslator = new UnparseTranslator(conf);
    typeCheckCtx.setUnparseTranslator(unparseTranslator);
    for (SQLCheckConstraint cc : checkConstraints) {
      try {
        ParseDriver parseDriver = new ParseDriver();
        ASTNode checkExprAST = parseDriver.parseExpression(cc.getCheck_expression());
        validateCheckExprAST(checkExprAST);
        Map<ASTNode, ExprNodeDesc> genExprs = ExprNodeTypeCheck.genExprNode(checkExprAST, typeCheckCtx);
        ExprNodeDesc checkExpr = genExprs.get(checkExprAST);
        if (checkExpr == null) {
          throw new SemanticException(
              ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Invalid type for CHECK constraint: ") + cc.getCheck_expression());
        }
        if (checkExpr.getTypeInfo().getTypeName() != serdeConstants.BOOLEAN_TYPE_NAME) {
          throw new SemanticException(
              ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Only boolean type is supported for CHECK constraint: ") +
              cc.getCheck_expression() + ". Found: " + checkExpr.getTypeInfo().getTypeName());
        }
        validateCheckExpr(checkExpr);
      } catch (Exception e) {
        throw new SemanticException(ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Invalid CHECK constraint expression: ") +
            cc.getCheck_expression() + ". " + e.getMessage(), e);
      }
    }
  }

  // given an ast node this method recursively goes over checkExpr ast. If it finds a node of type TOK_SUBQUERY_EXPR
  // it throws an error.
  // This method is used to validate check expression since check expression isn't allowed to have subquery
  private static void validateCheckExprAST(ASTNode checkExpr) throws SemanticException {
    if (checkExpr == null) {
      return;
    }
    if (checkExpr.getType() == HiveParser.TOK_SUBQUERY_EXPR) {
      throw new SemanticException(
          ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Subqueries are not allowed in Check Constraints"));
    }
    for (int i = 0; i < checkExpr.getChildCount(); i++) {
      validateCheckExprAST((ASTNode)checkExpr.getChild(i));
    }
  }

  // recursively go through expression and make sure the following:
  // * If expression is UDF it is not permanent UDF
  private static void validateCheckExpr(ExprNodeDesc checkExpr) throws SemanticException {
    if (checkExpr instanceof ExprNodeGenericFuncDesc) {
      ExprNodeGenericFuncDesc funcDesc = (ExprNodeGenericFuncDesc)checkExpr;
      boolean isBuiltIn = FunctionRegistry.isBuiltInFuncExpr(funcDesc);
      boolean isPermanent = FunctionRegistry.isPermanentFunction(funcDesc);
      if (!isBuiltIn && !isPermanent) {
        throw new SemanticException(
            ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Temporary UDFs are not allowed in Check Constraints"));
      }

      if (FunctionRegistry.impliesOrder(funcDesc.getFuncText())) {
        throw new SemanticException(
            ErrorMsg.INVALID_CSTR_SYNTAX.getMsg("Window functions are not allowed in Check Constraints"));
      }
    }

    if (checkExpr.getChildren() == null) {
      return;
    }
    for (ExprNodeDesc childExpr:checkExpr.getChildren()) {
      validateCheckExpr(childExpr);
    }
  }

  public static boolean hasEnabledOrValidatedConstraints(List<SQLNotNullConstraint> notNullConstraints,
      List<SQLDefaultConstraint> defaultConstraints, List<SQLCheckConstraint> checkConstraints) {
    if (notNullConstraints != null) {
      for (SQLNotNullConstraint nnC : notNullConstraints) {
        if (nnC.isEnable_cstr() || nnC.isValidate_cstr()) {
          return true;
        }
      }
    }

    if (defaultConstraints != null && !defaultConstraints.isEmpty()) {
      return true;
    }

    if (checkConstraints != null && !checkConstraints.isEmpty()) {
      return true;
    }

    return false;
  }
}
