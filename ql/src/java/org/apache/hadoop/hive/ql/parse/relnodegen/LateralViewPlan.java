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
package org.apache.hadoop.hive.ql.parse.relnodegen;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.JoinRelType;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.validate.SqlValidatorUtil;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.ColumnInfo;
import org.apache.hadoop.hive.ql.exec.Description;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.optimizer.calcite.TraitsUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveTableFunctionScan;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.ASTErrorUtils;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.BaseSemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.CalcitePlanner;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTFInline;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class LateralViewPlan {
  protected static final Logger LOG = LoggerFactory.getLogger(LateralViewPlan.class.getName());

  private final RelNode lateralViewRel;
  private final RowResolver outputRR = new RowResolver();

  public LateralViewPlan(ASTNode lateralView, RelOptCluster cluster, RelNode inputRel,
      RowResolver inputRR, Map<String, Integer> inputPosMap,
      RexNodeGenerator rexNodeGenerator) throws SemanticException {

    final RelDataTypeFactory dtFactory = cluster.getTypeFactory();
    final RexBuilder rexBuilder = cluster.getRexBuilder();
    final String inlineFunctionName =
        GenericUDTFInline.class.getAnnotation(Description.class).name();

    // Extract input refs. They will serve as input for the function invocation
    List<RexNode> inputRefs = Lists.transform(inputRel.getRowType().getFieldList(),
        input -> new RexInputRef(input.getIndex(), input.getType()));
    // Extract type for the arguments
    List<RelDataType> inputRefsTypes = new ArrayList<>();
    for (int i = 0; i < inputRefs.size(); i++) {
      inputRefsTypes.add(inputRefs.get(i).getType());
    }

    // 2) Generate HiveTableFunctionScan RelNode for lateral view
    // TODO: Support different functions (not only INLINE) with LATERAL VIEW JOIN
    // ^(TOK_LATERAL_VIEW ^(TOK_SELECT ^(TOK_SELEXPR ^(TOK_FUNCTION Identifier["inline"] valuesClause) identifier* tableAlias)))
    final ASTNode selExprClause =
        (ASTNode) lateralView.getChild(0).getChild(0);
    final ASTNode functionCall =
        (ASTNode) selExprClause.getChild(0);
    if (functionCall.getChild(0).getText().compareToIgnoreCase(inlineFunctionName) != 0) {
      throw new SemanticException("CBO only supports inline LVJ");
    }
    final ASTNode valuesClause =
        (ASTNode) functionCall.getChild(1);
    // Output types. They will be the concatenation of the input refs types and
    // the types of the expressions for the lateral view generated rows
    // Generate all expressions from lateral view
    RexCall valuesExpr = (RexCall) rexNodeGenerator.genRexNode(
        valuesClause, inputRR, false, false, cluster.getRexBuilder());
    RelDataType valuesRowType = valuesExpr.getType().getComponentType();
    List<RexNode> newStructExprs = new ArrayList<>();
    for (RexNode structExpr : valuesExpr.getOperands()) {
      RexCall structCall = (RexCall) structExpr;
      List<RexNode> exprs = new ArrayList<>(inputRefs);
      exprs.addAll(structCall.getOperands());
      newStructExprs.add(rexBuilder.makeCall(structCall.op, exprs));
    }
    RexNode convertedFinalValuesExpr =
        rexBuilder.makeCall(valuesExpr.op, newStructExprs);
    // The return type will be the concatenation of input type and original values type
    RelDataType retType = SqlValidatorUtil.deriveJoinRowType(inputRel.getRowType(),
        valuesRowType, JoinRelType.INNER, dtFactory, null, ImmutableList.of());

    // Create inline SQL operator
    FunctionInfo inlineFunctionInfo = FunctionRegistry.getFunctionInfo(inlineFunctionName);
    SqlOperator calciteOp = SqlFunctionConverter.getCalciteOperator(
        inlineFunctionName, inlineFunctionInfo.getGenericUDTF(),
        ImmutableList.copyOf(inputRefsTypes), retType);

    lateralViewRel = HiveTableFunctionScan.create(cluster, TraitsUtil.getDefaultTraitSet(cluster),
        ImmutableList.of(inputRel), rexBuilder.makeCall(calciteOp, convertedFinalValuesExpr),
        null, retType, null);

    // 3) Keep track of colname-to-posmap && RR for new op
    // Add all input columns
    if (!RowResolver.add(outputRR, inputRR)) {
      LOG.warn("Duplicates detected when adding columns to RR: see previous message");
    }
    // Add all columns from lateral view
    // First we extract the information that the query provides
    String tableAlias = null;
    List<String> columnAliases = new ArrayList<>();
    Set<String> uniqueNames = new HashSet<>();
    for (int i = 1; i < selExprClause.getChildren().size(); i++) {
      ASTNode child = (ASTNode) selExprClause.getChild(i);
      switch (child.getToken().getType()) {
        case HiveParser.TOK_TABALIAS:
          tableAlias = BaseSemanticAnalyzer.unescapeIdentifier(child.getChild(0).getText());
          break;
        default:
          String colAlias = BaseSemanticAnalyzer.unescapeIdentifier(child.getText());
          if (uniqueNames.contains(colAlias)) {
            // Column aliases defined by query for lateral view output are duplicated
            throw new SemanticException(ErrorMsg.COLUMN_ALIAS_ALREADY_EXISTS.getMsg(colAlias));
          }
          columnAliases.add(colAlias);
          uniqueNames.add(colAlias);
      }
    }
    if (tableAlias == null) {
      // Parser enforces that table alias is added, but check again
      throw new SemanticException("Alias should be specified LVJ");
    }
    if (!columnAliases.isEmpty() &&
            columnAliases.size() != valuesRowType.getFieldCount()) {
      // Number of columns in the aliases does not match with number of columns
      // generated by the lateral view
      throw new SemanticException(ErrorMsg.UDTF_ALIAS_MISMATCH.getMsg());
    }
    if (columnAliases.isEmpty()) {
      // Auto-generate column aliases
      for (int i = 0; i < valuesRowType.getFieldCount(); i++) {
        columnAliases.add(SemanticAnalyzer.getColumnInternalName(i));
      }
    }
    ListTypeInfo listTypeInfo = (ListTypeInfo) TypeConverter.convert(valuesExpr.getType()); // Array should have ListTypeInfo
    StructTypeInfo typeInfos = (StructTypeInfo) listTypeInfo.getListElementTypeInfo(); // Within the list, we extract types
    for (int i = 0, j = 0; i < columnAliases.size(); i++) {
      String internalColName;
      do {
        internalColName = SemanticAnalyzer.getColumnInternalName(j++);
      } while (inputRR.getPosition(internalColName) != -1);
      outputRR.put(tableAlias, columnAliases.get(i),
          new ColumnInfo(internalColName,  typeInfos.getAllStructFieldTypeInfos().get(i),
              tableAlias, false));
    }
  }

  public RelNode getRelNode() {
    return lateralViewRel;
  }

  public RowResolver getRowResolver() {
    return outputRR;
  }

  public static void validateLateralView(ASTNode lateralView) throws SemanticException {
    if (lateralView.getChildCount() != 2) {
      throw new SemanticException("Token Lateral View contains " + lateralView.getChildCount() +
          " children.");
    }
    ASTNode next = (ASTNode) lateralView.getChild(1);
    if (!CalcitePlanner.TABLE_ALIAS_TOKEN_TYPES.contains(next.getToken().getType()) &&
          HiveParser.TOK_LATERAL_VIEW != next.getToken().getType()) {
        throw new SemanticException(ASTErrorUtils.getMsg(
            ErrorMsg.LATERAL_VIEW_INVALID_CHILD.getMsg(), lateralView));
    }
  }
}
