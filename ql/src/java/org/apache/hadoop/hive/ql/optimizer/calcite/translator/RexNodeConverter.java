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
package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.CorrelationId;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexSubQuery;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.NlsString;
import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException.UnsupportedFeature;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSubquerySemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExtractDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFloorDate;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeSubQueryDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToChar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToVarchar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFUnixTimeStamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFWhen;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;

public class RexNodeConverter {

  private static class InputCtx {
    private final RelDataType                   calciteInpDataType;
    private final ImmutableMap<String, Integer> hiveNameToPosMap;
    private final RowResolver                   hiveRR;
    private final int                           offsetInCalciteSchema;

    private InputCtx(RelDataType calciteInpDataType, ImmutableMap<String, Integer> hiveNameToPosMap,
        RowResolver hiveRR, int offsetInCalciteSchema) {
      this.calciteInpDataType = calciteInpDataType;
      this.hiveNameToPosMap = hiveNameToPosMap;
      this.hiveRR = hiveRR;
      this.offsetInCalciteSchema = offsetInCalciteSchema;
    }
  };

  private final RelOptCluster           cluster;
  private final ImmutableList<InputCtx> inputCtxs;
  private final boolean                 flattenExpr;

  //outerRR belongs to outer query and is required to resolve correlated references
  private final RowResolver             outerRR;
  private final ImmutableMap<String, Integer> outerNameToPosMap;
  private int correlatedId;

  //Constructor used by HiveRexExecutorImpl
  public RexNodeConverter(RelOptCluster cluster) {
    this(cluster, new ArrayList<InputCtx>(), false);
  }

  //subqueries will need outer query's row resolver
  public RexNodeConverter(RelOptCluster cluster, RelDataType inpDataType,
                          ImmutableMap<String, Integer> outerNameToPosMap,
      ImmutableMap<String, Integer> nameToPosMap, RowResolver hiveRR, RowResolver outerRR, int offset, boolean flattenExpr, int correlatedId) {
    this.cluster = cluster;
    this.inputCtxs = ImmutableList.of(new InputCtx(inpDataType, nameToPosMap, hiveRR , offset));
    this.flattenExpr = flattenExpr;
    this.outerRR = outerRR;
    this.outerNameToPosMap = outerNameToPosMap;
    this.correlatedId = correlatedId;
  }

  public RexNodeConverter(RelOptCluster cluster, RelDataType inpDataType,
      ImmutableMap<String, Integer> nameToPosMap, int offset, boolean flattenExpr) {
    this.cluster = cluster;
    this.inputCtxs = ImmutableList.of(new InputCtx(inpDataType, nameToPosMap, null, offset));
    this.flattenExpr = flattenExpr;
    this.outerRR = null;
    this.outerNameToPosMap = null;
  }

  public RexNodeConverter(RelOptCluster cluster, List<InputCtx> inpCtxLst, boolean flattenExpr) {
    this.cluster = cluster;
    this.inputCtxs = ImmutableList.<InputCtx> builder().addAll(inpCtxLst).build();
    this.flattenExpr = flattenExpr;
    this.outerRR  = null;
    this.outerNameToPosMap = null;
  }

  public RexNode convert(ExprNodeDesc expr) throws SemanticException {
    if (expr instanceof ExprNodeGenericFuncDesc) {
      return convert((ExprNodeGenericFuncDesc) expr);
    } else if (expr instanceof ExprNodeConstantDesc) {
      return convert((ExprNodeConstantDesc) expr);
    } else if (expr instanceof ExprNodeColumnDesc) {
      return convert((ExprNodeColumnDesc) expr);
    } else if (expr instanceof ExprNodeFieldDesc) {
      return convert((ExprNodeFieldDesc) expr);
    } else if(expr instanceof  ExprNodeSubQueryDesc) {
      return convert((ExprNodeSubQueryDesc) expr);
    } else {
      throw new RuntimeException("Unsupported Expression");
    }
    // TODO: handle ExprNodeColumnListDesc
  }

  private RexNode convert(final ExprNodeSubQueryDesc subQueryDesc) throws  SemanticException {
    if(subQueryDesc.getType() == ExprNodeSubQueryDesc.SubqueryType.IN ) {
     /*
      * Check.5.h :: For In and Not In the SubQuery must implicitly or
      * explicitly only contain one select item.
      */
      if(subQueryDesc.getRexSubQuery().getRowType().getFieldCount() > 1) {
        throw new CalciteSubquerySemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
                "SubQuery can contain only 1 item in Select List."));
      }
      //create RexNode for LHS
      RexNode rexNodeLhs = convert(subQueryDesc.getSubQueryLhs());

      //create RexSubQuery node
      RexNode rexSubQuery = RexSubQuery.in(subQueryDesc.getRexSubQuery(),
                                              ImmutableList.<RexNode>of(rexNodeLhs) );
      return  rexSubQuery;
    }
    else if( subQueryDesc.getType() == ExprNodeSubQueryDesc.SubqueryType.EXISTS) {
      RexNode subQueryNode = RexSubQuery.exists(subQueryDesc.getRexSubQuery());
      return subQueryNode;
    }
    else if( subQueryDesc.getType() == ExprNodeSubQueryDesc.SubqueryType.SCALAR){
      if(subQueryDesc.getRexSubQuery().getRowType().getFieldCount() > 1) {
        throw new CalciteSubquerySemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
                "SubQuery can contain only 1 item in Select List."));
      }
      //create RexSubQuery node
      RexNode rexSubQuery = RexSubQuery.scalar(subQueryDesc.getRexSubQuery());
      return rexSubQuery;
    }

    else {
      throw new CalciteSubquerySemanticException(ErrorMsg.INVALID_SUBQUERY_EXPRESSION.getMsg(
              "Invalid subquery: " + subQueryDesc.getType()));
    }
  }

  private RexNode convert(final ExprNodeFieldDesc fieldDesc) throws SemanticException {
    RexNode rexNode = convert(fieldDesc.getDesc());
    if (rexNode.getType().isStruct()) {
      // regular case of accessing nested field in a column
      return cluster.getRexBuilder().makeFieldAccess(rexNode, fieldDesc.getFieldName(), true);
    } else {
      // This may happen for schema-less tables, where columns are dynamically
      // supplied by serdes.
      throw new CalciteSemanticException("Unexpected rexnode : "
          + rexNode.getClass().getCanonicalName(), UnsupportedFeature.Schema_less_table);
    }
  }

  private RexNode convert(ExprNodeGenericFuncDesc func) throws SemanticException {
    ExprNodeDesc tmpExprNode;
    RexNode tmpRN;

    List<RexNode> childRexNodeLst = new ArrayList<RexNode>();
    Builder<RelDataType> argTypeBldr = ImmutableList.<RelDataType> builder();

    // TODO: 1) Expand to other functions as needed 2) What about types other than primitive.
    TypeInfo tgtDT = null;
    GenericUDF tgtUdf = func.getGenericUDF();

    boolean isNumeric = (tgtUdf instanceof GenericUDFBaseBinary
        && func.getTypeInfo().getCategory() == Category.PRIMITIVE
        && (PrimitiveGrouping.NUMERIC_GROUP == PrimitiveObjectInspectorUtils.getPrimitiveGrouping(
            ((PrimitiveTypeInfo) func.getTypeInfo()).getPrimitiveCategory())));
    boolean isCompare = !isNumeric && tgtUdf instanceof GenericUDFBaseCompare;
    boolean isWhenCase = tgtUdf instanceof GenericUDFWhen || tgtUdf instanceof GenericUDFCase;
    boolean isTransformableTimeStamp = func.getGenericUDF() instanceof GenericUDFUnixTimeStamp &&
            func.getChildren().size() != 0;

    if (isNumeric) {
      tgtDT = func.getTypeInfo();

      assert func.getChildren().size() == 2;
      // TODO: checking 2 children is useless, compare already does that.
    } else if (isCompare && (func.getChildren().size() == 2)) {
      tgtDT = FunctionRegistry.getCommonClassForComparison(func.getChildren().get(0)
            .getTypeInfo(), func.getChildren().get(1).getTypeInfo());
    } else if (isWhenCase) {
      // If it is a CASE or WHEN, we need to check that children do not contain stateful functions
      // as they are not allowed
      if (checkForStatefulFunctions(func.getChildren())) {
        throw new SemanticException("Stateful expressions cannot be used inside of CASE");
      }
    } else if (isTransformableTimeStamp) {
      // unix_timestamp(args) -> to_unix_timestamp(args)
      func = ExprNodeGenericFuncDesc.newInstance(new GenericUDFToUnixTimeStamp(), func.getChildren());
    }

    for (ExprNodeDesc childExpr : func.getChildren()) {
      tmpExprNode = childExpr;
      if (tgtDT != null
          && TypeInfoUtils.isConversionRequiredForComparison(tgtDT, childExpr.getTypeInfo())) {
        if (isCompare) {
          // For compare, we will convert requisite children
          tmpExprNode = ParseUtils.createConversionCast(childExpr, (PrimitiveTypeInfo) tgtDT);
        } else if (isNumeric) {
          // For numeric, we'll do minimum necessary cast - if we cast to the type
          // of expression, bad things will happen.
          PrimitiveTypeInfo minArgType = ExprNodeDescUtils.deriveMinArgumentCast(childExpr, tgtDT);
          tmpExprNode = ParseUtils.createConversionCast(childExpr, minArgType);
        } else {
          throw new AssertionError("Unexpected " + tgtDT + " - not a numeric op or compare");
        }
      }

      argTypeBldr.add(TypeConverter.convert(tmpExprNode.getTypeInfo(), cluster.getTypeFactory()));
      tmpRN = convert(tmpExprNode);
      childRexNodeLst.add(tmpRN);
    }

    // See if this is an explicit cast.
    RexNode expr = null;
    RelDataType retType = null;
    expr = handleExplicitCast(func, childRexNodeLst);

    if (expr == null) {
      // This is not a cast; process the function.
      retType = TypeConverter.convert(func.getTypeInfo(), cluster.getTypeFactory());
      SqlOperator calciteOp = SqlFunctionConverter.getCalciteOperator(func.getFuncText(),
          func.getGenericUDF(), argTypeBldr.build(), retType);
      if (calciteOp.getKind() == SqlKind.CASE) {
        // If it is a case operator, we need to rewrite it
        childRexNodeLst = rewriteCaseChildren(func, childRexNodeLst);
      } else if (HiveExtractDate.ALL_FUNCTIONS.contains(calciteOp)) {
        // If it is a extract operator, we need to rewrite it
        childRexNodeLst = rewriteExtractDateChildren(calciteOp, childRexNodeLst);
      } else if (HiveFloorDate.ALL_FUNCTIONS.contains(calciteOp)) {
        // If it is a floor <date> operator, we need to rewrite it
        childRexNodeLst = rewriteFloorDateChildren(calciteOp, childRexNodeLst);
      }
      expr = cluster.getRexBuilder().makeCall(calciteOp, childRexNodeLst);
    } else {
      retType = expr.getType();
    }

    // TODO: Cast Function in Calcite have a bug where it infer type on cast throws
    // an exception
    if (flattenExpr && (expr instanceof RexCall)
        && !(((RexCall) expr).getOperator() instanceof SqlCastFunction)) {
      RexCall call = (RexCall) expr;
      expr = cluster.getRexBuilder().makeCall(retType, call.getOperator(),
          RexUtil.flatten(call.getOperands(), call.getOperator()));
    }

    return expr;
  }

  private boolean castExprUsingUDFBridge(GenericUDF gUDF) {
    boolean castExpr = false;
    if (gUDF != null && gUDF instanceof GenericUDFBridge) {
      String udfClassName = ((GenericUDFBridge) gUDF).getUdfClassName();
      if (udfClassName != null) {
        int sp = udfClassName.lastIndexOf('.');
        // TODO: add method to UDFBridge to say if it is a cast func
        if (sp >= 0 & (sp + 1) < udfClassName.length()) {
          udfClassName = udfClassName.substring(sp + 1);
          if (udfClassName.equals("UDFToBoolean") || udfClassName.equals("UDFToByte")
              || udfClassName.equals("UDFToDouble") || udfClassName.equals("UDFToInteger")
              || udfClassName.equals("UDFToLong") || udfClassName.equals("UDFToShort")
              || udfClassName.equals("UDFToFloat") || udfClassName.equals("UDFToString"))
            castExpr = true;
        }
      }
    }

    return castExpr;
  }

  private RexNode handleExplicitCast(ExprNodeGenericFuncDesc func, List<RexNode> childRexNodeLst)
      throws CalciteSemanticException {
    RexNode castExpr = null;

    if (childRexNodeLst != null && childRexNodeLst.size() == 1) {
      GenericUDF udf = func.getGenericUDF();
      if ((udf instanceof GenericUDFToChar) || (udf instanceof GenericUDFToVarchar)
          || (udf instanceof GenericUDFToDecimal) || (udf instanceof GenericUDFToDate)
          // Calcite can not specify the scale for timestamp. As a result, all
          // the millisecond part will be lost
          || (udf instanceof GenericUDFTimestamp)
          || (udf instanceof GenericUDFToBinary) || castExprUsingUDFBridge(udf)) {
        castExpr = cluster.getRexBuilder().makeAbstractCast(
            TypeConverter.convert(func.getTypeInfo(), cluster.getTypeFactory()),
            childRexNodeLst.get(0));
      }
    }

    return castExpr;
  }

  /*
   * Hive syntax allows to define CASE expressions in two ways:
   * - CASE a WHEN b THEN c [WHEN d THEN e]* [ELSE f] END (translated into the
   *   "case" function, ELSE clause is optional)
   * - CASE WHEN a THEN b [WHEN c THEN d]* [ELSE e] END (translated into the
   *   "when" function, ELSE clause is optional)
   * However, Calcite only has the equivalent to the "when" Hive function. Thus,
   * we need to transform the "case" function into "when". Further, ELSE clause is
   * not optional in Calcite.
   *
   * Example. Consider the following statement:
   * CASE x + y WHEN 1 THEN 'fee' WHEN 2 THEN 'fie' END
   * It will be transformed into:
   * CASE WHEN =(x + y, 1) THEN 'fee' WHEN =(x + y, 2) THEN 'fie' ELSE null END
   */
  private List<RexNode> rewriteCaseChildren(ExprNodeGenericFuncDesc func, List<RexNode> childRexNodeLst)
      throws SemanticException {
    List<RexNode> newChildRexNodeLst = new ArrayList<RexNode>();
    if (FunctionRegistry.getNormalizedFunctionName(func.getFuncText()).equals("case")) {
      RexNode firstPred = childRexNodeLst.get(0);
      int length = childRexNodeLst.size() % 2 == 1 ?
              childRexNodeLst.size() : childRexNodeLst.size() - 1;
      for (int i = 1; i < length; i++) {
        if (i % 2 == 1) {
          // We rewrite it
          newChildRexNodeLst.add(
                  cluster.getRexBuilder().makeCall(
                          SqlStdOperatorTable.EQUALS, firstPred, childRexNodeLst.get(i)));
        } else {
          newChildRexNodeLst.add(childRexNodeLst.get(i));
        }
      }
      // The else clause
      if (length != childRexNodeLst.size()) {
        newChildRexNodeLst.add(childRexNodeLst.get(childRexNodeLst.size()-1));
      }
    } else {
      newChildRexNodeLst.addAll(childRexNodeLst);
    }
    // Calcite always needs the else clause to be defined explicitly
    if (newChildRexNodeLst.size() % 2 == 0) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeNullLiteral(
              newChildRexNodeLst.get(newChildRexNodeLst.size()-1).getType().getSqlTypeName()));
    }
    return newChildRexNodeLst;
  }

  private List<RexNode> rewriteExtractDateChildren(SqlOperator op, List<RexNode> childRexNodeLst)
      throws SemanticException {
    List<RexNode> newChildRexNodeLst = new ArrayList<RexNode>();
    if (op == HiveExtractDate.YEAR) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.YEAR));
    } else if (op == HiveExtractDate.QUARTER) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.QUARTER));
    } else if (op == HiveExtractDate.MONTH) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.MONTH));
    } else if (op == HiveExtractDate.WEEK) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.WEEK));
    } else if (op == HiveExtractDate.DAY) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.DAY));
    } else if (op == HiveExtractDate.HOUR) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.HOUR));
    } else if (op == HiveExtractDate.MINUTE) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.MINUTE));
    } else if (op == HiveExtractDate.SECOND) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.SECOND));
    }
    assert childRexNodeLst.size() == 1;
    newChildRexNodeLst.add(childRexNodeLst.get(0));
    return newChildRexNodeLst;
  }

  private List<RexNode> rewriteFloorDateChildren(SqlOperator op, List<RexNode> childRexNodeLst)
      throws SemanticException {
    List<RexNode> newChildRexNodeLst = new ArrayList<RexNode>();
    assert childRexNodeLst.size() == 1;
    newChildRexNodeLst.add(childRexNodeLst.get(0));
    if (op == HiveFloorDate.YEAR) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.YEAR));
    } else if (op == HiveFloorDate.QUARTER) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.QUARTER));
    } else if (op == HiveFloorDate.MONTH) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.MONTH));
    } else if (op == HiveFloorDate.WEEK) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.WEEK));
    } else if (op == HiveFloorDate.DAY) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.DAY));
    } else if (op == HiveFloorDate.HOUR) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.HOUR));
    } else if (op == HiveFloorDate.MINUTE) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.MINUTE));
    } else if (op == HiveFloorDate.SECOND) {
      newChildRexNodeLst.add(cluster.getRexBuilder().makeFlag(TimeUnitRange.SECOND));
    }
    return newChildRexNodeLst;
  }

  private static boolean checkForStatefulFunctions(List<ExprNodeDesc> list) {
    for (ExprNodeDesc node : list) {
      if (node instanceof ExprNodeGenericFuncDesc) {
        GenericUDF nodeUDF = ((ExprNodeGenericFuncDesc) node).getGenericUDF();
        // Stateful?
        if (FunctionRegistry.isStateful(nodeUDF)) {
          return true;
        }
        if (node.getChildren() != null && !node.getChildren().isEmpty() &&
                checkForStatefulFunctions(node.getChildren())) {
          return true;
        }
      }
    }
    return false;
  }

  private InputCtx getInputCtx(ExprNodeColumnDesc col) throws SemanticException {
    InputCtx ctxLookingFor = null;

    if (inputCtxs.size() == 1 && inputCtxs.get(0).hiveRR == null) {
      ctxLookingFor = inputCtxs.get(0);
    } else {
      String tableAlias = col.getTabAlias();
      String colAlias = col.getColumn();
      int noInp = 0;
      for (InputCtx ic : inputCtxs) {
        if (tableAlias == null || ic.hiveRR.hasTableAlias(tableAlias)) {
          if (ic.hiveRR.getPosition(colAlias) >= 0) {
            ctxLookingFor = ic;
            noInp++;
          }
        }
      }

      if (noInp > 1)
        throw new RuntimeException("Ambiguous column mapping");
    }

    return ctxLookingFor;
  }

  protected RexNode convert(ExprNodeColumnDesc col) throws SemanticException {
    //if this is co-rrelated we need to make RexCorrelVariable(with id and type)
    // id and type should be retrieved from outerRR
    InputCtx ic = getInputCtx(col);
    if(ic == null) {
      // we have correlated column, build data type from outer rr
      RelDataType rowType = TypeConverter.getType(cluster, this.outerRR, null);
      if (this.outerNameToPosMap.get(col.getColumn()) == null) {
        throw new SemanticException(ErrorMsg.INVALID_COLUMN_NAME.getMsg(col.getColumn()));
      }

      int pos = this.outerNameToPosMap.get(col.getColumn());
      CorrelationId colCorr = new CorrelationId(this.correlatedId);
      RexNode corExpr = cluster.getRexBuilder().makeCorrel(rowType, colCorr);
      return cluster.getRexBuilder().makeFieldAccess(corExpr, pos);
    }
    int pos = ic.hiveNameToPosMap.get(col.getColumn());
    return cluster.getRexBuilder().makeInputRef(
        ic.calciteInpDataType.getFieldList().get(pos).getType(), pos + ic.offsetInCalciteSchema);
  }

  private static final BigInteger MIN_LONG_BI = BigInteger.valueOf(Long.MIN_VALUE),
      MAX_LONG_BI = BigInteger.valueOf(Long.MAX_VALUE);

  private static NlsString asUnicodeString(String text) {
    return new NlsString(text, ConversionUtil.NATIVE_UTF16_CHARSET_NAME, SqlCollation.IMPLICIT);
  }

  protected RexNode convert(ExprNodeConstantDesc literal) throws CalciteSemanticException {
    RexBuilder rexBuilder = cluster.getRexBuilder();
    RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
    PrimitiveTypeInfo hiveType = (PrimitiveTypeInfo) literal.getTypeInfo();
    RelDataType calciteDataType = TypeConverter.convert(hiveType, dtFactory);

    PrimitiveCategory hiveTypeCategory = hiveType.getPrimitiveCategory();

    ConstantObjectInspector coi = literal.getWritableObjectInspector();
    Object value = ObjectInspectorUtils.copyToStandardJavaObject(coi.getWritableConstantValue(),
        coi);

    RexNode calciteLiteral = null;
    // If value is null, the type should also be VOID.
    if (value == null) {
      hiveTypeCategory = PrimitiveCategory.VOID;
    }
    // TODO: Verify if we need to use ConstantObjectInspector to unwrap data
    switch (hiveTypeCategory) {
    case BOOLEAN:
      calciteLiteral = rexBuilder.makeLiteral(((Boolean) value).booleanValue());
      break;
    case BYTE:
      calciteLiteral = rexBuilder.makeExactLiteral(new BigDecimal((Byte) value), calciteDataType);
      break;
    case SHORT:
      calciteLiteral = rexBuilder.makeExactLiteral(new BigDecimal((Short) value), calciteDataType);
      break;
    case INT:
      calciteLiteral = rexBuilder.makeExactLiteral(new BigDecimal((Integer) value));
      break;
    case LONG:
      calciteLiteral = rexBuilder.makeBigintLiteral(new BigDecimal((Long) value));
      break;
    // TODO: is Decimal an exact numeric or approximate numeric?
    case DECIMAL:
      if (value instanceof HiveDecimal) {
        value = ((HiveDecimal) value).bigDecimalValue();
      } else if (value instanceof Decimal128) {
        value = ((Decimal128) value).toBigDecimal();
      }
      if (value == null) {
        // We have found an invalid decimal value while enforcing precision and
        // scale. Ideally,
        // we would replace it with null here, which is what Hive does. However,
        // we need to plumb
        // this thru up somehow, because otherwise having different expression
        // type in AST causes
        // the plan generation to fail after CBO, probably due to some residual
        // state in SA/QB.
        // For now, we will not run CBO in the presence of invalid decimal
        // literals.
        throw new CalciteSemanticException("Expression " + literal.getExprString()
            + " is not a valid decimal", UnsupportedFeature.Invalid_decimal);
        // TODO: return createNullLiteral(literal);
      }
      BigDecimal bd = (BigDecimal) value;
      BigInteger unscaled = bd.unscaledValue();
      if (unscaled.compareTo(MIN_LONG_BI) >= 0 && unscaled.compareTo(MAX_LONG_BI) <= 0) {
        calciteLiteral = rexBuilder.makeExactLiteral(bd);
      } else {
        // CBO doesn't support unlimited precision decimals. In practice, this
        // will work...
        // An alternative would be to throw CboSemanticException and fall back
        // to no CBO.
        RelDataType relType = cluster.getTypeFactory().createSqlType(SqlTypeName.DECIMAL,
            unscaled.toString().length(), bd.scale());
        calciteLiteral = rexBuilder.makeExactLiteral(bd, relType);
      }
      break;
    case FLOAT:
      calciteLiteral = rexBuilder.makeApproxLiteral(
              new BigDecimal(Float.toString((Float)value)), calciteDataType);
      break;
    case DOUBLE:
      // TODO: The best solution is to support NaN in expression reduction.
      if (Double.isNaN((Double) value)) {
        throw new CalciteSemanticException("NaN", UnsupportedFeature.Invalid_decimal);
      }
      calciteLiteral = rexBuilder.makeApproxLiteral(
              new BigDecimal(Double.toString((Double)value)), calciteDataType);
      break;
    case CHAR:
      if (value instanceof HiveChar) {
        value = ((HiveChar) value).getValue();
      }
      calciteLiteral = rexBuilder.makeCharLiteral(asUnicodeString((String) value));
      break;
    case VARCHAR:
      if (value instanceof HiveVarchar) {
        value = ((HiveVarchar) value).getValue();
      }
      calciteLiteral = rexBuilder.makeCharLiteral(asUnicodeString((String) value));
      break;
    case STRING:
      calciteLiteral = rexBuilder.makeCharLiteral(asUnicodeString((String) value));
      break;
    case DATE:
      Calendar cal = new GregorianCalendar();
      cal.setTime((Date) value);
      calciteLiteral = rexBuilder.makeDateLiteral(cal);
      break;
    case TIMESTAMP:
      Calendar c = null;
      if (value instanceof Calendar) {
        c = (Calendar)value;
      } else {
        c = Calendar.getInstance();
        c.setTimeInMillis(((Timestamp)value).getTime());
      }
      calciteLiteral = rexBuilder.makeTimestampLiteral(c, RelDataType.PRECISION_NOT_SPECIFIED);
      break;
    case INTERVAL_YEAR_MONTH:
      // Calcite year-month literal value is months as BigDecimal
      BigDecimal totalMonths = BigDecimal.valueOf(((HiveIntervalYearMonth) value).getTotalMonths());
      calciteLiteral = rexBuilder.makeIntervalLiteral(totalMonths,
          new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, new SqlParserPos(1,1)));
      break;
    case INTERVAL_DAY_TIME:
      // Calcite day-time interval is millis value as BigDecimal
      // Seconds converted to millis
      BigDecimal secsValueBd = BigDecimal
       .valueOf(((HiveIntervalDayTime) value).getTotalSeconds() * 1000);
      // Nanos converted to millis
       BigDecimal nanosValueBd = BigDecimal.valueOf(((HiveIntervalDayTime)
       value).getNanos(), 6);
       calciteLiteral =
       rexBuilder.makeIntervalLiteral(secsValueBd.add(nanosValueBd),
       new SqlIntervalQualifier(TimeUnit.MILLISECOND, null, new
       SqlParserPos(1, 1)));
       break;
    case VOID:
      calciteLiteral = cluster.getRexBuilder().makeLiteral(null,
          cluster.getTypeFactory().createSqlType(SqlTypeName.NULL), true);
      break;
    case BINARY:
    case UNKNOWN:
    default:
      throw new RuntimeException("UnSupported Literal");
    }

    return calciteLiteral;
  }

  public static RexNode convert(RelOptCluster cluster, ExprNodeDesc joinCondnExprNode,
      List<RelNode> inputRels, LinkedHashMap<RelNode, RowResolver> relToHiveRR,
      Map<RelNode, ImmutableMap<String, Integer>> relToHiveColNameCalcitePosMap, boolean flattenExpr)
      throws SemanticException {
    List<InputCtx> inputCtxLst = new ArrayList<InputCtx>();

    int offSet = 0;
    for (RelNode r : inputRels) {
      inputCtxLst.add(new InputCtx(r.getRowType(), relToHiveColNameCalcitePosMap.get(r), relToHiveRR
          .get(r), offSet));
      offSet += r.getRowType().getFieldCount();
    }

    return (new RexNodeConverter(cluster, inputCtxLst, flattenExpr)).convert(joinCondnExprNode);
  }
}
