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
package org.apache.hadoop.hive.ql.optimizer.calcite.translator;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;

import java.nio.charset.Charset;
import org.apache.calcite.avatica.util.TimeUnit;
import org.apache.calcite.avatica.util.TimeUnitRange;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlCollation;
import org.apache.calcite.sql.SqlIntervalQualifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.ConversionUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimestampString;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZ;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException.UnsupportedFeature;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveCalciteUtil;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExtractDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFloorDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveToDateSqlOperator;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.type.ExprNodeTypeCheck;
import org.apache.hadoop.hive.ql.parse.type.RexNodeExprFactory;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFCase;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFTimestamp;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToArray;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToChar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToMap;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToString;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToStruct;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToTimestampLocalTZ;
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

import java.math.BigDecimal;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;

/**
 * Class that contains logic to translate Hive expressions ({@link ExprNodeDesc})
 * into Calcite expressions ({@link RexNode}).
 */
public class RexNodeConverter {

  private final RexBuilder rexBuilder;
  private final RelDataTypeFactory typeFactory;


  /**
   * Constructor used by HiveRexExecutorImpl.
   */
  public RexNodeConverter(RexBuilder rexBuilder, RelDataTypeFactory typeFactory) {
    this.rexBuilder = rexBuilder;
    this.typeFactory = typeFactory;
  }

  public RexNode convert(ExprNodeDesc expr) throws SemanticException {
    if (expr instanceof ExprNodeGenericFuncDesc) {
      return convert((ExprNodeGenericFuncDesc) expr);
    } else if (expr instanceof ExprNodeConstantDesc) {
      return convert((ExprNodeConstantDesc) expr);
    } else if (expr instanceof ExprNodeFieldDesc) {
      return convert((ExprNodeFieldDesc) expr);
    } else {
      throw new RuntimeException("Unsupported Expression");
    }
    // TODO: Handle ExprNodeColumnDesc, ExprNodeColumnListDesc
  }

  private RexNode convert(final ExprNodeFieldDesc fieldDesc) throws SemanticException {
    RexNode rexNode = convert(fieldDesc.getDesc());
    if (rexNode.getType().isStruct()) {
      // regular case of accessing nested field in a column
      return rexBuilder.makeFieldAccess(rexNode, fieldDesc.getFieldName(), true);
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

    List<RexNode> childRexNodeLst = new ArrayList<>();
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
        !func.getChildren().isEmpty();
    boolean isBetween = !isNumeric && tgtUdf instanceof GenericUDFBetween;
    boolean isIN = !isNumeric && tgtUdf instanceof GenericUDFIn;
    boolean isAllPrimitive = true;

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
    } else if (isBetween) {
      assert func.getChildren().size() == 4;
      // We skip first child as is not involved (is the revert boolean)
      // The target type needs to account for all 3 operands
      tgtDT = FunctionRegistry.getCommonClassForComparison(
          func.getChildren().get(1).getTypeInfo(),
          FunctionRegistry.getCommonClassForComparison(
              func.getChildren().get(2).getTypeInfo(),
              func.getChildren().get(3).getTypeInfo()));
    } else if (isIN) {
      // We're only considering the first element of the IN list for the type
      assert func.getChildren().size() > 1;
      tgtDT = FunctionRegistry.getCommonClassForComparison(func.getChildren().get(0)
          .getTypeInfo(), func.getChildren().get(1).getTypeInfo());
    }

    for (int i =0; i < func.getChildren().size(); ++i) {
      ExprNodeDesc childExpr = func.getChildren().get(i);
      tmpExprNode = childExpr;
      if (tgtDT != null && tgtDT.getCategory() == Category.PRIMITIVE
          && TypeInfoUtils.isConversionRequiredForComparison(tgtDT, childExpr.getTypeInfo())) {
        if (isCompare || isBetween || isIN) {
          // For compare, we will convert requisite children
          // For BETWEEN skip the first child (the revert boolean)
          if (!isBetween || i > 0) {
            tmpExprNode = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
                .createConversionCast(childExpr, (PrimitiveTypeInfo) tgtDT);
          }
        } else if (isNumeric) {
          // For numeric, we'll do minimum necessary cast - if we cast to the type
          // of expression, bad things will happen.
          PrimitiveTypeInfo minArgType = ExprNodeDescUtils.deriveMinArgumentCast(childExpr, tgtDT);
          tmpExprNode = ExprNodeTypeCheck.getExprNodeDefaultExprProcessor()
              .createConversionCast(childExpr, minArgType);
        } else {
          throw new AssertionError("Unexpected " + tgtDT + " - not a numeric op or compare");
        }
      }

      isAllPrimitive =
          isAllPrimitive && tmpExprNode.getTypeInfo().getCategory() == Category.PRIMITIVE;
      argTypeBldr.add(TypeConverter.convert(tmpExprNode.getTypeInfo(), typeFactory));
      tmpRN = convert(tmpExprNode);
      childRexNodeLst.add(tmpRN);
    }

    // See if this is an explicit cast.
    RelDataType retType = TypeConverter.convert(func.getTypeInfo(), typeFactory);
    RexNode expr = handleExplicitCast(func.getGenericUDF(), retType, childRexNodeLst,
        rexBuilder);

    if (expr == null) {
      // This is not a cast; process the function.
      SqlOperator calciteOp = SqlFunctionConverter.getCalciteOperator(func.getFuncText(),
          func.getGenericUDF(), argTypeBldr.build(), retType);
      if (calciteOp.getKind() == SqlKind.CASE) {
        // If it is a case operator, we need to rewrite it
        childRexNodeLst = rewriteCaseChildren(func.getFuncText(), childRexNodeLst, rexBuilder);
        // Adjust branch types by inserting explicit casts if the actual is ambiguous
        childRexNodeLst = adjustCaseBranchTypes(childRexNodeLst, retType, rexBuilder);
      } else if (HiveExtractDate.ALL_FUNCTIONS.contains(calciteOp)) {
        // If it is a extract operator, we need to rewrite it
        childRexNodeLst = rewriteExtractDateChildren(calciteOp, childRexNodeLst, rexBuilder);
      } else if (HiveFloorDate.ALL_FUNCTIONS.contains(calciteOp)) {
        // If it is a floor <date> operator, we need to rewrite it
        childRexNodeLst = rewriteFloorDateChildren(calciteOp, childRexNodeLst, rexBuilder);
      } else if (calciteOp.getKind() == SqlKind.IN && isAllPrimitive) {
        if (childRexNodeLst.size() == 2) {
          // if it is a single item in an IN clause, transform A IN (B) to A = B
          // from IN [A,B] => EQUALS [A,B]
          // except complex types
          calciteOp = SqlStdOperatorTable.EQUALS;
        } else if (RexUtil.isReferenceOrAccess(childRexNodeLst.get(0), true)){
          // if it is more than an single item in an IN clause,
          // transform from IN [A,B,C] => OR [EQUALS [A,B], EQUALS [A,C]]
          // except complex types
          // Rewrite to OR is done only if number of operands are less than
          // the threshold configured
          childRexNodeLst = rewriteInClauseChildren(calciteOp, childRexNodeLst, rexBuilder);
          calciteOp = SqlStdOperatorTable.OR;
        }
      } else if (calciteOp.getKind() == SqlKind.COALESCE &&
          childRexNodeLst.size() > 1) {
        // Rewrite COALESCE as a CASE
        // This allows to be further reduced to OR, if possible
        calciteOp = SqlStdOperatorTable.CASE;
        childRexNodeLst = rewriteCoalesceChildren(childRexNodeLst, rexBuilder);
        // Adjust branch types by inserting explicit casts if the actual is ambiguous
        childRexNodeLst = adjustCaseBranchTypes(childRexNodeLst, retType, rexBuilder);
      } else if (calciteOp == HiveToDateSqlOperator.INSTANCE) {
        childRexNodeLst = rewriteToDateChildren(childRexNodeLst, rexBuilder);
      } else if (calciteOp.getKind() == SqlKind.BETWEEN) {
        assert childRexNodeLst.get(0).isAlwaysTrue() || childRexNodeLst.get(0).isAlwaysFalse();
        childRexNodeLst = rewriteBetweenChildren(childRexNodeLst, rexBuilder);
        if (childRexNodeLst.get(0).isAlwaysTrue()) {
          calciteOp = SqlStdOperatorTable.OR;
        } else {
          calciteOp = SqlStdOperatorTable.AND;
        }
      }
      expr = rexBuilder.makeCall(retType, calciteOp, childRexNodeLst);
    } else {
      retType = expr.getType();
    }

    // TODO: Cast Function in Calcite have a bug where it infer type on cast throws
    // an exception
    if (expr instanceof RexCall
        && !(((RexCall) expr).getOperator() instanceof SqlCastFunction)) {
      RexCall call = (RexCall) expr;
      expr = rexBuilder.makeCall(retType, call.getOperator(),
          RexUtil.flatten(call.getOperands(), call.getOperator()));
    }

    return expr;
  }

  private static boolean castExprUsingUDFBridge(GenericUDF gUDF) {
    boolean castExpr = false;
    if (gUDF instanceof GenericUDFBridge) {
      String udfClassName = ((GenericUDFBridge) gUDF).getUdfClassName();
      if (udfClassName != null) {
        int sp = udfClassName.lastIndexOf('.');
        // TODO: add method to UDFBridge to say if it is a cast func
        if (sp >= 0 & (sp + 1) < udfClassName.length()) {
          udfClassName = udfClassName.substring(sp + 1);
          if (udfClassName.equals("UDFToBoolean") || udfClassName.equals("UDFToByte")
              || udfClassName.equals("UDFToDouble") || udfClassName.equals("UDFToInteger")
              || udfClassName.equals("UDFToLong") || udfClassName.equals("UDFToShort")
              || udfClassName.equals("UDFToFloat")) {
            castExpr = true;
          }
        }
      }
    }

    return castExpr;
  }

  public static RexNode handleExplicitCast(GenericUDF udf, RelDataType returnType, List<RexNode> childRexNodeLst,
      RexBuilder rexBuilder) {
    RexNode castExpr = null;

    if (childRexNodeLst != null && childRexNodeLst.size() == 1) {
      if ((udf instanceof GenericUDFToChar) || (udf instanceof GenericUDFToVarchar)
          || (udf instanceof GenericUDFToString)
          || (udf instanceof GenericUDFToDecimal) || (udf instanceof GenericUDFToDate)
          || (udf instanceof GenericUDFTimestamp) || (udf instanceof GenericUDFToTimestampLocalTZ)
          || (udf instanceof GenericUDFToBinary) || castExprUsingUDFBridge(udf)
          || (udf instanceof GenericUDFToMap)
          || (udf instanceof GenericUDFToArray)
          || (udf instanceof GenericUDFToStruct)) {
        castExpr = rexBuilder.makeAbstractCast(returnType, childRexNodeLst.get(0));
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
  public static List<RexNode> rewriteCaseChildren(String funcText, List<RexNode> childRexNodeLst,
      RexBuilder rexBuilder) throws SemanticException {
    List<RexNode> newChildRexNodeLst = new ArrayList<>();
    if (FunctionRegistry.getNormalizedFunctionName(funcText).equals("case")) {
      RexNode firstPred = childRexNodeLst.get(0);
      int length = childRexNodeLst.size() % 2 == 1 ?
          childRexNodeLst.size() : childRexNodeLst.size() - 1;
      for (int i = 1; i < length; i++) {
        if (i % 2 == 1) {
          // We rewrite it
          RexNode node = childRexNodeLst.get(i);
          if (node.isA(SqlKind.LITERAL) && !node.getType().equals(firstPred.getType())) {
            // this effectively changes the type of the literal to that of the predicate
            // to which it is anyway going to be compared with
            // ex: CASE WHEN =($0:SMALLINT, 1:INTEGER) ... => CASE WHEN =($0:SMALLINT, 1:SMALLINT)
            node = rexBuilder.makeCast(firstPred.getType(), node);
          }
          newChildRexNodeLst.add(
              rexBuilder.makeCall(SqlStdOperatorTable.EQUALS, firstPred, node));
        } else {
          newChildRexNodeLst.add(childRexNodeLst.get(i));
        }
      }
      // The else clause
      if (length != childRexNodeLst.size()) {
        newChildRexNodeLst.add(childRexNodeLst.get(childRexNodeLst.size()-1));
      }
    } else {
      for (int i = 0; i < childRexNodeLst.size(); i++) {
        RexNode child = childRexNodeLst.get(i);
        if (RexUtil.isNull(child)) {
          if (i % 2 == 0 && i != childRexNodeLst.size() - 1) {
            if (SqlTypeName.NULL.equals(child.getType().getSqlTypeName())) {
              child = rexBuilder.makeNullLiteral(rexBuilder.getTypeFactory().createSqlType(SqlTypeName.BOOLEAN));
            }
          } else {
            // this is needed to provide typed NULLs which were working before
            // example: IF(false, array(1,2,3), NULL)
            if (!RexUtil.isNull(childRexNodeLst.get(1))) {
              child = rexBuilder.makeCast(childRexNodeLst.get(1).getType(), child);
            }
          }
        }
        newChildRexNodeLst.add(child);
      }
    }
    // Calcite always needs the else clause to be defined explicitly
    if (newChildRexNodeLst.size() % 2 == 0) {
      newChildRexNodeLst.add(rexBuilder.makeNullLiteral(
          newChildRexNodeLst.get(newChildRexNodeLst.size()-1).getType()));
    }
    return newChildRexNodeLst;
  }

  /**
   * Adds explicit casts if Calcite's type system could not resolve the CASE branches to a common type.
   *
   * Calcite is more stricter than hive w.r.t type conversions.
   * If a CASE has branches with string/int/boolean branch types; there is no common type.
   */
  public static List<RexNode> adjustCaseBranchTypes(List<RexNode> nodes, RelDataType retType, RexBuilder rexBuilder) {
    List<RexNode> newNodes = new ArrayList<>();
    for (int i = 0; i < nodes.size(); i++) {
      RexNode node = nodes.get(i);
      if ((i % 2 == 1 || i == nodes.size() - 1)
          && !node.getType().getSqlTypeName().equals(retType.getSqlTypeName())) {
        newNodes.add(rexBuilder.makeCast(retType, node));
      } else {
        newNodes.add(node);
      }
    }
    return newNodes;
  }

  public static List<RexNode> rewriteExtractDateChildren(SqlOperator op, List<RexNode> childRexNodeLst,
      RexBuilder rexBuilder) {
    List<RexNode> newChildRexNodeLst = new ArrayList<>(2);
    final boolean isTimestampLevel;
    if (op == HiveExtractDate.YEAR) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.YEAR));
      isTimestampLevel = false;
    } else if (op == HiveExtractDate.QUARTER) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.QUARTER));
      isTimestampLevel = false;
    } else if (op == HiveExtractDate.MONTH) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.MONTH));
      isTimestampLevel = false;
    } else if (op == HiveExtractDate.WEEK) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.WEEK));
      isTimestampLevel = false;
    } else if (op == HiveExtractDate.DAY) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.DAY));
      isTimestampLevel = false;
    } else if (op == HiveExtractDate.HOUR) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.HOUR));
      isTimestampLevel = true;
    } else if (op == HiveExtractDate.MINUTE) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.MINUTE));
      isTimestampLevel = true;
    } else if (op == HiveExtractDate.SECOND) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.SECOND));
      isTimestampLevel = true;
    } else {
      isTimestampLevel = false;
    }

    final RexNode child = Iterables.getOnlyElement(childRexNodeLst);
    if (SqlTypeUtil.isDatetime(child.getType()) || SqlTypeUtil.isInterval(child.getType())) {
      newChildRexNodeLst.add(child);
    } else {
      // We need to add a cast to DATETIME Family
      if (isTimestampLevel) {
        newChildRexNodeLst.add(makeCast(SqlTypeName.TIMESTAMP, child, rexBuilder));
      } else {
        newChildRexNodeLst.add(makeCast(SqlTypeName.DATE, child, rexBuilder));
      }
    }

    return newChildRexNodeLst;
  }

  private static RexNode makeCast(SqlTypeName typeName, final RexNode child, RexBuilder rexBuilder) {
    RelDataType sqlType = rexBuilder.getTypeFactory().createSqlType(typeName);
    RelDataType nullableType = rexBuilder.getTypeFactory().createTypeWithNullability(sqlType, true);
    return rexBuilder.makeCast(nullableType, child);
  }

  public static List<RexNode> rewriteFloorDateChildren(SqlOperator op, List<RexNode> childRexNodeLst,
      RexBuilder rexBuilder) {
    List<RexNode> newChildRexNodeLst = new ArrayList<>();
    assert childRexNodeLst.size() == 1;
    newChildRexNodeLst.add(childRexNodeLst.get(0));
    if (op == HiveFloorDate.YEAR) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.YEAR));
    } else if (op == HiveFloorDate.QUARTER) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.QUARTER));
    } else if (op == HiveFloorDate.MONTH) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.MONTH));
    } else if (op == HiveFloorDate.WEEK) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.WEEK));
    } else if (op == HiveFloorDate.DAY) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.DAY));
    } else if (op == HiveFloorDate.HOUR) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.HOUR));
    } else if (op == HiveFloorDate.MINUTE) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.MINUTE));
    } else if (op == HiveFloorDate.SECOND) {
      newChildRexNodeLst.add(rexBuilder.makeFlag(TimeUnitRange.SECOND));
    }
    return newChildRexNodeLst;
  }

  public static List<RexNode> rewriteToDateChildren(List<RexNode> childRexNodeLst, RexBuilder rexBuilder) {
    List<RexNode> newChildRexNodeLst = new ArrayList<>();
    assert childRexNodeLst.size() == 1;
    RexNode child = childRexNodeLst.get(0);
    if (SqlTypeUtil.isDatetime(child.getType()) || SqlTypeUtil.isInterval(child.getType())) {
      newChildRexNodeLst.add(child);
    } else {
      newChildRexNodeLst.add(makeCast(SqlTypeName.TIMESTAMP, child, rexBuilder));
    }
    return newChildRexNodeLst;
  }

  /**
   * The method tries to rewrite an the operands of an IN function call into
   * the operands for an OR function call.
   * For instance:
   * <pre>
   * (c) IN ( v1, v2, ...) =&gt; c=v1 || c=v2 || ...
   * Input: (c, v1, v2, ...)
   * Output: (c=v1, c=v2, ...)
   * </pre>
   * Or:
   * <pre>
   * (c,d) IN ( (v1,v2), (v3,v4), ...) =&gt; (c=v1 &amp;&amp; d=v2) || (c=v3 &amp;&amp; d=v4) || ...
   * Input: ((c,d), (v1,v2), (v3,v4), ...)
   * Output: (c=v1 &amp;&amp; d=v2, c=v3 &amp;&amp; d=v4, ...)
   * </pre>
   *
   * Returns null if the transformation fails, e.g., when non-deterministic
   * calls are found in the expressions.
   */
  public static List<RexNode> transformInToOrOperands(List<RexNode> operands, RexBuilder rexBuilder) {
    final List<RexNode> disjuncts = new ArrayList<>(operands.size() - 2);
    if (operands.get(0).getKind() != SqlKind.ROW) {
      final RexNode columnExpression = operands.get(0);
      if (!HiveCalciteUtil.isDeterministic(columnExpression)) {
        // Bail out
        return null;
      }
      for (int i = 1; i < operands.size(); i++) {
        final RexNode valueExpression = operands.get(i);
        if (!HiveCalciteUtil.isDeterministic(valueExpression)) {
          // Bail out
          return null;
        }
        disjuncts.add(rexBuilder.makeCall(
            SqlStdOperatorTable.EQUALS,
            columnExpression,
            valueExpression));
      }
    } else {
      final RexCall columnExpressions = (RexCall) operands.get(0);
      if (!HiveCalciteUtil.isDeterministic(columnExpressions)) {
        // Bail out
        return null;
      }
      for (int i = 1; i < operands.size(); i++) {
        List<RexNode> conjuncts = new ArrayList<>(columnExpressions.getOperands().size() - 1);
        RexCall valueExpressions = (RexCall) operands.get(i);
        if (!HiveCalciteUtil.isDeterministic(valueExpressions)) {
          // Bail out
          return null;
        }
        for (int j = 0; j < columnExpressions.getOperands().size(); j++) {
          conjuncts.add(rexBuilder.makeCall(
              SqlStdOperatorTable.EQUALS,
              columnExpressions.getOperands().get(j),
              valueExpressions.getOperands().get(j)));
        }
        if (conjuncts.size() > 1) {
          disjuncts.add(rexBuilder.makeCall(
              SqlStdOperatorTable.AND,
              conjuncts));
        } else {
          disjuncts.add(conjuncts.get(0));
        }
      }
    }
    return disjuncts;
  }

  public static List<RexNode> rewriteInClauseChildren(SqlOperator op, List<RexNode> childRexNodeLst,
      RexBuilder rexBuilder) throws SemanticException {
    assert op.getKind() == SqlKind.IN;
    RexNode firstPred = childRexNodeLst.get(0);
    List<RexNode> newChildRexNodeLst = new ArrayList<RexNode>();
    for (int i = 1; i < childRexNodeLst.size(); i++) {
      newChildRexNodeLst.add(
          rexBuilder.makeCall(
              SqlStdOperatorTable.EQUALS, firstPred, childRexNodeLst.get(i)));
    }
    return newChildRexNodeLst;
  }

  public static List<RexNode> rewriteCoalesceChildren(
      List<RexNode> childRexNodeLst, RexBuilder rexBuilder) {
    final List<RexNode> convertedChildList = Lists.newArrayList();
    assert childRexNodeLst.size() > 0;
    int i=0;
    for (; i < childRexNodeLst.size()-1; ++i) {
      // WHEN child not null THEN child
      final RexNode child = childRexNodeLst.get(i);
      RexNode childCond = rexBuilder.makeCall(
          SqlStdOperatorTable.IS_NOT_NULL, child);
      convertedChildList.add(childCond);
      convertedChildList.add(child);
    }
    // Add the last child as the ELSE element
    convertedChildList.add(childRexNodeLst.get(i));
    return convertedChildList;
  }

  public static List<RexNode> rewriteBetweenChildren(List<RexNode> childRexNodeLst,
      RexBuilder rexBuilder) {
    final List<RexNode> convertedChildList = Lists.newArrayList();
    SqlBinaryOperator cmpOp;
    if (childRexNodeLst.get(0).isAlwaysTrue()) {
      cmpOp = SqlStdOperatorTable.GREATER_THAN;
    } else {
      cmpOp = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
    }
    RexNode op = childRexNodeLst.get(1);
    RexNode rangeL = childRexNodeLst.get(2);
    RexNode rangeH = childRexNodeLst.get(3);
    convertedChildList.add(rexBuilder.makeCall(cmpOp, rangeL, op));
    convertedChildList.add(rexBuilder.makeCall(cmpOp, op, rangeH));
    return convertedChildList;
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

  protected RexNode convert(ExprNodeConstantDesc literal) throws CalciteSemanticException {
    final RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
    final PrimitiveTypeInfo hiveType = (PrimitiveTypeInfo) literal.getTypeInfo();
    final RelDataType calciteDataType = TypeConverter.convert(hiveType, dtFactory);

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
      calciteLiteral = rexBuilder.makeExactLiteral((BigDecimal) value, calciteDataType);
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
      final int lengthChar = TypeInfoUtils.getCharacterLengthForType(hiveType);
      RelDataType charType = rexBuilder.getTypeFactory().createTypeWithCharsetAndCollation(
          rexBuilder.getTypeFactory().createSqlType(SqlTypeName.CHAR, lengthChar),
          Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
      calciteLiteral = rexBuilder.makeLiteral(
          RexNodeExprFactory.makeHiveUnicodeString((String) value), charType, false);
      break;
    case VARCHAR:
      if (value instanceof HiveVarchar) {
        value = ((HiveVarchar) value).getValue();
      }
      final int lengthVarchar = TypeInfoUtils.getCharacterLengthForType(hiveType);
      RelDataType varcharType = rexBuilder.getTypeFactory().createTypeWithCharsetAndCollation(
          rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, lengthVarchar),
          Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
      calciteLiteral = rexBuilder.makeLiteral(
          RexNodeExprFactory.makeHiveUnicodeString((String) value), varcharType, true);
      break;
    case STRING:
      RelDataType stringType = rexBuilder.getTypeFactory().createTypeWithCharsetAndCollation(
          rexBuilder.getTypeFactory().createSqlType(SqlTypeName.VARCHAR, Integer.MAX_VALUE),
          Charset.forName(ConversionUtil.NATIVE_UTF16_CHARSET_NAME), SqlCollation.IMPLICIT);
      calciteLiteral = rexBuilder.makeLiteral(
          RexNodeExprFactory.makeHiveUnicodeString((String) value), stringType, true);
      break;
    case DATE:
      final Date date = (Date) value;
      calciteLiteral = rexBuilder.makeDateLiteral(
          DateString.fromDaysSinceEpoch(date.toEpochDay()));
      break;
    case TIMESTAMP:
      final TimestampString tsString;
      if (value instanceof Calendar) {
        tsString = TimestampString.fromCalendarFields((Calendar) value);
      } else {
        final Timestamp ts = (Timestamp) value;
        tsString = TimestampString.fromMillisSinceEpoch(ts.toEpochMilli()).withNanos(ts.getNanos());
      }
      // Must call makeLiteral, not makeTimestampLiteral
      // to have the RexBuilder.roundTime logic kick in
      calciteLiteral = rexBuilder.makeLiteral(
          tsString,
          rexBuilder.getTypeFactory().createSqlType(
              SqlTypeName.TIMESTAMP,
              rexBuilder.getTypeFactory().getTypeSystem().getDefaultPrecision(SqlTypeName.TIMESTAMP)),
          false);
      break;
    case TIMESTAMPLOCALTZ:
      final TimestampString tsLocalTZString;
      Instant i = ((TimestampTZ)value).getZonedDateTime().toInstant();
      tsLocalTZString = TimestampString
          .fromMillisSinceEpoch(i.toEpochMilli())
          .withNanos(i.getNano());
      calciteLiteral = rexBuilder.makeTimestampWithLocalTimeZoneLiteral(
          tsLocalTZString,
          rexBuilder.getTypeFactory().getTypeSystem().getDefaultPrecision(SqlTypeName.TIMESTAMP_WITH_LOCAL_TIME_ZONE));
      break;
    case INTERVAL_YEAR_MONTH:
      // Calcite year-month literal value is months as BigDecimal
      BigDecimal totalMonths = BigDecimal.valueOf(((HiveIntervalYearMonth) value).getTotalMonths());
      calciteLiteral = rexBuilder.makeIntervalLiteral(totalMonths,
          new SqlIntervalQualifier(TimeUnit.YEAR, TimeUnit.MONTH, new SqlParserPos(1, 1)));
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
      calciteLiteral = rexBuilder.makeLiteral(null, calciteDataType, true);
      break;
    case BINARY:
    case UNKNOWN:
    default:
      throw new RuntimeException("Unsupported Literal");
    }

    return calciteLiteral;
  }

}
