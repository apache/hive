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
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlCastFunction;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.optimizer.calcite.CalciteSemanticException;
import org.apache.hadoop.hive.ql.parse.ParseUtils;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeNullDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseNumeric;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBridge;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToChar;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDate;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToDecimal;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFToVarchar;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector.PrimitiveCategory;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;

public class RexNodeConverter {
  private static final Log LOG = LogFactory.getLog(RexNodeConverter.class);

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

  public RexNodeConverter(RelOptCluster cluster, RelDataType inpDataType,
      ImmutableMap<String, Integer> nameToPosMap, int offset, boolean flattenExpr) {
    this.cluster = cluster;
    this.inputCtxs = ImmutableList.of(new InputCtx(inpDataType, nameToPosMap, null, offset));
    this.flattenExpr = flattenExpr;
  }

  public RexNodeConverter(RelOptCluster cluster, List<InputCtx> inpCtxLst, boolean flattenExpr) {
    this.cluster = cluster;
    this.inputCtxs = ImmutableList.<InputCtx> builder().addAll(inpCtxLst).build();
    this.flattenExpr = flattenExpr;
  }

  public RexNode convert(ExprNodeDesc expr) throws SemanticException {
    if (expr instanceof ExprNodeNullDesc) {
      return createNullLiteral(expr);
    } else if (expr instanceof ExprNodeGenericFuncDesc) {
      return convert((ExprNodeGenericFuncDesc) expr);
    } else if (expr instanceof ExprNodeConstantDesc) {
      return convert((ExprNodeConstantDesc) expr);
    } else if (expr instanceof ExprNodeColumnDesc) {
      return convert((ExprNodeColumnDesc) expr);
    } else if (expr instanceof ExprNodeFieldDesc) {
      return convert((ExprNodeFieldDesc) expr);
    } else {
      throw new RuntimeException("Unsupported Expression");
    }
    // TODO: handle ExprNodeColumnListDesc
  }

  private RexNode convert(final ExprNodeFieldDesc fieldDesc) throws SemanticException {
    RexNode rexNode = convert(fieldDesc.getDesc());
    if (rexNode instanceof RexCall) {
      // regular case of accessing nested field in a column
      return cluster.getRexBuilder().makeFieldAccess(rexNode, fieldDesc.getFieldName(), true);
    } else {
      // This may happen for schema-less tables, where columns are dynamically
      // supplied by serdes.
      throw new CalciteSemanticException("Unexpected rexnode : "
          + rexNode.getClass().getCanonicalName());
    }
  }

  private RexNode convert(final ExprNodeGenericFuncDesc func) throws SemanticException {
    ExprNodeDesc tmpExprNode;
    RexNode tmpRN;

    List<RexNode> childRexNodeLst = new LinkedList<RexNode>();
    Builder<RelDataType> argTypeBldr = ImmutableList.<RelDataType> builder();

    // TODO: 1) Expand to other functions as needed 2) What about types other than primitive.
    TypeInfo tgtDT = null;
    GenericUDF tgtUdf = func.getGenericUDF();
    boolean isNumeric = tgtUdf instanceof GenericUDFBaseNumeric,
        isCompare = !isNumeric && tgtUdf instanceof GenericUDFBaseCompare;
    if (isNumeric) {
      tgtDT = func.getTypeInfo();

      assert func.getChildren().size() == 2;
      // TODO: checking 2 children is useless, compare already does that.
    } else if (isCompare && (func.getChildren().size() == 2)) {
      tgtDT = FunctionRegistry.getCommonClassForComparison(func.getChildren().get(0)
            .getTypeInfo(), func.getChildren().get(1).getTypeInfo());
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
          GenericUDFBaseNumeric numericUdf = (GenericUDFBaseNumeric)tgtUdf;
          PrimitiveTypeInfo minArgType = numericUdf.deriveMinArgumentCast(childExpr, tgtDT);
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
          || (udf instanceof GenericUDFToBinary) || castExprUsingUDFBridge(udf)) {
        castExpr = cluster.getRexBuilder().makeAbstractCast(
            TypeConverter.convert(func.getTypeInfo(), cluster.getTypeFactory()),
            childRexNodeLst.get(0));
      }
    }

    return castExpr;
  }

  private InputCtx getInputCtx(ExprNodeColumnDesc col) throws SemanticException {
    InputCtx ctxLookingFor = null;

    if (inputCtxs.size() == 1) {
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
        throw new RuntimeException("Ambigous column mapping");
    }

    return ctxLookingFor;
  }

  protected RexNode convert(ExprNodeColumnDesc col) throws SemanticException {
    InputCtx ic = getInputCtx(col);
    int pos = ic.hiveNameToPosMap.get(col.getColumn());
    return cluster.getRexBuilder().makeInputRef(
        ic.calciteInpDataType.getFieldList().get(pos).getType(), pos + ic.offsetInCalciteSchema);
  }

  private static final BigInteger MIN_LONG_BI = BigInteger.valueOf(Long.MIN_VALUE),
      MAX_LONG_BI = BigInteger.valueOf(Long.MAX_VALUE);

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
            + " is not a valid decimal");
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
            bd.scale(), unscaled.toString().length());
        calciteLiteral = rexBuilder.makeExactLiteral(bd, relType);
      }
      break;
    case FLOAT:
      calciteLiteral = rexBuilder.makeApproxLiteral(new BigDecimal((Float) value), calciteDataType);
      break;
    case DOUBLE:
      calciteLiteral = rexBuilder.makeApproxLiteral(new BigDecimal((Double) value), calciteDataType);
      break;
    case CHAR:
      if (value instanceof HiveChar)
        value = ((HiveChar) value).getValue();
      calciteLiteral = rexBuilder.makeLiteral((String) value);
      break;
    case VARCHAR:
      if (value instanceof HiveVarchar)
        value = ((HiveVarchar) value).getValue();
      calciteLiteral = rexBuilder.makeLiteral((String) value);
      break;
    case STRING:
      calciteLiteral = rexBuilder.makeLiteral((String) value);
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

  private RexNode createNullLiteral(ExprNodeDesc expr) throws CalciteSemanticException {
    return cluster.getRexBuilder().makeNullLiteral(
        TypeConverter.convert(expr.getTypeInfo(), cluster.getTypeFactory()).getSqlTypeName());
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
