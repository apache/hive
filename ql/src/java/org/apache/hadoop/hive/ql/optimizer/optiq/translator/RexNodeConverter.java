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
package org.apache.hadoop.hive.ql.optimizer.optiq.translator;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.GregorianCalendar;
import java.util.LinkedHashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import net.hydromatic.avatica.ByteString;

import org.apache.hadoop.hive.common.type.Decimal128;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.optimizer.optiq.OptiqSemanticException;
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
import org.eigenbase.rel.RelNode;
import org.eigenbase.relopt.RelOptCluster;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeFactory;
import org.eigenbase.rex.RexBuilder;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexUtil;
import org.eigenbase.sql.SqlOperator;
import org.eigenbase.sql.fun.SqlCastFunction;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;

public class RexNodeConverter {

  private static class InputCtx {
    private final RelDataType                   m_optiqInpDataType;
    private final ImmutableMap<String, Integer> m_hiveNameToPosMap;
    private final RowResolver                   m_hiveRR;
    private final int                           m_offsetInOptiqSchema;

    private InputCtx(RelDataType optiqInpDataType, ImmutableMap<String, Integer> hiveNameToPosMap,
        RowResolver hiveRR, int offsetInOptiqSchema) {
      m_optiqInpDataType = optiqInpDataType;
      m_hiveNameToPosMap = hiveNameToPosMap;
      m_hiveRR = hiveRR;
      m_offsetInOptiqSchema = offsetInOptiqSchema;
    }
  };

  private final RelOptCluster           m_cluster;
  private final ImmutableList<InputCtx> m_inputCtxs;
  private final boolean                 m_flattenExpr;

  public RexNodeConverter(RelOptCluster cluster, RelDataType inpDataType,
      ImmutableMap<String, Integer> nameToPosMap, int offset, boolean flattenExpr) {
    this.m_cluster = cluster;
    m_inputCtxs = ImmutableList.of(new InputCtx(inpDataType, nameToPosMap, null, offset));
    m_flattenExpr = flattenExpr;
  }

  public RexNodeConverter(RelOptCluster cluster, List<InputCtx> inpCtxLst, boolean flattenExpr) {
    this.m_cluster = cluster;
    m_inputCtxs = ImmutableList.<InputCtx> builder().addAll(inpCtxLst).build();
    m_flattenExpr = flattenExpr;
  }

  public RexNode convert(ExprNodeDesc expr) throws SemanticException {
    if (expr instanceof ExprNodeNullDesc) {
      return m_cluster.getRexBuilder().makeNullLiteral(
          TypeConverter.convert(expr.getTypeInfo(), m_cluster.getRexBuilder().getTypeFactory())
              .getSqlTypeName());
    }
    if (expr instanceof ExprNodeGenericFuncDesc) {
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
    return m_cluster.getRexBuilder().makeFieldAccess(convert(fieldDesc.getDesc()),
      fieldDesc.getFieldName(), true);
  }

  private RexNode convert(final ExprNodeGenericFuncDesc func) throws SemanticException {
    ExprNodeDesc tmpExprNode;
    RexNode tmpRN;
    TypeInfo tgtDT = null;

    List<RexNode> childRexNodeLst = new LinkedList<RexNode>();
    Builder<RelDataType> argTypeBldr = ImmutableList.<RelDataType> builder();

    // TODO: 1) Expand to other functions as needed 2) What about types
    // other
    // than primitive
    if (func.getGenericUDF() instanceof GenericUDFBaseNumeric) {
      tgtDT = func.getTypeInfo();
    } else if (func.getGenericUDF() instanceof GenericUDFBaseCompare) {
      if (func.getChildren().size() == 2) {
        tgtDT = FunctionRegistry.getCommonClassForComparison(func.getChildren().get(0)
            .getTypeInfo(), func.getChildren().get(1).getTypeInfo());
      }
    }

    for (ExprNodeDesc childExpr : func.getChildren()) {
      tmpExprNode = childExpr;
      if (tgtDT != null
          && TypeInfoUtils.isConversionRequiredForComparison(tgtDT, childExpr.getTypeInfo())) {
        tmpExprNode = ParseUtils.createConversionCast(childExpr, (PrimitiveTypeInfo) tgtDT);
      }
      argTypeBldr.add(TypeConverter.convert(tmpExprNode.getTypeInfo(), m_cluster.getTypeFactory()));
      tmpRN = convert(tmpExprNode);
      childRexNodeLst.add(tmpRN);
    }

    // This is an explicit cast
    RexNode expr = null;
    RelDataType retType = null;
    expr = handleExplicitCast(func, childRexNodeLst);

    if (expr == null) {
      retType = (expr != null) ? expr.getType() : TypeConverter.convert(func.getTypeInfo(),
          m_cluster.getTypeFactory());
      SqlOperator optiqOp = SqlFunctionConverter.getOptiqOperator(func.getGenericUDF(),
          argTypeBldr.build(), retType);
      expr = m_cluster.getRexBuilder().makeCall(optiqOp, childRexNodeLst);
    } else {
      retType = expr.getType();
    }

    // TODO: Cast Function in Optiq have a bug where it infertype on cast throws
    // an exception
    if (m_flattenExpr && (expr instanceof RexCall)
        && !(((RexCall) expr).getOperator() instanceof SqlCastFunction)) {
      RexCall call = (RexCall) expr;
      expr = m_cluster.getRexBuilder().makeCall(retType, call.getOperator(),
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

  private RexNode handleExplicitCast(ExprNodeGenericFuncDesc func,
    List<RexNode> childRexNodeLst) throws OptiqSemanticException {
    RexNode castExpr = null;

    if (childRexNodeLst != null && childRexNodeLst.size() == 1) {
      GenericUDF udf = func.getGenericUDF();
      if ((udf instanceof GenericUDFToChar) || (udf instanceof GenericUDFToVarchar)
          || (udf instanceof GenericUDFToDecimal) || (udf instanceof GenericUDFToDate)
          || (udf instanceof GenericUDFToBinary) || castExprUsingUDFBridge(udf)) {
        // || (udf instanceof GenericUDFToUnixTimeStamp) || (udf instanceof
        // GenericUDFTimestamp) || castExprUsingUDFBridge(udf)) {
        castExpr = m_cluster.getRexBuilder().makeAbstractCast(
            TypeConverter.convert(func.getTypeInfo(), m_cluster.getTypeFactory()),
            childRexNodeLst.get(0));
      }
    }

    return castExpr;
  }

  private InputCtx getInputCtx(ExprNodeColumnDesc col) throws SemanticException {
    InputCtx ctxLookingFor = null;

    if (m_inputCtxs.size() == 1) {
      ctxLookingFor = m_inputCtxs.get(0);
    } else {
      String tableAlias = col.getTabAlias();
      String colAlias = col.getColumn();
      int noInp = 0;
      for (InputCtx ic : m_inputCtxs) {
        if (tableAlias == null || ic.m_hiveRR.hasTableAlias(tableAlias)) {
          if (ic.m_hiveRR.getPosition(colAlias) >= 0) {
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
    int pos = ic.m_hiveNameToPosMap.get(col.getColumn());
    return m_cluster.getRexBuilder().makeInputRef(
        ic.m_optiqInpDataType.getFieldList().get(pos).getType(), pos + ic.m_offsetInOptiqSchema);
  }

  protected RexNode convert(ExprNodeConstantDesc literal) {
    RexBuilder rexBuilder = m_cluster.getRexBuilder();
    RelDataTypeFactory dtFactory = rexBuilder.getTypeFactory();
    PrimitiveTypeInfo hiveType = (PrimitiveTypeInfo) literal.getTypeInfo();
    RelDataType optiqDataType = TypeConverter.convert(hiveType, dtFactory);

    PrimitiveCategory hiveTypeCategory = hiveType.getPrimitiveCategory();

    ConstantObjectInspector coi = literal.getWritableObjectInspector();
    Object value = ObjectInspectorUtils.copyToStandardJavaObject(literal
        .getWritableObjectInspector().getWritableConstantValue(), coi);

    RexNode optiqLiteral = null;
    // TODO: Verify if we need to use ConstantObjectInspector to unwrap data
    switch (hiveTypeCategory) {
    case BOOLEAN:
      optiqLiteral = rexBuilder.makeLiteral(((Boolean) value).booleanValue());
      break;
    case BYTE:
      byte[] byteArray = new byte[] { (Byte) value};
      ByteString bs = new ByteString(byteArray);
      optiqLiteral = rexBuilder.makeBinaryLiteral(bs);
      break;
    case SHORT:
      optiqLiteral = rexBuilder.makeExactLiteral(new BigDecimal((Short) value));
      break;
    case INT:
      optiqLiteral = rexBuilder.makeExactLiteral(new BigDecimal((Integer) value));
      break;
    case LONG:
      optiqLiteral = rexBuilder.makeBigintLiteral(new BigDecimal((Long) value));
      break;
    // TODO: is Decimal an exact numeric or approximate numeric?
    case DECIMAL:
      if (value instanceof HiveDecimal)
        value = ((HiveDecimal) value).bigDecimalValue();
      if (value instanceof Decimal128)
        value = ((Decimal128) value).toBigDecimal();
      optiqLiteral = rexBuilder.makeExactLiteral((BigDecimal) value);
      break;
    case FLOAT:
      optiqLiteral = rexBuilder.makeApproxLiteral(new BigDecimal((Float) value), optiqDataType);
      break;
    case DOUBLE:
      optiqLiteral = rexBuilder.makeApproxLiteral(new BigDecimal((Double) value), optiqDataType);
      break;
    case CHAR:
      if (value instanceof HiveChar)
        value = ((HiveChar) value).getValue();
      optiqLiteral = rexBuilder.makeLiteral((String) value);
      break;
    case VARCHAR:
      if (value instanceof HiveVarchar)
        value = ((HiveVarchar) value).getValue();
      optiqLiteral = rexBuilder.makeLiteral((String) value);
      break;
    case STRING:
      optiqLiteral = rexBuilder.makeLiteral((String) value);
      break;
    case DATE:
      Calendar cal = new GregorianCalendar();
      cal.setTime((Date) value);
      optiqLiteral = rexBuilder.makeDateLiteral(cal);
      break;
    case TIMESTAMP:
      optiqLiteral = rexBuilder.makeTimestampLiteral((Calendar) value,
          RelDataType.PRECISION_NOT_SPECIFIED);
      break;
    case BINARY:
    case VOID:
    case UNKNOWN:
    default:
      throw new RuntimeException("UnSupported Literal");
    }

    return optiqLiteral;
  }

  public static RexNode convert(RelOptCluster cluster, ExprNodeDesc joinCondnExprNode,
      List<RelNode> inputRels, LinkedHashMap<RelNode, RowResolver> relToHiveRR,
      Map<RelNode, ImmutableMap<String, Integer>> relToHiveColNameOptiqPosMap, boolean flattenExpr)
      throws SemanticException {
    List<InputCtx> inputCtxLst = new ArrayList<InputCtx>();

    int offSet = 0;
    for (RelNode r : inputRels) {
      inputCtxLst.add(new InputCtx(r.getRowType(), relToHiveColNameOptiqPosMap.get(r), relToHiveRR
          .get(r), offSet));
      offSet += r.getRowType().getFieldCount();
    }

    return (new RexNodeConverter(cluster, inputCtxLst, flattenExpr)).convert(joinCondnExprNode);
  }
}
