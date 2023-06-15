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

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Lists;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rex.RexBuilder;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitor;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlBinaryOperator;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.fun.SqlStdOperatorTable;
import org.apache.calcite.util.Util;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.FunctionInfo;
import org.apache.hadoop.hive.ql.exec.FunctionRegistry;
import org.apache.hadoop.hive.ql.exec.HiveFunctionInfo;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRexExecutorImpl;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveExtractDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveFloorDate;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveToDateSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveToUnixTimestampSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.reloperators.HiveUnixTimestampSqlOperator;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ExprNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.SqlFunctionConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.RexNodeConverter;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.TypeConverter;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer;
import org.apache.hadoop.hive.ql.parse.SemanticAnalyzer.GenericUDAFInfo;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDescUtils;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator.Mode;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseBinary;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBaseCompare;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFBetween;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFIn;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPAnd;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqual;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPEqualNS;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPNotEqualNS;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDFOPOr;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDTF;
import org.apache.hadoop.hive.serde2.objectinspector.ConstantObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils.ObjectInspectorCopyOption;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveGrouping;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoUtils;
import org.apache.hadoop.io.IntWritable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Function helper for Hive.
 */
public class HiveFunctionHelper implements FunctionHelper {

  private static final Logger LOG = LoggerFactory.getLogger(HiveFunctionHelper.class);

  private final RexBuilder rexBuilder;
  private final int maxNodesForInToOrTransformation;

  public HiveFunctionHelper(RexBuilder rexBuilder) {
    this.rexBuilder = rexBuilder;
    try {
      this.maxNodesForInToOrTransformation = HiveConf.getIntVar(
          Hive.get().getConf(), HiveConf.ConfVars.HIVEOPT_TRANSFORM_IN_MAXNODES);
    } catch (HiveException e) {
      throw new IllegalStateException(e);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public FunctionInfo getFunctionInfo(String functionText)
      throws SemanticException {
    return new HiveFunctionInfo(FunctionRegistry.getFunctionInfo(functionText));
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RelDataType getReturnType(FunctionInfo fi, List<RexNode> inputs)
      throws SemanticException {
    // 1) Gather inputs
    ObjectInspector[] inputsOIs = new ObjectInspector[inputs.size()];
    for (int i = 0; i < inputsOIs.length; i++) {
      inputsOIs[i] = createObjectInspector(inputs.get(i));
    }
    // 2) Initialize and obtain return type
    ObjectInspector oi = fi.getGenericUDF() != null ?
        fi.getGenericUDF().initializeAndFoldConstants(inputsOIs) :
        fi.getGenericUDTF().initialize(inputsOIs);
    // 3) Convert to RelDataType
    return TypeConverter.convert(
        TypeInfoUtils.getTypeInfoFromObjectInspector(oi), rexBuilder.getTypeFactory());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public List<RexNode> convertInputs(FunctionInfo fi, List<RexNode> inputs,
      RelDataType returnType)
      throws SemanticException {
    // 1) Obtain UDF
    final GenericUDF genericUDF = fi.getGenericUDF();
    final TypeInfo typeInfo = TypeConverter.convert(returnType);
    TypeInfo targetType = null;

    boolean isNumeric = genericUDF instanceof GenericUDFBaseBinary
        && typeInfo.getCategory() == Category.PRIMITIVE
        && PrimitiveGrouping.NUMERIC_GROUP == PrimitiveObjectInspectorUtils.getPrimitiveGrouping(((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory());
    boolean isCompare = !isNumeric && genericUDF instanceof GenericUDFBaseCompare;
    boolean isBetween = !isNumeric && genericUDF instanceof GenericUDFBetween;
    boolean isIN = !isNumeric && genericUDF instanceof GenericUDFIn;

    if (isNumeric) {
      targetType = typeInfo;
    } else if (genericUDF instanceof GenericUDFBaseCompare) {
      targetType = FunctionRegistry.getCommonClassForComparison(
          TypeConverter.convert(inputs.get(0).getType()),
          TypeConverter.convert(inputs.get(1).getType()));
    } else if (genericUDF instanceof GenericUDFBetween) {
      assert inputs.size() == 4;
      // We skip first child as is not involved (is the revert boolean)
      // The target type needs to account for all 3 operands
      targetType = FunctionRegistry.getCommonClassForComparison(
          TypeConverter.convert(inputs.get(1).getType()),
          FunctionRegistry.getCommonClassForComparison(
              TypeConverter.convert(inputs.get(2).getType()),
              TypeConverter.convert(inputs.get(3).getType())));
    } else if (genericUDF instanceof GenericUDFIn) {
      // We're only considering the first element of the IN list for the type
      assert inputs.size() > 1;
      targetType = FunctionRegistry.getCommonClassForComparison(
          TypeConverter.convert(inputs.get(0).getType()),
          TypeConverter.convert(inputs.get(1).getType()));
    }

    if (targetType != null) {
      List<RexNode> newInputs = new ArrayList<>();
      // Convert inputs if needed
      for (int i = 0; i < inputs.size(); ++i) {
        RexNode input = inputs.get(i);
        TypeInfo inputTypeInfo = TypeConverter.convert(input.getType());
        RexNode tmpExprNode = input;
        if (TypeInfoUtils.isConversionRequiredForComparison(targetType, inputTypeInfo)) {
          if (isIN || isCompare) {
            // For IN and compare, we will convert requisite children
            tmpExprNode = convert(targetType, input);
          } else if (isBetween) {
            // For BETWEEN skip the first child (the revert boolean)
            if (i > 0) {
              tmpExprNode = convert(targetType, input);
            }
          } else if (isNumeric) {
            // For numeric, we'll do minimum necessary cast - if we cast to the type
            // of expression, bad things will happen.
            PrimitiveTypeInfo minArgType = ExprNodeDescUtils.deriveMinArgumentCast(inputTypeInfo, targetType);
            tmpExprNode = convert(minArgType, input);
          } else {
            throw new AssertionError("Unexpected " + targetType + " - not a numeric op or compare");
          }
        }

        newInputs.add(tmpExprNode);
      }
      return newInputs;
    }
    return inputs;
  }

  private RexNode convert(TypeInfo targetType, RexNode input) throws SemanticException {
    if (targetType.getCategory() == Category.PRIMITIVE) {
      return RexNodeTypeCheck.getExprNodeDefaultExprProcessor(rexBuilder)
          .createConversionCast(input, (PrimitiveTypeInfo) targetType);
    } else {
      StructTypeInfo structTypeInfo = (StructTypeInfo) targetType; // struct
      RexCall call = (RexCall) input;
      List<RexNode> exprNodes = new ArrayList<>();
      for (int j = 0; j < structTypeInfo.getAllStructFieldTypeInfos().size(); j++) {
        exprNodes.add(
            convert(
                structTypeInfo.getAllStructFieldTypeInfos().get(j), call.getOperands().get(j)));
      }
      return rexBuilder.makeCall(SqlStdOperatorTable.ROW, exprNodes);
    }
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RexNode getExpression(String functionText, FunctionInfo fi,
      List<RexNode> inputs, RelDataType returnType)
      throws SemanticException {
    // See if this is an explicit cast.
    RexNode expr = RexNodeConverter.handleExplicitCast(
        fi.getGenericUDF(), returnType, inputs, rexBuilder);

    if (expr == null) {
      // This is not a cast; process the function.
      ImmutableList.Builder<RelDataType> argsTypes = ImmutableList.builder();
      for (RexNode input : inputs) {
        argsTypes.add(input.getType());
      }
      SqlOperator calciteOp = SqlFunctionConverter.getCalciteOperator(functionText,
          fi.getGenericUDF(), argsTypes.build(), returnType);
      if (calciteOp.getKind() == SqlKind.CASE) {
        // If it is a case operator, we need to rewrite it
        inputs = RexNodeConverter.rewriteCaseChildren(functionText, inputs, rexBuilder);
        // Adjust branch types by inserting explicit casts if the actual is ambiguous
        inputs = RexNodeConverter.adjustCaseBranchTypes(inputs, returnType, rexBuilder);
        checkForStatefulFunctions(inputs);
      } else if (HiveExtractDate.ALL_FUNCTIONS.contains(calciteOp)) {
        // If it is a extract operator, we need to rewrite it
        inputs = RexNodeConverter.rewriteExtractDateChildren(calciteOp, inputs, rexBuilder);
      } else if (HiveFloorDate.ALL_FUNCTIONS.contains(calciteOp)) {
        // If it is a floor <date> operator, we need to rewrite it
        inputs = RexNodeConverter.rewriteFloorDateChildren(calciteOp, inputs, rexBuilder);
      } else if (calciteOp.getKind() == SqlKind.IN) {
        // if it is a single item in an IN clause, transform A IN (B) to A = B
        // from IN [A,B] => EQUALS [A,B]
        // if it is more than an single item in an IN clause,
        // transform from IN [A,B,C] => OR [EQUALS [A,B], EQUALS [A,C]]
        // Rewrite to OR is done only if number of operands are less than
        // the threshold configured
        boolean rewriteToOr = true;
        if(maxNodesForInToOrTransformation != 0) {
          if(inputs.size() > maxNodesForInToOrTransformation) {
            rewriteToOr = false;
          }
        }
        if(rewriteToOr) {
          // If there are non-deterministic functions, we cannot perform this rewriting
          List<RexNode> newInputs = RexNodeConverter.transformInToOrOperands(inputs, rexBuilder);
          if (newInputs != null) {
            inputs = newInputs;
            if (inputs.size() == 1) {
              inputs.add(rexBuilder.makeLiteral(false));
            }
            calciteOp = SqlStdOperatorTable.OR;
          }
        }
      } else if (calciteOp.getKind() == SqlKind.COALESCE &&
          inputs.size() > 1) {
        // Rewrite COALESCE as a CASE
        // This allows to be further reduced to OR, if possible
        calciteOp = SqlStdOperatorTable.CASE;
        inputs = RexNodeConverter.rewriteCoalesceChildren(inputs, rexBuilder);
        // Adjust branch types by inserting explicit casts if the actual is ambiguous
        inputs = RexNodeConverter.adjustCaseBranchTypes(inputs, returnType, rexBuilder);
        checkForStatefulFunctions(inputs);
      } else if (calciteOp == HiveToDateSqlOperator.INSTANCE) {
        inputs = RexNodeConverter.rewriteToDateChildren(inputs, rexBuilder);
      } else if (calciteOp.getKind() == SqlKind.BETWEEN) {
        assert inputs.get(0).isAlwaysTrue() || inputs.get(0).isAlwaysFalse();
        boolean invert = inputs.get(0).isAlwaysTrue();
        SqlBinaryOperator cmpOp;
        if (invert) {
          calciteOp = SqlStdOperatorTable.OR;
          cmpOp = SqlStdOperatorTable.GREATER_THAN;
        } else {
          calciteOp = SqlStdOperatorTable.AND;
          cmpOp = SqlStdOperatorTable.LESS_THAN_OR_EQUAL;
        }
        RexNode op = inputs.get(1);
        RexNode rangeL = inputs.get(2);
        RexNode rangeH = inputs.get(3);
        inputs = new ArrayList<>();
        inputs.add(rexBuilder.makeCall(cmpOp, rangeL, op));
        inputs.add(rexBuilder.makeCall(cmpOp, op, rangeH));
      } else if (calciteOp == HiveUnixTimestampSqlOperator.INSTANCE &&
          inputs.size() > 0) {
        // unix_timestamp(args) -> to_unix_timestamp(args)
        calciteOp = HiveToUnixTimestampSqlOperator.INSTANCE;
      }
      expr = rexBuilder.makeCall(returnType, calciteOp, inputs);
    }

    if (expr instanceof RexCall && !expr.isA(SqlKind.CAST)) {
      RexCall call = (RexCall) expr;
      expr = rexBuilder.makeCall(returnType, call.getOperator(),
          RexUtil.flatten(call.getOperands(), call.getOperator()));
    }

    return expr;
  }

  private void checkForStatefulFunctions(List<RexNode> exprs)
      throws SemanticException {
    RexVisitor<Void> visitor = new RexVisitorImpl<Void>(true) {
      @Override
      public Void visitCall(final RexCall call) {
        // TODO: We should be able to annotate functions in Calcite as stateful
        // so we do not have to map back and forth to Hive functions when we are
        // doing this check.
        GenericUDF nodeUDF = SqlFunctionConverter.getHiveUDF(
            call.getOperator(), call.getType(), call.getOperands().size());
        if (nodeUDF == null) {
          throw new AssertionError("Cannot retrieve function " + call.getOperator().getName()
              + " within StatefulFunctionsChecker");
        }
        // Stateful?
        if (FunctionRegistry.isStateful(nodeUDF)) {
          throw new Util.FoundOne(call);
        }
        return super.visitCall(call);
      }
    };

    try {
      for (RexNode expr : exprs) {
        expr.accept(visitor);
      }
    } catch (Util.FoundOne e) {
      throw new SemanticException("Stateful expressions cannot be used inside of CASE");
    }
  }


  /**
   * {@inheritDoc}
   */
  @Override
  public AggregateInfo getAggregateFunctionInfo(boolean isDistinct, boolean isAllColumns,
                                                String aggregateName, List<RexNode> aggregateParameters,
                                                List<FieldCollation> fieldCollations)
      throws SemanticException {
    Mode udafMode = SemanticAnalyzer.groupByDescModeToUDAFMode(
        GroupByDesc.Mode.COMPLETE, isDistinct);
    List<ObjectInspector> aggParameterOIs = new ArrayList<>();
    Iterator<FieldCollation> obKeyIterator = fieldCollations.iterator();
    for (RexNode aggParameter : aggregateParameters) {
      aggParameterOIs.add(createObjectInspector(aggParameter));
      if (obKeyIterator.hasNext()) {
        FieldCollation fieldCollation = obKeyIterator.next();
        aggParameterOIs.add(createObjectInspector(fieldCollation.getSortExpression()));
        aggParameterOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
                TypeInfoFactory.intTypeInfo, new IntWritable(fieldCollation.getSortDirection())));
        aggParameterOIs.add(PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
                TypeInfoFactory.intTypeInfo, new IntWritable(fieldCollation.getNullOrdering().getCode())));
      }
    }
    GenericUDAFEvaluator genericUDAFEvaluator = SemanticAnalyzer.getGenericUDAFEvaluator2(
        aggregateName, aggParameterOIs, null, isDistinct, isAllColumns);
    assert (genericUDAFEvaluator != null);
    GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo2(
        genericUDAFEvaluator, udafMode, aggParameterOIs);
    return new AggregateInfo(aggregateParameters, udaf.returnType, aggregateName, isDistinct, fieldCollations);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public AggregateInfo getWindowAggregateFunctionInfo(boolean isDistinct, boolean isAllColumns,
      String aggregateName, List<RexNode> aggregateParameters)
      throws SemanticException {
    TypeInfo returnType = null;

    if (FunctionRegistry.isRankingFunction(aggregateName)) {
      // Rank functions type is 'int'/'double'
      if (aggregateName.equalsIgnoreCase("percent_rank") || aggregateName.equalsIgnoreCase("cume_dist")) {
        returnType = TypeInfoFactory.doubleTypeInfo;
      } else {
        returnType = TypeInfoFactory.intTypeInfo;
      }
    } else {
      // Try obtaining UDAF evaluators to determine the ret type
      try {
        Mode udafMode = SemanticAnalyzer.groupByDescModeToUDAFMode(
            GroupByDesc.Mode.COMPLETE, isDistinct);
        List<ObjectInspector> aggParameterOIs = new ArrayList<>();
        for (RexNode aggParameter : aggregateParameters) {
          aggParameterOIs.add(createObjectInspector(aggParameter));
        }
        if (aggregateName.toLowerCase().equals(FunctionRegistry.LEAD_FUNC_NAME)
            || aggregateName.toLowerCase().equals(FunctionRegistry.LAG_FUNC_NAME)) {
          GenericUDAFEvaluator genericUDAFEvaluator = FunctionRegistry.getGenericWindowingEvaluator(aggregateName,
              aggParameterOIs, isDistinct, isAllColumns, true);
          GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo2(
              genericUDAFEvaluator, udafMode, aggParameterOIs);
          returnType = ((ListTypeInfo) udaf.returnType).getListElementTypeInfo();
        } else {
          GenericUDAFEvaluator genericUDAFEvaluator = SemanticAnalyzer.getGenericUDAFEvaluator2(
              aggregateName, aggParameterOIs, null, isDistinct, isAllColumns);
          assert (genericUDAFEvaluator != null);
          GenericUDAFInfo udaf = SemanticAnalyzer.getGenericUDAFInfo2(
              genericUDAFEvaluator, udafMode, aggParameterOIs);
          if (FunctionRegistry.pivotResult(aggregateName)) {
            returnType = ((ListTypeInfo) udaf.returnType).getListElementTypeInfo();
          } else {
            returnType = udaf.returnType;
          }
        }
      } catch (Exception e) {
        LOG.debug("CBO: Couldn't Obtain UDAF evaluators for " + aggregateName
            + ", trying to translate to GenericUDF");
      }
    }

    return returnType != null ?
        new AggregateInfo(aggregateParameters, returnType, aggregateName, isDistinct) : null;
  }

  public RexCall getUDTFFunction(String functionName, List<RexNode> operands)
      throws SemanticException {
    // Extract the argument types for the operands into a list
    List<RelDataType> operandTypes = Lists.transform(operands, RexNode::getType);

    FunctionInfo functionInfo = FunctionRegistry.getFunctionInfo(functionName);
    GenericUDTF genericUDTF = functionInfo.getGenericUDTF();
    Preconditions.checkNotNull(genericUDTF, "Generic UDTF not found: " + functionName);

    RelDataType udtfRetType = getReturnType(functionInfo, operands);

    SqlOperator calciteOp = SqlFunctionConverter.getCalciteOperator(functionName, genericUDTF,
        ImmutableList.copyOf(operandTypes), udtfRetType);

    return (RexCall) rexBuilder.makeCall(calciteOp, operands);
  }

  private ObjectInspector createObjectInspector(RexNode expr) {
    ObjectInspector oi = createConstantObjectInspector(expr);
    if (oi != null) {
      return oi;
    }
    return TypeInfoUtils.getStandardWritableObjectInspectorFromTypeInfo(
        TypeConverter.convert(expr.getType()));
  }

  /**
   * Returns constant object inspector or null if it could not be generated.
   */
  private ConstantObjectInspector createConstantObjectInspector(RexNode expr) {
    if (RexUtil.isLiteral(expr, true)) { // Literal or cast on literal
      final ExprNodeConstantDesc constant;
      if (expr.isA(SqlKind.LITERAL)) {
        constant = ExprNodeConverter.toExprNodeConstantDesc((RexLiteral) expr);
      } else {
        RexNode foldedExpr = foldExpression(expr);
        if (!foldedExpr.isA(SqlKind.LITERAL)) {
          // Constant could not be generated
          return null;
        }
        constant = ExprNodeConverter.toExprNodeConstantDesc((RexLiteral) foldedExpr);
      }
      PrimitiveTypeInfo typeInfo = (PrimitiveTypeInfo) constant.getTypeInfo();
      Object value = constant.getValue();
      Object writableValue = value == null ? null :
          PrimitiveObjectInspectorFactory.getPrimitiveJavaObjectInspector(typeInfo)
              .getPrimitiveWritableObject(value);
      return PrimitiveObjectInspectorFactory.getPrimitiveWritableConstantObjectInspector(
          typeInfo, writableValue);
    } else if (expr instanceof RexCall) {
      RexCall call = (RexCall) expr;
      if (call.getOperator() == SqlStdOperatorTable.ROW) { // Struct
        List<String> fieldNames = new ArrayList<>();
        List<ObjectInspector> fieldObjectInspectors = new ArrayList<>();
        List<Object> writableValues = new ArrayList<>();
        for (int i = 0; i < call.getOperands().size(); i++) {
          RexNode input = call.getOperands().get(i);
          ConstantObjectInspector objectInspector = createConstantObjectInspector(input);
          if (objectInspector == null) {
            // Constant could not be generated
            return null;
          }
          fieldNames.add(expr.getType().getFieldList().get(i).getName());
          fieldObjectInspectors.add(objectInspector);
          writableValues.add(objectInspector.getWritableConstantValue());
        }
        return ObjectInspectorFactory.getStandardConstantStructObjectInspector(
            fieldNames,
            fieldObjectInspectors,
            writableValues);
      } else if (call.getOperator() == SqlStdOperatorTable.ARRAY_VALUE_CONSTRUCTOR) { // List
        ListTypeInfo listTypeInfo = (ListTypeInfo) TypeConverter.convert(expr.getType());
        TypeInfo typeInfo = listTypeInfo.getListElementTypeInfo();
        List<Object> writableValues = new ArrayList<>();
        for (RexNode input : call.getOperands()) {
          ConstantObjectInspector objectInspector = createConstantObjectInspector(input);
          if (objectInspector == null) {
            // Constant could not be generated
            return null;
          }
          writableValues.add(objectInspector.getWritableConstantValue());
        }
        return ObjectInspectorFactory.getStandardConstantListObjectInspector(
            ObjectInspectorUtils.getStandardObjectInspector(
                TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(typeInfo),
                ObjectInspectorCopyOption.WRITABLE),
            writableValues);
      } else if (call.getOperator() == SqlStdOperatorTable.MAP_VALUE_CONSTRUCTOR) { // Map
        MapTypeInfo mapTypeInfo = (MapTypeInfo) TypeConverter.convert(expr.getType());
        Map<Object, Object> writableValues = new HashMap<>();
        Iterator<RexNode> it = call.getOperands().iterator();
        while (it.hasNext()) {
          ConstantObjectInspector keyObjectInspector = createConstantObjectInspector(it.next());
          if (keyObjectInspector == null) {
            // Constant could not be generated
            return null;
          }
          ConstantObjectInspector valueObjectInspector = createConstantObjectInspector(it.next());
          if (valueObjectInspector == null) {
            // Constant could not be generated
            return null;
          }
          writableValues.put(
              keyObjectInspector.getWritableConstantValue(),
              valueObjectInspector.getWritableConstantValue());
        }
        return ObjectInspectorFactory.getStandardConstantMapObjectInspector(
            ObjectInspectorUtils.getStandardObjectInspector(
                TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(mapTypeInfo.getMapKeyTypeInfo()),
                ObjectInspectorCopyOption.WRITABLE),
            ObjectInspectorUtils.getStandardObjectInspector(
                TypeInfoUtils.getStandardJavaObjectInspectorFromTypeInfo(mapTypeInfo.getMapValueTypeInfo()),
                ObjectInspectorCopyOption.WRITABLE),
            writableValues);
      }
    }
    // Constant could not be generated
    return null;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public RexNode foldExpression(RexNode expr) {
    HiveRexExecutorImpl executor = new HiveRexExecutorImpl();
    List<RexNode> result = new ArrayList<>();
    executor.reduce(rexBuilder, ImmutableList.of(expr), result);
    return result.get(0);
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isAndFunction(FunctionInfo fi) {
    return fi.getGenericUDF() instanceof GenericUDFOPAnd;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isOrFunction(FunctionInfo fi) {
    return fi.getGenericUDF() instanceof GenericUDFOPOr;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isInFunction(FunctionInfo fi) {
    return fi.getGenericUDF() instanceof GenericUDFIn;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isCompareFunction(FunctionInfo fi) {
    return fi.getGenericUDF() instanceof GenericUDFBaseCompare;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isEqualFunction(FunctionInfo fi) {
    return fi.getGenericUDF() instanceof GenericUDFOPEqual
        && !(fi.getGenericUDF() instanceof GenericUDFOPEqualNS);
  }

  @Override
  public boolean isNSCompareFunction(FunctionInfo fi) {
    return fi.getGenericUDF() instanceof GenericUDFOPEqualNS ||
        fi.getGenericUDF() instanceof GenericUDFOPNotEqualNS;
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isConsistentWithinQuery(FunctionInfo fi) {
    return FunctionRegistry.isConsistentWithinQuery(fi.getGenericUDF());
  }

  /**
   * {@inheritDoc}
   */
  @Override
  public boolean isStateful(FunctionInfo fi) {
    GenericUDF genericUDF = fi.getGenericUDF();
    if (genericUDF == null) {
      return false;
    }
    return FunctionRegistry.isStateful(fi.getGenericUDF());
  }

}
