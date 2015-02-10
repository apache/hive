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

import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;

/*
 * convert a RexNode to an ExprNodeDesc
 */
public class ExprNodeConverter extends RexVisitorImpl<ExprNodeDesc> {

  RelDataType rType;
  String      tabAlias;
  boolean     partitioningExpr;
  private final RelDataTypeFactory dTFactory;

  public ExprNodeConverter(String tabAlias, RelDataType rType, boolean partitioningExpr, RelDataTypeFactory dTFactory) {
    super(true);
    /*
     * hb: 6/25/14 for now we only support expressions that only contain
     * partition cols. there is no use case for supporting generic expressions.
     * for supporting generic exprs., we need to give the converter information
     * on whether a column is a partition column or not, whether a column is a
     * virtual column or not.
     */
    assert partitioningExpr == true;
    this.tabAlias = tabAlias;
    this.rType = rType;
    this.partitioningExpr = partitioningExpr;
    this.dTFactory = dTFactory;
  }

  @Override
  public ExprNodeDesc visitInputRef(RexInputRef inputRef) {
    RelDataTypeField f = rType.getFieldList().get(inputRef.getIndex());
    return new ExprNodeColumnDesc(TypeConverter.convert(f.getType()), f.getName(), tabAlias,
        partitioningExpr);
  }

  @Override
  public ExprNodeDesc visitCall(RexCall call) {
    ExprNodeGenericFuncDesc gfDesc = null;

    if (!deep) {
      return null;
    }

    List<ExprNodeDesc> args = new LinkedList<ExprNodeDesc>();

    for (RexNode operand : call.operands) {
      args.add(operand.accept(this));
    }

    // If Call is a redundant cast then bail out. Ex: cast(true)BOOLEAN
    if (call.isA(SqlKind.CAST)
        && (call.operands.size() == 1)
        && SqlTypeUtil.equalSansNullability(dTFactory, call.getType(),
            call.operands.get(0).getType())) {
      return args.get(0);
    } else if (ASTConverter.isFlat(call)) {
      // If Expr is flat (and[p,q,r,s] or[p,q,r,s]) then recursively build the
      // exprnode
      ArrayList<ExprNodeDesc> tmpExprArgs = new ArrayList<ExprNodeDesc>();
      tmpExprArgs.addAll(args.subList(0, 2));
      gfDesc = new ExprNodeGenericFuncDesc(TypeConverter.convert(call.getType()),
          SqlFunctionConverter.getHiveUDF(call.getOperator(), call.getType(), 2), tmpExprArgs);
      for (int i = 2; i < call.operands.size(); i++) {
        tmpExprArgs = new ArrayList<ExprNodeDesc>();
        tmpExprArgs.add(gfDesc);
        tmpExprArgs.add(args.get(i));
        gfDesc = new ExprNodeGenericFuncDesc(TypeConverter.convert(call.getType()),
            SqlFunctionConverter.getHiveUDF(call.getOperator(), call.getType(), 2), tmpExprArgs);
      }
    } else {
      GenericUDF hiveUdf = SqlFunctionConverter.getHiveUDF(
          call.getOperator(), call.getType(), args.size());
      if (hiveUdf == null) {
        throw new RuntimeException("Cannot find UDF for " + call.getType() + " " + call.getOperator()
            + "[" + call.getOperator().getKind() + "]/" + args.size());
      }
      gfDesc = new ExprNodeGenericFuncDesc(TypeConverter.convert(call.getType()), hiveUdf, args);
    }

    return gfDesc;
  }

  @Override
  public ExprNodeDesc visitLiteral(RexLiteral literal) {
    RelDataType lType = literal.getType();

    switch (literal.getType().getSqlTypeName()) {
    case BOOLEAN:
      return new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, Boolean.valueOf(RexLiteral
          .booleanValue(literal)));
    case TINYINT:
      return new ExprNodeConstantDesc(TypeInfoFactory.byteTypeInfo, Byte.valueOf(((Number) literal
          .getValue3()).byteValue()));
    case SMALLINT:
      return new ExprNodeConstantDesc(TypeInfoFactory.shortTypeInfo,
          Short.valueOf(((Number) literal.getValue3()).shortValue()));
    case INTEGER:
      return new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo,
          Integer.valueOf(((Number) literal.getValue3()).intValue()));
    case BIGINT:
      return new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, Long.valueOf(((Number) literal
          .getValue3()).longValue()));
    case FLOAT:
      return new ExprNodeConstantDesc(TypeInfoFactory.floatTypeInfo,
          Float.valueOf(((Number) literal.getValue3()).floatValue()));
    case DOUBLE:
      return new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo,
          Double.valueOf(((Number) literal.getValue3()).doubleValue()));
    case DATE:
      return new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo,
        new Date(((Calendar)literal.getValue()).getTimeInMillis()));
    case TIMESTAMP: {
      Object value = literal.getValue3();
      if (value instanceof Long) {
        value = new Timestamp((Long)value);
      }
      return new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, value);
    }
    case BINARY:
      return new ExprNodeConstantDesc(TypeInfoFactory.binaryTypeInfo, literal.getValue3());
    case DECIMAL:
      return new ExprNodeConstantDesc(TypeInfoFactory.getDecimalTypeInfo(lType.getPrecision(),
          lType.getScale()), literal.getValue3());
    case VARCHAR:
      return new ExprNodeConstantDesc(TypeInfoFactory.getVarcharTypeInfo(lType.getPrecision()),
          new HiveVarchar((String) literal.getValue3(), lType.getPrecision()));
    case CHAR:
      return new ExprNodeConstantDesc(TypeInfoFactory.getCharTypeInfo(lType.getPrecision()),
          new HiveChar((String) literal.getValue3(), lType.getPrecision()));
    case OTHER:
    default:
      return new ExprNodeConstantDesc(TypeInfoFactory.voidTypeInfo, literal.getValue3());
    }
  }

}
