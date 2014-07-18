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

import java.util.LinkedList;
import java.util.List;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.eigenbase.reltype.RelDataType;
import org.eigenbase.reltype.RelDataTypeField;
import org.eigenbase.rex.RexCall;
import org.eigenbase.rex.RexInputRef;
import org.eigenbase.rex.RexLiteral;
import org.eigenbase.rex.RexNode;
import org.eigenbase.rex.RexVisitorImpl;

/*
 * convert a RexNode to an ExprNodeDesc
 */
public class ExprNodeConverter extends RexVisitorImpl<ExprNodeDesc> {

  RelDataType rType;
  String tabAlias;
  boolean partitioningExpr;

  public ExprNodeConverter(String tabAlias, RelDataType rType,
      boolean partitioningExpr) {
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
  }

  @Override
  public ExprNodeDesc visitInputRef(RexInputRef inputRef) {
    RelDataTypeField f = rType.getFieldList().get(inputRef.getIndex());
    return new ExprNodeColumnDesc(TypeConverter.convert(f.getType()),
        f.getName(), tabAlias, partitioningExpr);
  }

  @Override
  public ExprNodeDesc visitCall(RexCall call) {
    if (!deep) {
      return null;
    }

    List<ExprNodeDesc> args = new LinkedList<ExprNodeDesc>();

    for (RexNode operand : call.operands) {
      args.add(operand.accept(this));
    }

    return new ExprNodeGenericFuncDesc(TypeConverter.convert(call.getType()),
        SqlFunctionConverter.getHiveUDF(call.getOperator()), args);
  }

  @Override
  public ExprNodeDesc visitLiteral(RexLiteral literal) {
    RelDataType lType = literal.getType();

    switch (literal.getTypeName()) {
    case BOOLEAN:
      return new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo,
          literal.getValue3());
    case TINYINT:
      return new ExprNodeConstantDesc(TypeInfoFactory.byteTypeInfo,
          literal.getValue3());
    case SMALLINT:
      return new ExprNodeConstantDesc(TypeInfoFactory.shortTypeInfo,
          literal.getValue3());
    case INTEGER:
      return new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo,
          literal.getValue3());
    case BIGINT:
      return new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo,
          literal.getValue3());
    case FLOAT:
      return new ExprNodeConstantDesc(TypeInfoFactory.floatTypeInfo,
          literal.getValue3());
    case DOUBLE:
      return new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo,
          literal.getValue3());
    case DATE:
      return new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo,
          literal.getValue3());
    case TIMESTAMP:
      return new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo,
          literal.getValue3());
    case BINARY:
      return new ExprNodeConstantDesc(TypeInfoFactory.binaryTypeInfo,
          literal.getValue3());
    case DECIMAL:
      return new ExprNodeConstantDesc(TypeInfoFactory.getDecimalTypeInfo(
          lType.getPrecision(), lType.getScale()), literal.getValue3());
    case VARCHAR:
      return new ExprNodeConstantDesc(TypeInfoFactory.getVarcharTypeInfo(lType
          .getPrecision()), 
          new HiveVarchar((String)literal.getValue3(), lType.getPrecision()));
    case CHAR:
      return new ExprNodeConstantDesc(TypeInfoFactory.getCharTypeInfo(lType
          .getPrecision()), 
          new HiveChar((String)literal.getValue3(), lType.getPrecision()));
    case OTHER:
    default:
      return new ExprNodeConstantDesc(TypeInfoFactory.voidTypeInfo,
          literal.getValue3());
    }
  }

}
