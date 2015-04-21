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
import java.util.Set;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.common.type.HiveVarchar;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTConverter.RexVisitor;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTConverter.Schema;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitioningSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.CurrentRowSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.RangeBoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.ValueBoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFrameSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFunctionSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowSpec;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import com.google.common.collect.ImmutableSet;

/*
 * convert a RexNode to an ExprNodeDesc
 */
public class ExprNodeConverter extends RexVisitorImpl<ExprNodeDesc> {

  private final String             tabAlias;
  private final String             columnAlias;
  private final RelDataType        inputRowType;
  private final RelDataType        outputRowType;
  private final ImmutableSet<Integer>       inputVCols;
  private WindowFunctionSpec wfs;
  private final RelDataTypeFactory dTFactory;
  protected final Log LOG = LogFactory.getLog(this.getClass().getName());

  public ExprNodeConverter(String tabAlias, RelDataType inputRowType,
      Set<Integer> vCols, RelDataTypeFactory dTFactory) {
    this(tabAlias, null, inputRowType, null, vCols, dTFactory);
  }

  public ExprNodeConverter(String tabAlias, String columnAlias, RelDataType inputRowType,
          RelDataType outputRowType, Set<Integer> inputVCols, RelDataTypeFactory dTFactory) {
    super(true);
    this.tabAlias = tabAlias;
    this.columnAlias = columnAlias;
    this.inputRowType = inputRowType;
    this.outputRowType = outputRowType;
    this.inputVCols = ImmutableSet.copyOf(inputVCols);
    this.dTFactory = dTFactory;
  }

  public WindowFunctionSpec getWindowFunctionSpec() {
    return this.wfs;
  }

  @Override
  public ExprNodeDesc visitInputRef(RexInputRef inputRef) {
    RelDataTypeField f = inputRowType.getFieldList().get(inputRef.getIndex());
    return new ExprNodeColumnDesc(TypeConverter.convert(f.getType()), f.getName(), tabAlias,
        inputVCols.contains(inputRef.getIndex()));
  }

  /**
   * TODO: Handle 1) cast 2) Field Access 3) Windowing Over() 4, Windowing Agg Call
   */
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
      GenericUDF hiveUdf = SqlFunctionConverter.getHiveUDF(call.getOperator(), call.getType(), 2);
      ArrayList<ExprNodeDesc> tmpExprArgs = new ArrayList<ExprNodeDesc>();
      tmpExprArgs.addAll(args.subList(0, 2));
      try {
        gfDesc = ExprNodeGenericFuncDesc.newInstance(hiveUdf, tmpExprArgs);
      } catch (UDFArgumentException e) {
        LOG.error(e);
        throw new RuntimeException(e);
      }
      for (int i = 2; i < call.operands.size(); i++) {
        tmpExprArgs = new ArrayList<ExprNodeDesc>();
        tmpExprArgs.add(gfDesc);
        tmpExprArgs.add(args.get(i));
        try {
          gfDesc = ExprNodeGenericFuncDesc.newInstance(hiveUdf, tmpExprArgs);
        } catch (UDFArgumentException e) {
          LOG.error(e);
          throw new RuntimeException(e);
        }
      }
    } else {
      GenericUDF hiveUdf = SqlFunctionConverter.getHiveUDF(call.getOperator(), call.getType(),
          args.size());
      if (hiveUdf == null) {
        throw new RuntimeException("Cannot find UDF for " + call.getType() + " "
            + call.getOperator() + "[" + call.getOperator().getKind() + "]/" + args.size());
      }
      try {
        gfDesc = ExprNodeGenericFuncDesc.newInstance(hiveUdf, args);
      } catch (UDFArgumentException e) {
        LOG.error(e);
        throw new RuntimeException(e);
      }
    }
    return gfDesc;
  }

  /**
   * TODO: 1. Handle NULL
   */
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

  @Override
  public ExprNodeDesc visitOver(RexOver over) {
    if (!deep) {
      return null;
    }

    final RexWindow window = over.getWindow();

    final WindowSpec windowSpec = new WindowSpec();
    final PartitioningSpec partitioningSpec = getPSpec(window);
    windowSpec.setPartitioning(partitioningSpec);
    final WindowFrameSpec windowFrameSpec = getWindowRange(window);
    windowSpec.setWindowFrame(windowFrameSpec);

    wfs = new WindowFunctionSpec();
    wfs.setWindowSpec(windowSpec);
    final Schema schema = new Schema(tabAlias, inputRowType.getFieldList());
    final ASTNode wUDAFAst = new ASTConverter.RexVisitor(schema).visitOver(over);
    wfs.setExpression(wUDAFAst);
    ASTNode nameNode = (ASTNode) wUDAFAst.getChild(0);
    wfs.setName(nameNode.getText());
    for(int i=1; i < wUDAFAst.getChildCount()-1; i++) {
      ASTNode child = (ASTNode) wUDAFAst.getChild(i);
      wfs.addArg(child);
    }
    wfs.setAlias(columnAlias);

    RelDataTypeField f = outputRowType.getField(columnAlias, false, false);
    return new ExprNodeColumnDesc(TypeConverter.convert(f.getType()), columnAlias, tabAlias,
            false);
  }

  private PartitioningSpec getPSpec(RexWindow window) {
    PartitioningSpec partitioning = new PartitioningSpec();

    Schema schema = new Schema(tabAlias, inputRowType.getFieldList());

    if (window.partitionKeys != null && !window.partitionKeys.isEmpty()) {
      PartitionSpec pSpec = new PartitionSpec();
      for (RexNode pk : window.partitionKeys) {
        PartitionExpression exprSpec = new PartitionExpression();
        ASTNode astNode = pk.accept(new RexVisitor(schema));
        exprSpec.setExpression(astNode);
        pSpec.addExpression(exprSpec);
      }
      partitioning.setPartSpec(pSpec);
    }

    if (window.orderKeys != null && !window.orderKeys.isEmpty()) {
      OrderSpec oSpec = new OrderSpec();
      for (RexFieldCollation ok : window.orderKeys) {
        OrderExpression exprSpec = new OrderExpression();
        Order order = ok.getDirection() == RelFieldCollation.Direction.ASCENDING ?
                Order.ASC : Order.DESC;
        exprSpec.setOrder(order);
        ASTNode astNode = ok.left.accept(new RexVisitor(schema));
        exprSpec.setExpression(astNode);
        oSpec.addExpression(exprSpec);
      }
      partitioning.setOrderSpec(oSpec);
    }

    return partitioning;
  }

  private WindowFrameSpec getWindowRange(RexWindow window) {
    // NOTE: in Hive AST Rows->Range(Physical) & Range -> Values (logical)

    WindowFrameSpec windowFrame = new WindowFrameSpec();

    BoundarySpec start = null;
    RexWindowBound ub = window.getUpperBound();
    if (ub != null) {
      start = getWindowBound(ub, window.isRows());
    }

    BoundarySpec end = null;
    RexWindowBound lb = window.getLowerBound();
    if (lb != null) {
      end = getWindowBound(lb, window.isRows());
    }

    if (start != null || end != null) {
      if (start != null) {
        windowFrame.setStart(start);
      }
      if (end != null) {
        windowFrame.setEnd(end);
      }
    }

    return windowFrame;
  }

  private BoundarySpec getWindowBound(RexWindowBound wb, boolean isRows) {
    BoundarySpec boundarySpec;

    if (wb.isCurrentRow()) {
      boundarySpec = new CurrentRowSpec();
    } else {
      final Direction direction;
      final int amt;
      if (wb.isPreceding()) {
        direction = Direction.PRECEDING;
      } else {
        direction = Direction.FOLLOWING;
      }
      if (wb.isUnbounded()) {
        amt = BoundarySpec.UNBOUNDED_AMOUNT;
      } else {
        amt = RexLiteral.intValue(wb.getOffset());
      }
      if (isRows) {
        boundarySpec = new RangeBoundarySpec(direction, amt);
      } else {
        boundarySpec = new ValueBoundarySpec(direction, amt);
      }
    }

    return boundarySpec;
  }

}
