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

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;

import org.apache.calcite.rel.RelFieldCollation;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.rex.RexCall;
import org.apache.calcite.rex.RexFieldAccess;
import org.apache.calcite.rex.RexFieldCollation;
import org.apache.calcite.rex.RexInputRef;
import org.apache.calcite.rex.RexLiteral;
import org.apache.calcite.rex.RexNode;
import org.apache.calcite.rex.RexOver;
import org.apache.calcite.rex.RexUtil;
import org.apache.calcite.rex.RexVisitorImpl;
import org.apache.calcite.rex.RexWindow;
import org.apache.calcite.rex.RexWindowBound;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.type.SqlTypeUtil;
import org.apache.calcite.util.DateString;
import org.apache.calcite.util.TimeString;
import org.apache.calcite.util.TimestampString;
import org.apache.hadoop.hive.common.type.Date;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.hive.common.type.HiveIntervalDayTime;
import org.apache.hadoop.hive.common.type.HiveIntervalYearMonth;
import org.apache.hadoop.hive.common.type.Timestamp;
import org.apache.hadoop.hive.common.type.TimestampTZUtil;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.ConstantPropagateProcFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTConverter.RexVisitor;
import org.apache.hadoop.hive.ql.optimizer.calcite.translator.ASTConverter.Schema;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.NullOrder;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.OrderSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionExpression;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitionSpec;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PartitioningSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.BoundarySpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFrameSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowFunctionSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowSpec;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.WindowType;
import org.apache.hadoop.hive.ql.plan.ExprNodeColumnDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeConstantDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeFieldDesc;
import org.apache.hadoop.hive.ql.plan.ExprNodeGenericFuncDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableSet;

/*
 * convert a RexNode to an ExprNodeDesc
 */
public class ExprNodeConverter extends RexVisitorImpl<ExprNodeDesc> {

  private final boolean            foldExpr;
  private final String             tabAlias;
  private final RelDataType        inputRowType;
  private final ImmutableSet<Integer>       inputVCols;
  private final List<WindowFunctionSpec> windowFunctionSpecs = new ArrayList<>();
  private final RelDataTypeFactory dTFactory;
  protected final Logger LOG = LoggerFactory.getLogger(this.getClass().getName());
  private static long uniqueCounter = 0;

  public ExprNodeConverter(String tabAlias, RelDataType inputRowType,
      Set<Integer> vCols, RelDataTypeFactory dTFactory) {
    this(tabAlias, null, inputRowType, null, vCols, dTFactory, false);
  }

  public ExprNodeConverter(String tabAlias, RelDataType inputRowType,
      Set<Integer> vCols, RelDataTypeFactory dTFactory, boolean foldExpr) {
    this(tabAlias, null, inputRowType, null, vCols, dTFactory, foldExpr);
  }

  public ExprNodeConverter(String tabAlias, String columnAlias, RelDataType inputRowType,
          RelDataType outputRowType, Set<Integer> inputVCols, RelDataTypeFactory dTFactory) {
    this(tabAlias, columnAlias, inputRowType, outputRowType, inputVCols, dTFactory, false);
  }

  public ExprNodeConverter(String tabAlias, String columnAlias, RelDataType inputRowType,
          RelDataType outputRowType, Set<Integer> inputVCols, RelDataTypeFactory dTFactory,
          boolean foldExpr) {
    super(true);
    this.tabAlias = tabAlias;
    this.inputRowType = inputRowType;
    this.inputVCols = ImmutableSet.copyOf(inputVCols);
    this.dTFactory = dTFactory;
    this.foldExpr = foldExpr;
  }

  public List<WindowFunctionSpec> getWindowFunctionSpec() {
    return this.windowFunctionSpecs;
  }

  @Override
  public ExprNodeDesc visitInputRef(RexInputRef inputRef) {
    RelDataTypeField f = inputRowType.getFieldList().get(inputRef.getIndex());
    return new ExprNodeColumnDesc(TypeConverter.convert(f.getType()), f.getName(), tabAlias,
        inputVCols.contains(inputRef.getIndex()));
  }

  /**
   * TODO: Handle 1) cast 2), Windowing Agg Call
   */

  @Override
  /*
   * Handles expr like struct(key,value).key
   * Follows same rules as TypeCheckProcFactory::getXpathOrFuncExprNodeDesc()
   * which is equivalent version of parsing such an expression from AST
   */
  public ExprNodeDesc visitFieldAccess(RexFieldAccess fieldAccess) {
    ExprNodeDesc parent = fieldAccess.getReferenceExpr().accept(this);
    String child = fieldAccess.getField().getName();
    TypeInfo parentType = parent.getTypeInfo();
    // Allow accessing a field of list element structs directly from a list
    boolean isList = (parentType.getCategory() == ObjectInspector.Category.LIST);
    if (isList) {
      parentType = ((ListTypeInfo) parentType).getListElementTypeInfo();
    }
    TypeInfo t = ((StructTypeInfo) parentType).getStructFieldTypeInfo(child);
    return  new ExprNodeFieldDesc(t, parent, child, isList);
  }

  @Override
  public ExprNodeDesc visitCall(RexCall call) {
    ExprNodeDesc gfDesc = null;

    if (!deep) {
      return null;
    }

    List<ExprNodeDesc> args = new LinkedList<ExprNodeDesc>();
    if (call.getKind() == SqlKind.EXTRACT) {
      // Extract on date: special handling since function in Hive does
      // include <time_unit>. Observe that <time_unit> information
      // is implicit in the function name, thus translation will
      // proceed correctly if we just ignore the <time_unit>
      args.add(call.operands.get(1).accept(this));
    } else if (call.getKind() == SqlKind.FLOOR &&
            call.operands.size() == 2) {
      // Floor on date: special handling since function in Hive does
      // include <time_unit>. Observe that <time_unit> information
      // is implicit in the function name, thus translation will
      // proceed correctly if we just ignore the <time_unit>
      args.add(call.operands.get(0).accept(this));
    } else {
      for (RexNode operand : call.operands) {
        args.add(operand.accept(this));
      }
    }

    // If Call is a redundant cast then bail out. Ex: cast(true)BOOLEAN
    if (call.isA(SqlKind.CAST)
        && (call.operands.size() == 1)
        && SqlTypeUtil.equalSansNullability(dTFactory, call.getType(),
            call.operands.get(0).getType())) {
      return args.get(0);
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
        LOG.error("Failed to instantiate udf: ", e);
        throw new RuntimeException(e);
      }
    }

    // Try to fold if it is a constant expression
    if (foldExpr && RexUtil.isConstant(call)) {
      ExprNodeDesc constantExpr = ConstantPropagateProcFactory.foldExpr((ExprNodeGenericFuncDesc)gfDesc);
      if (constantExpr != null) {
        gfDesc = constantExpr;
      }
    }

    return gfDesc;
  }

  @Override
  public ExprNodeDesc visitLiteral(RexLiteral literal) {
    RelDataType lType = literal.getType();

    if (RexLiteral.value(literal) == null) {
      switch (literal.getType().getSqlTypeName()) {
      case BOOLEAN:
        return new ExprNodeConstantDesc(TypeInfoFactory.booleanTypeInfo, null);
      case TINYINT:
        return new ExprNodeConstantDesc(TypeInfoFactory.byteTypeInfo, null);
      case SMALLINT:
        return new ExprNodeConstantDesc(TypeInfoFactory.shortTypeInfo, null);
      case INTEGER:
        return new ExprNodeConstantDesc(TypeInfoFactory.intTypeInfo, null);
      case BIGINT:
        return new ExprNodeConstantDesc(TypeInfoFactory.longTypeInfo, null);
      case FLOAT:
      case REAL:
        return new ExprNodeConstantDesc(TypeInfoFactory.floatTypeInfo, null);
      case DOUBLE:
        return new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo, null);
      case DATE:
        return new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo, null);
      case TIME:
      case TIMESTAMP:
        return new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo, null);
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        HiveConf conf;
        try {
          conf = Hive.get().getConf();
        } catch (HiveException e) {
          throw new RuntimeException(e);
        }
        return new ExprNodeConstantDesc(
                TypeInfoFactory.getTimestampTZTypeInfo(conf.getLocalTimeZone()), null);
      case BINARY:
        return new ExprNodeConstantDesc(TypeInfoFactory.binaryTypeInfo, null);
      case DECIMAL:
        return new ExprNodeConstantDesc(
                TypeInfoFactory.getDecimalTypeInfo(lType.getPrecision(), lType.getScale()), null);
      case VARCHAR:
      case CHAR:
        return new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, null);
      case INTERVAL_YEAR:
      case INTERVAL_MONTH:
      case INTERVAL_YEAR_MONTH:
        return new ExprNodeConstantDesc(TypeInfoFactory.intervalYearMonthTypeInfo, null);
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND:
        return new ExprNodeConstantDesc(TypeInfoFactory.intervalDayTimeTypeInfo, null);
      default:
        return new ExprNodeConstantDesc(TypeInfoFactory.voidTypeInfo, null);
      }
    } else {
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
      case REAL:
        return new ExprNodeConstantDesc(TypeInfoFactory.floatTypeInfo,
            Float.valueOf(((Number) literal.getValue3()).floatValue()));
      case DOUBLE:
        return new ExprNodeConstantDesc(TypeInfoFactory.doubleTypeInfo,
            Double.valueOf(((Number) literal.getValue3()).doubleValue()));
      case DATE:
        return new ExprNodeConstantDesc(TypeInfoFactory.dateTypeInfo,
            Date.valueOf(literal.getValueAs(DateString.class).toString()));
      case TIME:
        return new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo,
            Timestamp.valueOf(literal.getValueAs(TimeString.class).toString()));
      case TIMESTAMP:
        return new ExprNodeConstantDesc(TypeInfoFactory.timestampTypeInfo,
            Timestamp.valueOf(literal.getValueAs(TimestampString.class).toString()));
      case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
        HiveConf conf;
        try {
          conf = Hive.get().getConf();
        } catch (HiveException e) {
          throw new RuntimeException(e);
        }
        // Calcite stores timestamp with local time-zone in UTC internally, thus
        // when we bring it back, we need to add the UTC suffix.
        return new ExprNodeConstantDesc(TypeInfoFactory.getTimestampTZTypeInfo(conf.getLocalTimeZone()),
            TimestampTZUtil.parse(literal.getValueAs(TimestampString.class).toString() + " UTC"));
      case BINARY:
        return new ExprNodeConstantDesc(TypeInfoFactory.binaryTypeInfo, literal.getValue3());
      case DECIMAL:
        return new ExprNodeConstantDesc(TypeInfoFactory.getDecimalTypeInfo(lType.getPrecision(),
            lType.getScale()), HiveDecimal.create((BigDecimal)literal.getValue3()));
      case VARCHAR:
      case CHAR: {
        return new ExprNodeConstantDesc(TypeInfoFactory.stringTypeInfo, literal.getValue3());
      }
      case INTERVAL_YEAR:
      case INTERVAL_MONTH:
      case INTERVAL_YEAR_MONTH: {
        BigDecimal monthsBd = (BigDecimal) literal.getValue();
        return new ExprNodeConstantDesc(TypeInfoFactory.intervalYearMonthTypeInfo,
                new HiveIntervalYearMonth(monthsBd.intValue()));
      }
      case INTERVAL_DAY:
      case INTERVAL_DAY_HOUR:
      case INTERVAL_DAY_MINUTE:
      case INTERVAL_DAY_SECOND:
      case INTERVAL_HOUR:
      case INTERVAL_HOUR_MINUTE:
      case INTERVAL_HOUR_SECOND:
      case INTERVAL_MINUTE:
      case INTERVAL_MINUTE_SECOND:
      case INTERVAL_SECOND: {
        BigDecimal millisBd = (BigDecimal) literal.getValue();
        // Calcite literal is in millis, we need to convert to seconds
        BigDecimal secsBd = millisBd.divide(BigDecimal.valueOf(1000));
        return new ExprNodeConstantDesc(TypeInfoFactory.intervalDayTimeTypeInfo,
                new HiveIntervalDayTime(secsBd));
      }
      default:
        return new ExprNodeConstantDesc(TypeInfoFactory.voidTypeInfo, literal.getValue3());
      }
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

    WindowFunctionSpec wfs = new WindowFunctionSpec();
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
    if (wUDAFAst.getText().equals("TOK_FUNCTIONSTAR")) {
      wfs.setStar(true);
    }
    String columnAlias = getWindowColumnAlias();
    wfs.setAlias(columnAlias);

    this.windowFunctionSpecs.add(wfs);

    return new ExprNodeColumnDesc(TypeConverter.convert(over.getType()), columnAlias, tabAlias,
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
        NullOrder nullOrder;
        if ( ok.right.contains(SqlKind.NULLS_FIRST) ) {
          nullOrder = NullOrder.NULLS_FIRST;
        } else if ( ok.right.contains(SqlKind.NULLS_LAST) ) {
          nullOrder = NullOrder.NULLS_LAST;
        } else {
          // Default
          nullOrder = ok.getDirection() == RelFieldCollation.Direction.ASCENDING ?
                  NullOrder.NULLS_FIRST : NullOrder.NULLS_LAST;
        }
        exprSpec.setOrder(order);
        exprSpec.setNullOrder(nullOrder);
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
    BoundarySpec start = null;
    RexWindowBound ub = window.getUpperBound();
    if (ub != null) {
      start = getWindowBound(ub);
    }

    BoundarySpec end = null;
    RexWindowBound lb = window.getLowerBound();
    if (lb != null) {
      end = getWindowBound(lb);
    }

    return new WindowFrameSpec(window.isRows() ? WindowType.ROWS : WindowType.RANGE, start, end);
  }

  private BoundarySpec getWindowBound(RexWindowBound wb) {
    BoundarySpec boundarySpec;

    if (wb.isCurrentRow()) {
      boundarySpec = new BoundarySpec(Direction.CURRENT);
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

      boundarySpec = new BoundarySpec(direction, amt);
    }

    return boundarySpec;
  }

  private String getWindowColumnAlias() {
    return "$win$_col_" + (uniqueCounter++);
  }

}
