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

package org.apache.hadoop.hive.ql.plan;

import java.util.ArrayList;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.PTFQueryInputType;
import org.apache.hadoop.hive.ql.parse.PTFTranslator.LeadLagInfo;
import org.apache.hadoop.hive.ql.parse.RowResolver;
import org.apache.hadoop.hive.ql.parse.TypeCheckCtx;
import org.apache.hadoop.hive.ql.parse.WindowingSpec.Direction;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator;
import org.apache.hadoop.hive.serde2.SerDe;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;

@Explain(displayName = "PTF Operator")
public class PTFDesc extends AbstractOperatorDesc
{
  private static final long serialVersionUID = 1L;
  @SuppressWarnings("unused")
  private static final Log LOG = LogFactory.getLog(PTFDesc.class.getName());

  PartitionedTableFunctionDef funcDef;
  LeadLagInfo llInfo;
  /*
   * is this PTFDesc for a Map-Side PTF Operation?
   */
  boolean isMapSide = false;

  HiveConf cfg;

  static{
    PTFUtils.makeTransient(PTFDesc.class, "llInfo");
    PTFUtils.makeTransient(PTFDesc.class, "cfg");
  }

  public PartitionedTableFunctionDef getFuncDef() {
    return funcDef;
  }

  public void setFuncDef(PartitionedTableFunctionDef funcDef) {
    this.funcDef = funcDef;
  }

  public PartitionedTableFunctionDef getStartOfChain() {
    return funcDef == null ? null : funcDef.getStartOfChain();
  }

  public LeadLagInfo getLlInfo() {
    return llInfo;
  }

  public void setLlInfo(LeadLagInfo llInfo) {
    this.llInfo = llInfo;
  }

  public boolean forWindowing() {
    return funcDef != null && (funcDef instanceof WindowTableFunctionDef);
  }

  public boolean isMapSide() {
    return isMapSide;
  }

  public void setMapSide(boolean isMapSide) {
    this.isMapSide = isMapSide;
  }

  public HiveConf getCfg() {
    return cfg;
  }

  public void setCfg(HiveConf cfg) {
    this.cfg = cfg;
  }

  public abstract static class PTFInputDef {
    String expressionTreeString;
    ShapeDetails outputShape;
    String alias;

    public String getExpressionTreeString() {
      return expressionTreeString;
    }

    public void setExpressionTreeString(String expressionTreeString) {
      this.expressionTreeString = expressionTreeString;
    }

    public ShapeDetails getOutputShape() {
      return outputShape;
    }

    public void setOutputShape(ShapeDetails outputShape) {
      this.outputShape = outputShape;
    }
    public String getAlias() {
      return alias;
    }
    public void setAlias(String alias) {
      this.alias = alias;
    }

    public abstract PTFInputDef getInput();
  }

  public static class PTFQueryInputDef extends PTFInputDef {
    String destination;
    PTFQueryInputType type;
    public String getDestination() {
      return destination;
    }
    public void setDestination(String destination) {
      this.destination = destination;
    }
    public PTFQueryInputType getType() {
      return type;
    }
    public void setType(PTFQueryInputType type) {
      this.type = type;
    }

    @Override
    public PTFInputDef getInput() {
      return null;
    }
  }

  public static class PartitionedTableFunctionDef extends  PTFInputDef {
    String name;
    String resolverClassName;
    ShapeDetails rawInputShape;
    boolean carryForwardNames;
    PTFInputDef input;
    ArrayList<PTFExpressionDef> args;
    PartitionDef partition;
    OrderDef order;
    TableFunctionEvaluator tFunction;
    boolean transformsRawInput;
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public ShapeDetails getRawInputShape() {
      return rawInputShape;
    }
    public void setRawInputShape(ShapeDetails rawInputShape) {
      this.rawInputShape = rawInputShape;
    }
    public boolean isCarryForwardNames() {
      return carryForwardNames;
    }
    public void setCarryForwardNames(boolean carryForwardNames) {
      this.carryForwardNames = carryForwardNames;
    }
    @Override
    public PTFInputDef getInput() {
      return input;
    }
    public void setInput(PTFInputDef input) {
      this.input = input;
    }
    public PartitionDef getPartition() {
      return partition;
    }
    public void setPartition(PartitionDef partition) {
      this.partition = partition;
    }
    public OrderDef getOrder() {
      return order;
    }
    public void setOrder(OrderDef order) {
      this.order = order;
    }
    public TableFunctionEvaluator getTFunction() {
      return tFunction;
    }
    public void setTFunction(TableFunctionEvaluator tFunction) {
      this.tFunction = tFunction;
    }
    public ArrayList<PTFExpressionDef> getArgs() {
      return args;
    }

    public void setArgs(ArrayList<PTFExpressionDef> args) {
      this.args = args;
    }

    public void addArg(PTFExpressionDef arg) {
      args = args == null ? new ArrayList<PTFExpressionDef>() : args;
      args.add(arg);
    }

    public PartitionedTableFunctionDef getStartOfChain() {
      if (input instanceof PartitionedTableFunctionDef ) {
        return ((PartitionedTableFunctionDef)input).getStartOfChain();
      }
      return this;
    }
    public boolean isTransformsRawInput() {
      return transformsRawInput;
    }
    public void setTransformsRawInput(boolean transformsRawInput) {
      this.transformsRawInput = transformsRawInput;
    }
    public String getResolverClassName() {
      return resolverClassName;
    }
    public void setResolverClassName(String resolverClassName) {
      this.resolverClassName = resolverClassName;
    }
  }

  public static class WindowTableFunctionDef extends PartitionedTableFunctionDef {
    ArrayList<WindowFunctionDef> windowFunctions;

    public ArrayList<WindowFunctionDef> getWindowFunctions() {
      return windowFunctions;
    }
    public void setWindowFunctions(ArrayList<WindowFunctionDef> windowFunctions) {
      this.windowFunctions = windowFunctions;
    }
  }

  public static class ShapeDetails {
    String serdeClassName;
    Map<String, String> serdeProps;
    ArrayList<String> columnNames;
    transient StructObjectInspector OI;
    transient SerDe serde;
    transient RowResolver rr;
    transient TypeCheckCtx typeCheckCtx;

    static{
      PTFUtils.makeTransient(ShapeDetails.class, "OI", "serde", "rr", "typeCheckCtx");
    }

    public String getSerdeClassName() {
      return serdeClassName;
    }

    public void setSerdeClassName(String serdeClassName) {
      this.serdeClassName = serdeClassName;
    }

    public Map<String, String> getSerdeProps() {
      return serdeProps;
    }

    public void setSerdeProps(Map<String, String> serdeProps) {
      this.serdeProps = serdeProps;
    }

    public ArrayList<String> getColumnNames() {
      return columnNames;
    }

    public void setColumnNames(ArrayList<String> columnNames) {
      this.columnNames = columnNames;
    }

    public StructObjectInspector getOI() {
      return OI;
    }

    public void setOI(StructObjectInspector oI) {
      OI = oI;
    }

    public SerDe getSerde() {
      return serde;
    }

    public void setSerde(SerDe serde) {
      this.serde = serde;
    }

    public RowResolver getRr() {
      return rr;
    }

    public void setRr(RowResolver rr) {
      this.rr = rr;
    }

    public TypeCheckCtx getTypeCheckCtx() {
      return typeCheckCtx;
    }

    public void setTypeCheckCtx(TypeCheckCtx typeCheckCtx) {
      this.typeCheckCtx = typeCheckCtx;
    }
  }

  public static class PartitionDef {
    ArrayList<PTFExpressionDef> expressions;

    public ArrayList<PTFExpressionDef> getExpressions() {
      return expressions;
    }

    public void setExpressions(ArrayList<PTFExpressionDef> expressions) {
      this.expressions = expressions;
    }
    public void addExpression(PTFExpressionDef e) {
      expressions = expressions == null ? new ArrayList<PTFExpressionDef>() : expressions;
      expressions.add(e);
    }
  }

  public static class OrderDef {
    ArrayList<OrderExpressionDef> expressions;

    public OrderDef() {}

    public OrderDef(PartitionDef pDef) {
      for(PTFExpressionDef eDef : pDef.getExpressions())
      {
        addExpression(new OrderExpressionDef(eDef));
      }
    }

    public ArrayList<OrderExpressionDef> getExpressions() {
      return expressions;
    }

    public void setExpressions(ArrayList<OrderExpressionDef> expressions) {
      this.expressions = expressions;
    }
    public void addExpression(OrderExpressionDef e) {
      expressions = expressions == null ? new ArrayList<OrderExpressionDef>() : expressions;
      expressions.add(e);
    }
  }

  public static class OrderExpressionDef extends PTFExpressionDef {
    Order order;

    public OrderExpressionDef() {}
    public OrderExpressionDef(PTFExpressionDef e) {
      super(e);
      order = Order.ASC;
    }

    public Order getOrder() {
      return order;
    }

    public void setOrder(Order order) {
      this.order = order;
    }
  }

  public static class WindowExpressionDef  extends PTFExpressionDef {
    String alias;

    public WindowExpressionDef() {}
    public WindowExpressionDef(PTFExpressionDef eDef) {
      super(eDef);
    }
    public String getAlias() {
      return alias;
    }

    public void setAlias(String alias) {
      this.alias = alias;
    }
  }

  public static class WindowFunctionDef extends WindowExpressionDef
  {
    String name;
    boolean isStar;
    boolean isDistinct;
    ArrayList<PTFExpressionDef> args;
    WindowFrameDef windowFrame;
    GenericUDAFEvaluator wFnEval;
    boolean pivotResult;

    public String getName() {
      return name;
    }

    public void setName(String name) {
      this.name = name;
    }

    public boolean isStar() {
      return isStar;
    }

    public void setStar(boolean isStar) {
      this.isStar = isStar;
    }

    public boolean isDistinct() {
      return isDistinct;
    }

    public void setDistinct(boolean isDistinct) {
      this.isDistinct = isDistinct;
    }

    public ArrayList<PTFExpressionDef> getArgs() {
      return args;
    }

    public void setArgs(ArrayList<PTFExpressionDef> args) {
      this.args = args;
    }

    public void addArg(PTFExpressionDef arg) {
      args = args == null ? new ArrayList<PTFExpressionDef>() : args;
      args.add(arg);
    }

    public WindowFrameDef getWindowFrame() {
      return windowFrame;
    }

    public void setWindowFrame(WindowFrameDef windowFrame) {
      this.windowFrame = windowFrame;
    }

    public GenericUDAFEvaluator getWFnEval() {
      return wFnEval;
    }

    public void setWFnEval(GenericUDAFEvaluator wFnEval) {
      this.wFnEval = wFnEval;
    }

    public boolean isPivotResult() {
      return pivotResult;
    }

    public void setPivotResult(boolean pivotResult) {
      this.pivotResult = pivotResult;
    }

  }

  public static class WindowFrameDef
  {
    BoundaryDef start;
    BoundaryDef end;
    public BoundaryDef getStart() {
      return start;
    }
    public void setStart(BoundaryDef start) {
      this.start = start;
    }
    public BoundaryDef getEnd() {
      return end;
    }
    public void setEnd(BoundaryDef end) {
      this.end = end;
    }
  }

  public static abstract class BoundaryDef {
    Direction direction;

    public Direction getDirection() {
      return direction;
    }

    public void setDirection(Direction direction) {
      this.direction = direction;
    }

    public abstract int getAmt();
  }

  public static class RangeBoundaryDef extends BoundaryDef {
    int amt;

    public int compareTo(BoundaryDef other)
    {
      int c = getDirection().compareTo(other.getDirection());
      if ( c != 0) {
        return c;
      }
      RangeBoundaryDef rb = (RangeBoundaryDef) other;
      return getAmt() - rb.getAmt();
    }

    @Override
    public int getAmt() {
      return amt;
    }

    public void setAmt(int amt) {
      this.amt = amt;
    }
  }

  public static class CurrentRowDef extends BoundaryDef
  {
    public int compareTo(BoundaryDef other)
    {
      return getDirection().compareTo(other.getDirection());
    }
    @Override
    public Direction getDirection() {
      return Direction.CURRENT;
    }

    @Override
    public int getAmt() { return 0; }
  }

  public static class ValueBoundaryDef extends BoundaryDef
  {
    PTFExpressionDef expressionDef;
    int amt;

    public int compareTo(BoundaryDef other) {
      int c = getDirection().compareTo(other.getDirection());
      if ( c != 0) {
        return c;
      }
      ValueBoundaryDef vb = (ValueBoundaryDef) other;
      return getAmt() - vb.getAmt();
    }

    public PTFExpressionDef getExpressionDef() {
      return expressionDef;
    }

    public void setExpressionDef(PTFExpressionDef expressionDef) {
      this.expressionDef = expressionDef;
    }

    public ExprNodeDesc getExprNode() {
      return expressionDef == null ? null : expressionDef.getExprNode();
    }

    public ExprNodeEvaluator getExprEvaluator() {
      return expressionDef == null ? null : expressionDef.getExprEvaluator();
    }

    public ObjectInspector getOI() {
      return expressionDef == null ? null : expressionDef.getOI();
    }

    @Override
    public int getAmt() {
      return amt;
    }

    public void setAmt(int amt) {
      this.amt = amt;
    }
  }

  public static class PTFExpressionDef
  {
    String expressionTreeString;
    ExprNodeDesc exprNode;
    transient ExprNodeEvaluator exprEvaluator;
    transient ObjectInspector OI;

    static{
      PTFUtils.makeTransient(PTFExpressionDef.class, "exprEvaluator", "OI");
    }

    public PTFExpressionDef() {}
    public PTFExpressionDef(PTFExpressionDef e) {
      expressionTreeString = e.getExpressionTreeString();
      exprNode = e.getExprNode();
      exprEvaluator = e.getExprEvaluator();
      OI = e.getOI();
    }

    public String getExpressionTreeString() {
      return expressionTreeString;
    }

    public void setExpressionTreeString(String expressionTreeString) {
      this.expressionTreeString = expressionTreeString;
    }

    public ExprNodeDesc getExprNode() {
      return exprNode;
    }

    public void setExprNode(ExprNodeDesc exprNode) {
      this.exprNode = exprNode;
    }

    public ExprNodeEvaluator getExprEvaluator() {
      return exprEvaluator;
    }

    public void setExprEvaluator(ExprNodeEvaluator exprEvaluator) {
      this.exprEvaluator = exprEvaluator;
    }

    public ObjectInspector getOI() {
      return OI;
    }

    public void setOI(ObjectInspector oI) {
      OI = oI;
    }
  }

}
