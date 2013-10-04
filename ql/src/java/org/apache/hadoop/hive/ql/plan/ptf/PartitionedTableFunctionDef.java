package org.apache.hadoop.hive.ql.plan.ptf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator;

public class PartitionedTableFunctionDef extends PTFInputDef {
  private String name;
  private String resolverClassName;
  private ShapeDetails rawInputShape;
  private boolean carryForwardNames;
  private PTFInputDef input;
  private List<PTFExpressionDef> args;
  private PartitionDef partition;
  private OrderDef order;
  private TableFunctionEvaluator tFunction;
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

  public List<PTFExpressionDef> getArgs() {
    return args;
  }

  public void setArgs(List<PTFExpressionDef> args) {
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