package org.apache.hadoop.hive.ql.plan.ptf;


public abstract class PTFInputDef {
  private String expressionTreeString;
  private ShapeDetails outputShape;
  private String alias;

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