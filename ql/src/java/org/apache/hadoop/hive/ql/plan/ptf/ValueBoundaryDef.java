package org.apache.hadoop.hive.ql.plan.ptf;

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class ValueBoundaryDef extends BoundaryDef {
  private PTFExpressionDef expressionDef;
  private int amt;

  public int compareTo(BoundaryDef other) {
    int c = getDirection().compareTo(other.getDirection());
    if (c != 0) {
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