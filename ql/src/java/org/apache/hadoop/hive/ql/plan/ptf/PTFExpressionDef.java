package org.apache.hadoop.hive.ql.plan.ptf;

import org.apache.hadoop.hive.ql.exec.ExprNodeEvaluator;
import org.apache.hadoop.hive.ql.exec.PTFUtils;
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

public class PTFExpressionDef {
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
