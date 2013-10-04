package org.apache.hadoop.hive.ql.plan.ptf;

import java.util.ArrayList;
import java.util.List;


public class PartitionDef {
  private List<PTFExpressionDef> expressions;

  public List<PTFExpressionDef> getExpressions() {
    return expressions;
  }

  public void setExpressions(List<PTFExpressionDef> expressions) {
    this.expressions = expressions;
  }
  public void addExpression(PTFExpressionDef e) {
    expressions = expressions == null ? new ArrayList<PTFExpressionDef>() : expressions;
    expressions.add(e);
  }
}
