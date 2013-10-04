package org.apache.hadoop.hive.ql.plan.ptf;

import java.util.ArrayList;
import java.util.List;

public class OrderDef {
  List<OrderExpressionDef> expressions;

  public OrderDef() {}

  public OrderDef(PartitionDef pDef) {
    for(PTFExpressionDef eDef : pDef.getExpressions()) {
      addExpression(new OrderExpressionDef(eDef));
    }
  }

  public List<OrderExpressionDef> getExpressions() {
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

