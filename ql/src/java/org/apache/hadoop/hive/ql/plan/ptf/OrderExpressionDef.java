package org.apache.hadoop.hive.ql.plan.ptf;

import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order;

public class OrderExpressionDef extends PTFExpressionDef {
  private Order order;

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

