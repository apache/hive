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

package org.apache.hadoop.hive.ql.plan.ptf;

import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.NullOrder;
import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec.Order;

public class OrderExpressionDef extends PTFExpressionDef {
  private Order order;
  private NullOrder nullOrder;

  public OrderExpressionDef() {}
  public OrderExpressionDef(PTFExpressionDef e) {
    super(e);
    order = Order.ASC;
    nullOrder = NullOrder.NULLS_FIRST;
  }

  public Order getOrder() {
    return order;
  }

  public void setOrder(Order order) {
    this.order = order;
  }

  public NullOrder getNullOrder() {
    return nullOrder;
  }

  public void setNullOrder(NullOrder nullOrder) {
    this.nullOrder = nullOrder;
  }

}
