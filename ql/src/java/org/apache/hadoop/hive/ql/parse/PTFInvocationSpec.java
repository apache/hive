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

package org.apache.hadoop.hive.ql.parse;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.exec.PTFUtils;

public class PTFInvocationSpec {

  PartitionedTableFunctionSpec function;

  public PartitionedTableFunctionSpec getFunction() {
    return function;
  }

  public void setFunction(PartitionedTableFunctionSpec function) {
    this.function = function;
  }

  public PartitionedTableFunctionSpec getStartOfChain() {
    return function == null ? null : function.getStartOfChain();
  }

  public String getQueryInputName() {
    return function == null ? null : function.getQueryInputName();
  }

  public PTFQueryInputSpec getQueryInput() {
    return function == null ? null : function.getQueryInput();
  }

  /*
   * A PTF Input represents the input to a PTF Function. An Input can be a Hive SubQuery or Table
   * or another PTF Function. An Input instance captures the ASTNode that this instance was created from.
   */
  public abstract static class PTFInputSpec {
    ASTNode astNode;

    public ASTNode getAstNode() {
      return astNode;
    }

    public void setAstNode(ASTNode astNode) {
      this.astNode = astNode;
    }

    public abstract PTFInputSpec getInput();

    public abstract String getQueryInputName();
    public abstract PTFQueryInputSpec getQueryInput();
  }

  public static enum PTFQueryInputType {
    TABLE,
    SUBQUERY,
    PTFCOMPONENT,
    WINDOWING;
  }

  /*
   * A PTF input that represents a source in the overall Query. This could be a Table or a SubQuery.
   * If a PTF chain requires execution by multiple PTF Operators;
   * then the original Invocation object is decomposed into a set of Component Invocations.
   * Every component Invocation but the first one ends in a PTFQueryInputSpec instance.
   * During the construction of the Operator plan a PTFQueryInputSpec object in the chain implies connect the PTF Operator to the
   * 'input' i.e. has been generated so far.
   */
  public static class PTFQueryInputSpec extends PTFInputSpec {
    String source;
    PTFQueryInputType type;

    public String getSource() {
      return source;
    }
    public void setSource(String source) {
      this.source = source;
    }
    public PTFQueryInputType getType() {
      return type;
    }
    public void setType(PTFQueryInputType type) {
      this.type = type;
    }

    @Override
    public PTFInputSpec getInput() {
      return null;
    }

    @Override
    public String getQueryInputName() {
      return getSource();
    }
    @Override
    public PTFQueryInputSpec getQueryInput() {
      return this;
    }
  }

  /*
   * Represents a PTF Invocation. Captures:
   * - function name and alias
   * - the Partitioning details about its input
   * - its arguments. The ASTNodes representing the arguments are captured here.
   * - a reference to its Input
   */
  public static class PartitionedTableFunctionSpec  extends PTFInputSpec {
    String name;
    String alias;
    List<ASTNode> args;
    PartitioningSpec partitioning;
    PTFInputSpec input;
    public String getName() {
      return name;
    }
    public void setName(String name) {
      this.name = name;
    }
    public String getAlias() {
      return alias;
    }
    public void setAlias(String alias) {
      this.alias = alias;
    }
    public List<ASTNode> getArgs() {
      return args;
    }
    public void setArgs(List<ASTNode> args) {
      this.args = args;
    }
    public PartitioningSpec getPartitioning() {
      return partitioning;
    }
    public void setPartitioning(PartitioningSpec partitioning) {
      this.partitioning = partitioning;
    }
    @Override
    public PTFInputSpec getInput() {
      return input;
    }
    public void setInput(PTFInputSpec input) {
      this.input = input;
    }
    public PartitionSpec getPartition() {
      return getPartitioning() == null ? null : getPartitioning().getPartSpec();
    }
    public void setPartition(PartitionSpec partSpec) {
      partitioning = partitioning == null ? new PartitioningSpec() : partitioning;
      partitioning.setPartSpec(partSpec);
    }
    public OrderSpec getOrder() {
      return getPartitioning() == null ? null : getPartitioning().getOrderSpec();
    }
    public void setOrder(OrderSpec orderSpec) {
      partitioning = partitioning == null ? new PartitioningSpec() : partitioning;
      partitioning.setOrderSpec(orderSpec);
    }
    public void addArg(ASTNode arg)
    {
      args = args == null ? new ArrayList<ASTNode>() : args;
      args.add(arg);
    }

    public PartitionedTableFunctionSpec getStartOfChain() {
      if ( input instanceof PartitionedTableFunctionSpec ) {
        return ((PartitionedTableFunctionSpec)input).getStartOfChain();
      }
      return this;
    }
    @Override
    public String getQueryInputName() {
      return input.getQueryInputName();
    }
    @Override
    public PTFQueryInputSpec getQueryInput() {
      return input.getQueryInput();
    }
  }

  /*
   * Captures how the Input to a PTF Function should be partitioned and
   * ordered. Refers to a /Partition/ and /Order/ instance.
   */
  public static class PartitioningSpec {
    PartitionSpec partSpec;
    OrderSpec orderSpec;

    public PartitionSpec getPartSpec() {
      return partSpec;
    }
    public void setPartSpec(PartitionSpec partSpec) {
      this.partSpec = partSpec;
    }
    public OrderSpec getOrderSpec() {
      return orderSpec;
    }
    public void setOrderSpec(OrderSpec orderSpec) {
      this.orderSpec = orderSpec;
    }
    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((orderSpec == null) ? 0 : orderSpec.hashCode());
      result = prime * result + ((partSpec == null) ? 0 : partSpec.hashCode());
      return result;
    }
    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      PartitioningSpec other = (PartitioningSpec) obj;
      if (orderSpec == null) {
        if (other.orderSpec != null) {
          return false;
        }
      } else if (!orderSpec.equals(other.orderSpec)) {
        return false;
      }
      if (partSpec == null) {
        if (other.partSpec != null) {
          return false;
        }
      } else if (!partSpec.equals(other.partSpec)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString() {
      return String.format("PartitioningSpec=[%s%s]",
          partSpec == null ? "" : partSpec,
          orderSpec == null ? "" : orderSpec);
    }
  }

  /*
   * Captures how an Input should be Partitioned. This is captured as a
   * list of ASTNodes that are the expressions in the Distribute/Cluster
   * by clause specifying the partitioning applied for a PTF invocation.
   */
  public static class PartitionSpec {
    ArrayList<PartitionExpression> expressions;

    public ArrayList<PartitionExpression> getExpressions()
    {
      return expressions;
    }

    public void setExpressions(ArrayList<PartitionExpression> columns)
    {
      this.expressions = columns;
    }

    public void addExpression(PartitionExpression c)
    {
      expressions = expressions == null ? new ArrayList<PartitionExpression>() : expressions;
      expressions.add(c);
    }

    @Override
    public int hashCode()
    {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((expressions == null) ? 0 : expressions.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      PartitionSpec other = (PartitionSpec) obj;
      if (expressions == null)
      {
        if (other.expressions != null) {
          return false;
        }
      }
      else if (!expressions.equals(other.expressions)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString()
    {
      return String.format("partitionColumns=%s",PTFUtils.toString(expressions));
    }
  }

  public static class PartitionExpression
  {
    ASTNode expression;

    public PartitionExpression() {}

    public PartitionExpression(PartitionExpression peSpec)
    {
      expression = peSpec.getExpression();
    }

    public ASTNode getExpression() {
      return expression;
    }

    public void setExpression(ASTNode expression) {
      this.expression = expression;
    }

    @Override
    public int hashCode() {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((expression == null) ? 0 : expression.toStringTree().hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj) {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (!getClass().isAssignableFrom(obj.getClass())) {
        return false;
      }
      PartitionExpression other = (PartitionExpression) obj;
      if (expression == null) {
        if (other.expression != null) {
          return false;
        }
      } else if (!expression.toStringTree().equals(other.expression.toStringTree())) {
        return false;
      }
      return true;
    }

    @Override
    public String toString()
    {
    return expression.toStringTree();
    }

  }

  /*
   * Captures how the Input should be Ordered. This is captured as a list
   * of ASTNodes that are the expressions in the Sort By clause in a
   * PTF invocation.
   */
  public static class OrderSpec
  {
    ArrayList<OrderExpression> expressions;

    public OrderSpec() {}

    public OrderSpec(PartitionSpec pSpec)
    {
      for(PartitionExpression peSpec : pSpec.getExpressions())
      {
        addExpression(new OrderExpression(peSpec));
      }
    }

    public ArrayList<OrderExpression> getExpressions()
    {
      return expressions;
    }

    public void setExpressions(ArrayList<OrderExpression> columns)
    {
      this.expressions = columns;
    }

    public void addExpression(OrderExpression c)
    {
      expressions = expressions == null ? new ArrayList<OrderExpression>() : expressions;
      expressions.add(c);
    }

    protected boolean isPrefixedBy(PartitionSpec pSpec) {
      if ( pSpec == null || pSpec.getExpressions() == null) {
        return true;
      }

      int pExprCnt = pSpec.getExpressions().size();
      int exprCnt = getExpressions() == null ? 0 : getExpressions().size();

      if ( exprCnt < pExprCnt ) {
        return false;
      }

      for(int i=0; i < pExprCnt; i++) {
        if ( !pSpec.getExpressions().get(i).equals(getExpressions().get(i)) ) {
          return false;
        }
      }
      return true;
    }

    protected void prefixBy(PartitionSpec pSpec) {
      if ( pSpec == null || pSpec.getExpressions() == null) {
        return;
      }
      if ( expressions == null ) {
        expressions = new ArrayList<PTFInvocationSpec.OrderExpression>();
      }
      for(int i = pSpec.getExpressions().size() - 1; i >= 0; i--) {
        expressions.add(0, new OrderExpression(pSpec.getExpressions().get(i)));
      }
    }

    @Override
    public int hashCode()
    {
      final int prime = 31;
      int result = 1;
      result = prime * result + ((expressions == null) ? 0 : expressions.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (obj == null) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      OrderSpec other = (OrderSpec) obj;
      if (expressions == null)
      {
        if (other.expressions != null) {
          return false;
        }
      }
      else if (!expressions.equals(other.expressions)) {
        return false;
      }
      return true;
    }

    @Override
    public String toString()
    {
      return String.format("orderColumns=%s",PTFUtils.toString(expressions));
    }
  }

  public static enum Order
  {
    ASC,
    DESC;
  }

  public static class OrderExpression extends PartitionExpression
  {
    Order order;

    public OrderExpression() {}

    public OrderExpression(PartitionExpression peSpec)
    {
      super(peSpec);
      order = Order.ASC;
    }

    public Order getOrder()
    {
      return order;
    }

    public void setOrder(Order order)
    {
      this.order = order;
    }

    @Override
    public int hashCode()
    {
      final int prime = 31;
      int result = super.hashCode();
      result = prime * result + ((order == null) ? 0 : order.hashCode());
      return result;
    }

    @Override
    public boolean equals(Object obj)
    {
      if (this == obj) {
        return true;
      }
      if (!super.equals(obj)) {
        return false;
      }
      if (getClass() != obj.getClass()) {
        return false;
      }
      OrderExpression other = (OrderExpression) obj;
      if (order != other.order) {
        return false;
      }
      return true;
    }

    @Override
    public String toString()
    {
      return String.format("%s %s", super.toString(), order);
    }
  }

}
