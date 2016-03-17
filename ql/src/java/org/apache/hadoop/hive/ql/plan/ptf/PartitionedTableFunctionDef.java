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

package org.apache.hadoop.hive.ql.plan.ptf;

import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.hive.ql.parse.PTFInvocationSpec;
import org.apache.hadoop.hive.ql.plan.Explain;
import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.udf.ptf.TableFunctionEvaluator;

@Explain(displayName = "Partition table definition", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
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
  
  private transient List<String> referencedColumns;

  @Explain(displayName = "name", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getName() {
    return name;
  }

  public void setName(String name) {
    this.name = name;
  }

  public ShapeDetails getRawInputShape() {
    return rawInputShape;
  }

  @Explain(displayName = "raw input shape")
  public ShapeDetails getRawInputShapeExplain() {
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

  @Explain(displayName = "partition by", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getPartitionExplain() {
    if (partition == null || partition.getExpressions() == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder();
    for (PTFExpressionDef expression : partition.getExpressions()) {
      if (builder.length() > 0) {
        builder.append(", ");
      }
      builder.append(expression.getExprNode().getExprString());
    }
    return builder.toString();
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

  @Explain(displayName = "order by", explainLevels = { Level.USER, Level.DEFAULT, Level.EXTENDED })
  public String getOrderExplain() {
    if (order == null || order.getExpressions() == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder();
    for (OrderExpressionDef expression : order.getExpressions()) {
      if (builder.length() > 0) {
        builder.append(", ");
      }
      builder.append(expression.getExprNode().getExprString());
      builder.append(" ");
      if (expression.getOrder() == PTFInvocationSpec.Order.ASC) {
        builder.append("ASC ");
      } else {
        builder.append("DESC ");
      }
      if (expression.getNullOrder() == PTFInvocationSpec.NullOrder.NULLS_FIRST) {
        builder.append("NULLS FIRST");
      } else {
        builder.append("NULLS LAST");
      }
    }
    return builder.toString();
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

  @Explain(displayName = "arguments")
  public String getArgsExplain() {
    if (args == null) {
      return null;
    }
    StringBuilder builder = new StringBuilder();
    for (PTFExpressionDef expression : args) {
      if (builder.length() > 0) {
        builder.append(", ");
      }
      builder.append(expression.getExprNode().getExprString());
    }
    return builder.toString();
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

  @Explain(displayName = "transforms raw input", displayOnlyOnTrue=true)
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

  @Explain(displayName = "referenced columns")
  public List<String> getReferencedColumns() {
    return referencedColumns;
  }

  public void setReferencedColumns(List<String> referencedColumns) {
    this.referencedColumns = referencedColumns;
  }
}