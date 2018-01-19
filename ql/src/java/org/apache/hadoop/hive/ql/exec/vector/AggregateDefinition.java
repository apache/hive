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

package org.apache.hadoop.hive.ql.exec.vector;

import org.apache.hadoop.hive.ql.exec.vector.expressions.aggregates.VectorAggregateExpression;
import org.apache.hadoop.hive.ql.plan.GroupByDesc;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDAFEvaluator;

class AggregateDefinition {

  private String name;
  private VectorExpressionDescriptor.ArgumentType type;
  private GenericUDAFEvaluator.Mode udafEvaluatorMode;
  private Class<? extends VectorAggregateExpression> aggClass;

  AggregateDefinition(String name, VectorExpressionDescriptor.ArgumentType type,
      GenericUDAFEvaluator.Mode udafEvaluatorMode, Class<? extends VectorAggregateExpression> aggClass) {
    this.name = name;
    this.type = type;
    this.udafEvaluatorMode = udafEvaluatorMode;
    this.aggClass = aggClass;
  }

  String getName() {
    return name;
  }
  VectorExpressionDescriptor.ArgumentType getType() {
    return type;
  }
  GenericUDAFEvaluator.Mode getUdafEvaluatorMode() {
	return udafEvaluatorMode;
  }
  Class<? extends VectorAggregateExpression> getAggClass() {
    return aggClass;
  }
}