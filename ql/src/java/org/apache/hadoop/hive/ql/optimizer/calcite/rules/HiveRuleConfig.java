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
package org.apache.hadoop.hive.ql.optimizer.calcite.rules;

import org.apache.calcite.plan.RelRule;
import org.apache.calcite.tools.RelBuilderFactory;
import org.apache.hadoop.hive.ql.optimizer.calcite.HiveRelFactories;

import java.util.Objects;

public abstract class HiveRuleConfig implements RelRule.Config {
  private RelBuilderFactory factory = HiveRelFactories.HIVE_BUILDER;
  private String description;
  private RelRule.OperandTransform operandSupplier;

  @Override
  public RelBuilderFactory relBuilderFactory() {
    return factory;
  }

  @Override
  public RelRule.Config withRelBuilderFactory(RelBuilderFactory factory) {
    this.factory = Objects.requireNonNull(factory);
    return this;
  }

  @Override
  public String description() {
    return this.description;
  }

  @Override
  public RelRule.Config withDescription(String description) {
    if (description == null || description.isEmpty()) {
      throw new IllegalArgumentException("Description can not be null/empty");
    }
    this.description = description;
    return this;
  }

  @Override
  public RelRule.OperandTransform operandSupplier() {
    return operandSupplier;
  }

  @Override
  public RelRule.Config withOperandSupplier(RelRule.OperandTransform transform) {
    this.operandSupplier = Objects.requireNonNull(transform);
    return this;
  }

}
