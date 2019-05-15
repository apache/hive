/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * http://www.apache.org/licenses/LICENSE-2.0
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.wm;

import java.util.Objects;

import org.apache.hadoop.hive.metastore.api.WMTrigger;

/**
 * Trigger with query level scope that contains a name, trigger expression violating which defined action will be
 * executed.
 */
public class ExecutionTrigger implements Trigger {
  private String name;
  private Expression expression;
  private Action action;
  private String violationMsg;

  public ExecutionTrigger(final String name, final Expression expression, final Action action) {
    this.name = name;
    this.expression = expression;
    this.action = action;
  }

  @Override
  public String getName() {
    return name;
  }

  @Override
  public Expression getExpression() {
    return expression;
  }

  @Override
  public Action getAction() {
    return action;
  }

  @Override
  public Trigger clone() {
    return new ExecutionTrigger(name, expression.clone(), action);
  }

  @Override
  public String getViolationMsg() {
    return violationMsg;
  }

  @Override
  public void setViolationMsg(final String violationMsg) {
    this.violationMsg = violationMsg;
  }

  @Override
  public boolean apply(final long current) {
    return expression.evaluate(current);
  }

  @Override
  public String toString() {
    return "{ name: " + name + ", expression: " + expression + ", action: " + action + " }";
  }

  @Override
  public int hashCode() {
    int hash = name == null ? 31 : 31 * name.hashCode();
    hash += expression == null ? 31 * hash : 31 * hash * expression.hashCode();
    hash += action == null ? 31 * hash : 31 * hash * action.hashCode();
    return hash;
  }

  @Override
  public boolean equals(final Object other) {
    if (other == null || !(other instanceof ExecutionTrigger)) {
      return false;
    }

    if (other == this) {
      return true;
    }

    ExecutionTrigger otherQR = (ExecutionTrigger) other;
    return Objects.equals(name, otherQR.name) &&
      Objects.equals(expression, otherQR.expression) &&
      Objects.equals(action, otherQR.action);
  }

  public static ExecutionTrigger fromWMTrigger(final WMTrigger trigger) {
    final Action action = Action.fromMetastoreExpression(trigger.getActionExpression());
    ExecutionTrigger execTrigger = new ExecutionTrigger(trigger.getTriggerName(),
      ExpressionFactory.fromString(trigger.getTriggerExpression()), action);
    return execTrigger;
  }
}
