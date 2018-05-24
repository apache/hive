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

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.Validator;
import org.apache.hadoop.hive.ql.parse.ASTNode;
import org.apache.hadoop.hive.ql.parse.HiveParser;
import org.apache.hadoop.hive.ql.parse.ParseDriver;
import org.apache.hadoop.hive.ql.parse.ParseException;
import org.apache.hadoop.hive.ql.plan.PlanUtils;

/**
 * Factory to create expressions
 */
public class ExpressionFactory {

  public static Expression fromString(final String expression) {
    if (expression == null || expression.isEmpty()) {
      return null;
    }

    ParseDriver driver = new ParseDriver();
    ASTNode node = null;
    try {
      node = driver.parseTriggerExpression(expression);
    } catch (ParseException e) {
      throw new IllegalArgumentException("Invalid expression: " + expression, e);
    }
    if (node.getChildCount() == 2 && node.getChild(1).getType() == HiveParser.EOF) {
      node = (ASTNode) node.getChild(0);
    }
    if (node.getType() != HiveParser.TOK_TRIGGER_EXPRESSION) {
      throw new IllegalArgumentException(
          "Expected trigger expression, got: " + node.toStringTree());
    }

    if (node.getChildCount() != 3) {
      throw new IllegalArgumentException("Only single > condition supported: " + expression);
    }

    // Only ">" predicate is supported right now, this has to be extended to support
    // expression tree when multiple conditions are required. HIVE-17622
    if (node.getChild(1).getType() != HiveParser.GREATERTHAN) {
      throw new IllegalArgumentException("Invalid predicate in expression");
    }

    final String counterName = node.getChild(0).getText();
    final String counterValueStr = PlanUtils.stripQuotes(
        node.getChild(2).getText().toLowerCase());
    if (counterName.isEmpty()) {
      throw new IllegalArgumentException("Counter name cannot be empty!");
    }

    // look for matches in file system counters
    long counterValue;
    for (FileSystemCounterLimit.FSCounter fsCounter : FileSystemCounterLimit.FSCounter.values()) {
      if (counterName.toUpperCase().endsWith(fsCounter.name())) {
        try {
          counterValue = getCounterValue(counterValueStr, new Validator.SizeValidator());
          if (counterValue < 0) {
            throw new IllegalArgumentException("Illegal value for counter limit. Expected a positive long value.");
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Invalid counter value: " + counterValueStr);
        }
        // this is file system counter, valid and create counter
        FileSystemCounterLimit fsCounterLimit = FileSystemCounterLimit.fromName(counterName, counterValue);
        return createExpression(fsCounterLimit);
      }
    }

    // look for matches in time based counters
    for (TimeCounterLimit.TimeCounter timeCounter : TimeCounterLimit.TimeCounter.values()) {
      if (counterName.equalsIgnoreCase(timeCounter.name())) {
        try {
          counterValue = getCounterValue(counterValueStr, new Validator.TimeValidator(TimeUnit.MILLISECONDS));
          if (counterValue < 0) {
            throw new IllegalArgumentException("Illegal value for counter limit. Expected a positive long value.");
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Invalid counter value: " + counterValueStr);
        }
        TimeCounterLimit timeCounterLimit = new TimeCounterLimit(
          TimeCounterLimit.TimeCounter.valueOf(counterName.toUpperCase()), counterValue);
        return createExpression(timeCounterLimit);
      }
    }

    // look for matches in vertex specific counters
    for (VertexCounterLimit.VertexCounter vertexCounter : VertexCounterLimit.VertexCounter.values()) {
      if (counterName.equalsIgnoreCase(vertexCounter.name())) {
        try {
          counterValue = getCounterValue(counterValueStr, null);
          if (counterValue < 0) {
            throw new IllegalArgumentException("Illegal value for counter limit. Expected a positive long value.");
          }
        } catch (NumberFormatException e) {
          throw new IllegalArgumentException("Invalid counter value: " + counterValueStr);
        }
        VertexCounterLimit vertexCounterLimit = new VertexCounterLimit(
          VertexCounterLimit.VertexCounter.valueOf(counterName.toUpperCase()), counterValue);
        return createExpression(vertexCounterLimit);
      }
    }

    // if nothing matches, try creating a custom counter
    try {
      counterValue = getCounterValue(counterValueStr, null);
      if (counterValue < 0) {
        throw new IllegalArgumentException("Illegal value for counter limit. Expected a positive long value.");
      }
    } catch (NumberFormatException e) {
      throw new IllegalArgumentException("Invalid counter value: " + counterValueStr);
    }
    CustomCounterLimit customCounterLimit = new CustomCounterLimit(counterName, counterValue);
    return createExpression(customCounterLimit);
  }

  private static long getCounterValue(final String counterValueStr, final Validator validator) throws
    NumberFormatException {
    long counter;
    try {
      counter = Long.parseLong(counterValueStr);
    } catch (NumberFormatException e) {
      if (validator != null) {
        if (validator instanceof Validator.SizeValidator) {
          return HiveConf.toSizeBytes(counterValueStr);
        } else if (validator instanceof Validator.TimeValidator) {
          return HiveConf.toTime(counterValueStr, TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS);
        }
      }
      throw e;
    }
    return counter;
  }

  static Expression createExpression(CounterLimit counterLimit) {
    return new TriggerExpression(counterLimit, Expression.Predicate.GREATER_THAN);
  }
}
