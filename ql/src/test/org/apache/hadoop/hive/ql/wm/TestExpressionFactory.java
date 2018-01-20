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

package org.apache.hadoop.hive.ql.wm;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

import org.apache.hadoop.hive.ql.wm.Expression.Predicate;
import org.junit.Test;

public class TestExpressionFactory {
  @Test
  public void testSize() {
    Expression expr = null;

    expr = ExpressionFactory.fromString("BYTES_READ > 5");
    assertNotNull(expr);
    assertEquals(Predicate.GREATER_THAN, expr.getPredicate());
    assertEquals("BYTES_READ", expr.getCounterLimit().getName());
    assertEquals(5, expr.getCounterLimit().getLimit());

    expr = ExpressionFactory.fromString("BYTES_READ > 5kb");
    assertNotNull(expr);
    assertEquals(Predicate.GREATER_THAN, expr.getPredicate());
    assertEquals("BYTES_READ", expr.getCounterLimit().getName());
    assertEquals(5 * (1 << 10), expr.getCounterLimit().getLimit());

    expr = ExpressionFactory.fromString("BYTES_READ > 2mb");
    assertNotNull(expr);
    assertEquals(Predicate.GREATER_THAN, expr.getPredicate());
    assertEquals("BYTES_READ", expr.getCounterLimit().getName());
    assertEquals(2 * (1 << 20), expr.getCounterLimit().getLimit());

    expr = ExpressionFactory.fromString("BYTES_READ > 3gb");
    assertNotNull(expr);
    assertEquals(Predicate.GREATER_THAN, expr.getPredicate());
    assertEquals("BYTES_READ", expr.getCounterLimit().getName());
    assertEquals(3L * (1 << 30), expr.getCounterLimit().getLimit());

    expr = ExpressionFactory.fromString("SHUFFLE_BYTES > 7tb");
    assertNotNull(expr);
    assertEquals(Predicate.GREATER_THAN, expr.getPredicate());
    assertEquals("SHUFFLE_BYTES", expr.getCounterLimit().getName());
    assertEquals(7L * (1L << 40), expr.getCounterLimit().getLimit());

    expr = ExpressionFactory.fromString("SHUFFLE_BYTES > 6pb");
    assertNotNull(expr);
    assertEquals(Predicate.GREATER_THAN, expr.getPredicate());
    assertEquals("SHUFFLE_BYTES", expr.getCounterLimit().getName());
    assertEquals(6L * (1L << 50), expr.getCounterLimit().getLimit());

    expr = ExpressionFactory.fromString("BYTES_WRITTEN > 27");
    assertNotNull(expr);
    assertEquals(Predicate.GREATER_THAN, expr.getPredicate());
    assertEquals("BYTES_WRITTEN", expr.getCounterLimit().getName());
    assertEquals(27, expr.getCounterLimit().getLimit());
  }

  @Test
  public void testTime() {
    Expression expr = null;

    expr = ExpressionFactory.fromString("ELAPSED_TIME > 1");
    assertNotNull(expr);
    assertEquals(Predicate.GREATER_THAN, expr.getPredicate());
    assertEquals("ELAPSED_TIME", expr.getCounterLimit().getName());
    assertEquals(1, expr.getCounterLimit().getLimit());

    expr = ExpressionFactory.fromString("ELAPSED_TIME > 1ms");
    assertNotNull(expr);
    assertEquals(Predicate.GREATER_THAN, expr.getPredicate());
    assertEquals("ELAPSED_TIME", expr.getCounterLimit().getName());
    assertEquals(1, expr.getCounterLimit().getLimit());

    expr = ExpressionFactory.fromString("ELAPSED_TIME > 1sec");
    assertNotNull(expr);
    assertEquals(Predicate.GREATER_THAN, expr.getPredicate());
    assertEquals("ELAPSED_TIME", expr.getCounterLimit().getName());
    assertEquals(1000, expr.getCounterLimit().getLimit());

    expr = ExpressionFactory.fromString("ELAPSED_TIME > 1min");
    assertNotNull(expr);
    assertEquals(Predicate.GREATER_THAN, expr.getPredicate());
    assertEquals("ELAPSED_TIME", expr.getCounterLimit().getName());
    assertEquals(60 * 1000, expr.getCounterLimit().getLimit());

    expr = ExpressionFactory.fromString("ELAPSED_TIME > 1hour");
    assertNotNull(expr);
    assertEquals(Predicate.GREATER_THAN, expr.getPredicate());
    assertEquals("ELAPSED_TIME", expr.getCounterLimit().getName());
    assertEquals(3600 * 1000, expr.getCounterLimit().getLimit());

    expr = ExpressionFactory.fromString("ELAPSED_TIME > 1day");
    assertNotNull(expr);
    assertEquals(Predicate.GREATER_THAN, expr.getPredicate());
    assertEquals("ELAPSED_TIME", expr.getCounterLimit().getName());
    assertEquals(24 * 3600 * 1000, expr.getCounterLimit().getLimit());
  }
}
