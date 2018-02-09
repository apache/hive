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

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import org.junit.Test;
import org.junit.rules.ExpectedException;

public class TestTrigger {
  @org.junit.Rule
  public ExpectedException thrown = ExpectedException.none();

  @Test
  public void testSimpleQueryTrigger() {
    Expression expression = ExpressionFactory.createExpression(new FileSystemCounterLimit("hdfs",
      FileSystemCounterLimit.FSCounter.BYTES_READ, 1024));
    Trigger trigger = new ExecutionTrigger("hdfs_read_heavy", expression, new Action(Action.Type.KILL_QUERY));
    assertEquals("counter: HDFS_BYTES_READ limit: 1024", expression.getCounterLimit().toString());
    assertFalse(trigger.apply(1000));
    assertTrue(trigger.apply(1025));

    expression = ExpressionFactory.createExpression(new FileSystemCounterLimit("hdfs",
      FileSystemCounterLimit.FSCounter.BYTES_WRITTEN, 1024));
    trigger = new ExecutionTrigger("hdfs_write_heavy", expression, new Action(Action.Type.KILL_QUERY));
    assertEquals("counter: HDFS_BYTES_WRITTEN limit: 1024", expression.getCounterLimit().toString());
    assertFalse(trigger.apply(1000));
    assertTrue(trigger.apply(1025));

    expression = ExpressionFactory.createExpression(new FileSystemCounterLimit("",
      FileSystemCounterLimit.FSCounter.BYTES_READ, 1024));
    trigger = new ExecutionTrigger("local_read_heavy", expression, new Action(Action.Type.KILL_QUERY));
    assertEquals("counter: BYTES_READ limit: 1024", expression.getCounterLimit().toString());
    assertFalse(trigger.apply(1000));
    assertTrue(trigger.apply(1025));

    expression = ExpressionFactory.createExpression(new FileSystemCounterLimit("",
      FileSystemCounterLimit.FSCounter.BYTES_WRITTEN, 1024));
    trigger = new ExecutionTrigger("local_write_heavy", expression, new Action(Action.Type.KILL_QUERY));
    assertEquals("counter: BYTES_WRITTEN limit: 1024", expression.getCounterLimit().toString());
    assertFalse(trigger.apply(1000));
    assertTrue(trigger.apply(1025));

    expression = ExpressionFactory.createExpression(new FileSystemCounterLimit("",
      FileSystemCounterLimit.FSCounter.SHUFFLE_BYTES, 1024));
    trigger = new ExecutionTrigger("shuffle_heavy", expression, new Action(Action.Type.KILL_QUERY));
    assertEquals("counter: SHUFFLE_BYTES limit: 1024", expression.getCounterLimit().toString());
    assertFalse(trigger.apply(1000));
    assertTrue(trigger.apply(1025));

    expression = ExpressionFactory.createExpression(new TimeCounterLimit(TimeCounterLimit.TimeCounter
      .EXECUTION_TIME, 10000));
    trigger = new ExecutionTrigger("slow_query", expression, new Action(Action.Type.MOVE_TO_POOL,"fake_pool"));
    assertEquals("counter: EXECUTION_TIME limit: 10000", expression.getCounterLimit().toString());
    assertFalse(trigger.apply(1000));
    assertTrue(trigger.apply(100000));

    expression = ExpressionFactory.createExpression(new VertexCounterLimit(VertexCounterLimit.VertexCounter
      .VERTEX_TOTAL_TASKS, 10000));
    trigger = new ExecutionTrigger("highly_parallel", expression, new Action(Action.Type.KILL_QUERY));
    assertEquals("counter: VERTEX_TOTAL_TASKS limit: 10000", expression.getCounterLimit().toString());
    assertFalse(trigger.apply(1000));
    assertTrue(trigger.apply(100000));

    expression = ExpressionFactory.createExpression(new VertexCounterLimit(VertexCounterLimit.VertexCounter
      .DAG_TOTAL_TASKS, 10000));
    trigger = new ExecutionTrigger("highly_parallel", expression, new Action(Action.Type.KILL_QUERY));
    assertEquals("counter: DAG_TOTAL_TASKS limit: 10000", expression.getCounterLimit().toString());
    assertFalse(trigger.apply(1000));
    assertTrue(trigger.apply(100000));

    expression = ExpressionFactory.createExpression(new CustomCounterLimit("HDFS_WRITE_OPS",10000));
    trigger = new ExecutionTrigger("write_heavy", expression, new Action(Action.Type.KILL_QUERY));
    assertEquals("counter: HDFS_WRITE_OPS limit: 10000", expression.getCounterLimit().toString());
    assertFalse(trigger.apply(1000));
    assertTrue(trigger.apply(100000));
  }

  @Test
  public void testExpressionFromString() {
    Expression expression = ExpressionFactory.fromString("BYTES_READ>1024");
    Expression expected = ExpressionFactory.createExpression(new FileSystemCounterLimit("",
      FileSystemCounterLimit.FSCounter.BYTES_READ, 1024));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());
    expression = ExpressionFactory.fromString("BYTES_READ > 1024");
    assertEquals(expected, expression);

    expression = ExpressionFactory.fromString(expected.toString());
    assertEquals(expected.toString(), expression.toString());

    assertEquals(expected.hashCode(), expression.hashCode());
    expression = ExpressionFactory.fromString("  BYTES_READ   >   1024  ");
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString("BYTES_WRITTEN > 1024");
    expected = ExpressionFactory.createExpression(new FileSystemCounterLimit("",
      FileSystemCounterLimit.FSCounter.BYTES_WRITTEN, 1024));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" HDFS_BYTES_READ > 1024 ");
    expected = ExpressionFactory.createExpression(new FileSystemCounterLimit("hdfs",
      FileSystemCounterLimit.FSCounter.BYTES_READ, 1024));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" HDFS_BYTES_WRITTEN > 1024");
    expected = ExpressionFactory.createExpression(new FileSystemCounterLimit("hdfs",
      FileSystemCounterLimit.FSCounter.BYTES_WRITTEN, 1024));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" S3A_BYTES_READ > 1024");
    expected = ExpressionFactory.createExpression(new FileSystemCounterLimit("s3a",
      FileSystemCounterLimit.FSCounter.BYTES_READ, 1024));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" S3A_BYTES_WRITTEN > 1024");
    expected = ExpressionFactory.createExpression(new FileSystemCounterLimit("s3a",
      FileSystemCounterLimit.FSCounter.BYTES_WRITTEN, 1024));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" s3a_ByTeS_WRiTTeN > 1024");
    expected = ExpressionFactory.createExpression(new FileSystemCounterLimit("s3a",
      FileSystemCounterLimit.FSCounter.BYTES_WRITTEN, 1024));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" SHUFFLE_BYTES > 1024");
    expected = ExpressionFactory.createExpression(new FileSystemCounterLimit("",
      FileSystemCounterLimit.FSCounter.SHUFFLE_BYTES, 1024));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" EXECUTION_TIME > 300");
    expected = ExpressionFactory.createExpression(new TimeCounterLimit(TimeCounterLimit.TimeCounter
      .EXECUTION_TIME, 300));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" ELAPSED_TIME > 300");
    expected = ExpressionFactory.createExpression(new TimeCounterLimit(TimeCounterLimit.TimeCounter
      .ELAPSED_TIME, 300));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" elapsed_TIME > 300");
    expected = ExpressionFactory.createExpression(new TimeCounterLimit(TimeCounterLimit.TimeCounter
      .ELAPSED_TIME, 300));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" VERTEX_TOTAL_TASKS > 10000");
    expected = ExpressionFactory.createExpression(new VertexCounterLimit(VertexCounterLimit.VertexCounter
      .VERTEX_TOTAL_TASKS, 10000));
    assertEquals("counter: VERTEX_TOTAL_TASKS limit: 10000", expression.getCounterLimit().toString());
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" DAG_TOTAL_TASKS > 10000");
    expected = ExpressionFactory.createExpression(new VertexCounterLimit(VertexCounterLimit.VertexCounter
      .DAG_TOTAL_TASKS, 10000));
    assertEquals("counter: DAG_TOTAL_TASKS limit: 10000", expression.getCounterLimit().toString());
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" HDFS_WRITE_OPS > 10000");
    expected = ExpressionFactory.createExpression(new CustomCounterLimit("HDFS_WRITE_OPS",10000));
    assertEquals("counter: HDFS_WRITE_OPS limit: 10000", expression.getCounterLimit().toString());
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());
  }

  @Test
  public void testSizeValidationInTrigger() {
    Expression expression = ExpressionFactory.fromString(" SHUFFLE_BYTES > 100MB");
    Expression expected = ExpressionFactory.createExpression(new FileSystemCounterLimit("",
      FileSystemCounterLimit.FSCounter.SHUFFLE_BYTES, 100 * 1024 * 1024));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" SHUFFLE_BYTES > 1gB");
    expected = ExpressionFactory.createExpression(new FileSystemCounterLimit("",
      FileSystemCounterLimit.FSCounter.SHUFFLE_BYTES, 1024 * 1024 * 1024));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" SHUFFLE_BYTES > 1TB");
    expected = ExpressionFactory.createExpression(new FileSystemCounterLimit("",
      FileSystemCounterLimit.FSCounter.SHUFFLE_BYTES, 1024L * 1024 * 1024 * 1024));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" SHUFFLE_BYTES > 100");
    expected = ExpressionFactory.createExpression(new FileSystemCounterLimit("",
      FileSystemCounterLimit.FSCounter.SHUFFLE_BYTES, 100));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" SHUFFLE_BYTES > 100");
    expected = ExpressionFactory.createExpression(new FileSystemCounterLimit("",
      FileSystemCounterLimit.FSCounter.SHUFFLE_BYTES, 100));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());
  }

  @Test
  public void testIllegalSizeCounterValue1() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid expression:  SHUFFLE_BYTES > 300GiB");
    ExpressionFactory.fromString(" SHUFFLE_BYTES > 300GiB");
  }

  @Test
  public void testIllegalSizeCounterValue2() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid expression:  SHUFFLE_BYTES > 300 foo");
    ExpressionFactory.fromString(" SHUFFLE_BYTES > 300 foo");
  }

  @Test
  public void testTimeValidationInTrigger() {
    Expression expression = ExpressionFactory.fromString(" elapsed_TIME > 300sec");
    Expression expected = ExpressionFactory.createExpression(new TimeCounterLimit(TimeCounterLimit.TimeCounter
      .ELAPSED_TIME, 300000));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" elapsed_TIME > 300seconds");
    expected = ExpressionFactory.createExpression(new TimeCounterLimit(TimeCounterLimit.TimeCounter
      .ELAPSED_TIME, 300000));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" elapsed_TIME > 300sec");
    expected = ExpressionFactory.createExpression(new TimeCounterLimit(TimeCounterLimit.TimeCounter
      .ELAPSED_TIME, 300000));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" elapsed_TIME > 300second");
    expected = ExpressionFactory.createExpression(new TimeCounterLimit(TimeCounterLimit.TimeCounter
      .ELAPSED_TIME, 300000));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" elapsed_TIME > 300seconds");
    expected = ExpressionFactory.createExpression(new TimeCounterLimit(TimeCounterLimit.TimeCounter
      .ELAPSED_TIME, 300000));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" elapsed_TIME > 300sec");
    expected = ExpressionFactory.createExpression(new TimeCounterLimit(TimeCounterLimit.TimeCounter
      .ELAPSED_TIME, 300000));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" elapsed_TIME > 300000ms");
    expected = ExpressionFactory.createExpression(new TimeCounterLimit(TimeCounterLimit.TimeCounter
      .ELAPSED_TIME, 300000));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());


    expression = ExpressionFactory.fromString(" elapsed_TIME > 300000000microseconds");
    expected = ExpressionFactory.createExpression(new TimeCounterLimit(TimeCounterLimit.TimeCounter
      .ELAPSED_TIME, 300000));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());

    expression = ExpressionFactory.fromString(" elapsed_TIME > 1DAY");
    expected = ExpressionFactory.createExpression(new TimeCounterLimit(TimeCounterLimit.TimeCounter
      .ELAPSED_TIME, 24 * 60 * 60 * 1000));
    assertEquals(expected, expression);
    assertEquals(expected.hashCode(), expression.hashCode());
  }

  @Test
  public void testIllegalTimeCounterValue1() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid expression:  elapsed_TIME > 300lightyears");
    ExpressionFactory.fromString(" elapsed_TIME > 300lightyears");
  }

  @Test
  public void testIllegalTimeCounterValue2() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid expression:  elapsed_TIME > 300secTOR");
    ExpressionFactory.fromString(" elapsed_TIME > 300secTOR");
  }

  @Test
  public void testActionFromMetastoreStr() {
    assertEquals(Action.Type.KILL_QUERY, Action.fromMetastoreExpression("KILL").getType());
    assertEquals(Action.Type.MOVE_TO_POOL, Action.fromMetastoreExpression("MOVE TO bi").getType());
    assertEquals("bi", Action.fromMetastoreExpression("MOVE TO bi").getPoolName());
    assertEquals("bi.c1.c2", Action.fromMetastoreExpression("MOVE TO bi.c1.c2").getPoolName());
    assertEquals("MOVE TO etl", Action.fromMetastoreExpression("MOVE TO etl").toString());

    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid action expression: MOVE TO  ");
    assertEquals(Action.Type.MOVE_TO_POOL, Action.fromMetastoreExpression("MOVE TO    ").getType());
  }

  @Test
  public void testTriggerClone() {
    Expression expression = ExpressionFactory.createExpression(new FileSystemCounterLimit("hdfs",
      FileSystemCounterLimit.FSCounter.BYTES_READ, 1024));
    Trigger trigger = new ExecutionTrigger("hdfs_read_heavy", expression, new Action(Action.Type.KILL_QUERY));
    Trigger clonedTrigger = trigger.clone();
    assertNotEquals(System.identityHashCode(trigger), System.identityHashCode(clonedTrigger));
    assertNotEquals(System.identityHashCode(trigger.getExpression()), System.identityHashCode(clonedTrigger.getExpression()));
    assertNotEquals(System.identityHashCode(trigger.getExpression().getCounterLimit()),
      System.identityHashCode(clonedTrigger.getExpression().getCounterLimit()));
    assertEquals(trigger, clonedTrigger);
    assertEquals(trigger.hashCode(), clonedTrigger.hashCode());

    expression = ExpressionFactory.fromString(" ELAPSED_TIME > 300");
    trigger = new ExecutionTrigger("slow_query", expression, new Action(Action.Type.KILL_QUERY));
    clonedTrigger = trigger.clone();
    assertNotEquals(System.identityHashCode(trigger), System.identityHashCode(clonedTrigger));
    assertNotEquals(System.identityHashCode(trigger.getExpression()), System.identityHashCode(clonedTrigger.getExpression()));
    assertNotEquals(System.identityHashCode(trigger.getExpression().getCounterLimit()),
      System.identityHashCode(clonedTrigger.getExpression().getCounterLimit()));
    assertEquals(trigger, clonedTrigger);
    assertEquals(trigger.hashCode(), clonedTrigger.hashCode());
  }

  @Test
  public void testIllegalExpressionsUnsupportedPredicate() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid expression: BYTES_READ < 1024");
    ExpressionFactory.fromString("BYTES_READ < 1024");
  }

  @Test
  public void testIllegalExpressionsMissingLimit() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid expression: BYTES_READ >");
    ExpressionFactory.fromString("BYTES_READ >");
  }

  @Test
  public void testIllegalExpressionsMissingCounter() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid expression: > 1024");
    ExpressionFactory.fromString("> 1024");
  }

  @Test
  public void testIllegalExpressionsMultipleLimit() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid expression: BYTES_READ > 1024 > 1025");
    ExpressionFactory.fromString("BYTES_READ > 1024 > 1025");
  }

  @Test
  public void testIllegalExpressionsMultipleCounters() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid expression: BYTES_READ > BYTES_READ > 1025");
    ExpressionFactory.fromString("BYTES_READ > BYTES_READ > 1025");
  }

  @Test
  public void testIllegalExpressionsInvalidLimitPost() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid expression: BYTES_READ > 1024aaaa");
    ExpressionFactory.fromString("BYTES_READ > 1024aaaa");
  }

  @Test
  public void testIllegalExpressionsInvalidLimitPre() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid expression: BYTES_READ > foo1024");
    ExpressionFactory.fromString("BYTES_READ > foo1024");
  }

  @Test
  public void testIllegalExpressionsInvalidNegativeLimit() {
    thrown.expect(IllegalArgumentException.class);
    thrown.expectMessage("Invalid expression: BYTES_READ > -1024");
    ExpressionFactory.fromString("BYTES_READ > -1024");
  }
}
