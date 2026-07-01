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
package org.apache.hadoop.hive.ql.exec;

import org.apache.hadoop.hive.ql.exec.tez.monitoring.SkewedMergeJoinMonitor;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit tests for merge join skew threshold detection in {@link CommonMergeJoinOperator}.
 */
public class TestCommonMergeJoinSkewThreshold {

  private static final String KEY_COLS_A = "customer_id";
  private static final String TABLE_ALIAS_A = "orders";

  private CommonMergeJoinOperator op;

  @Before
  public void setUp() {
    op = new CommonMergeJoinOperator();
  }

  @Test
  public void testDisabledNoWarnNoThrow() throws HiveException {
    SkewedMergeJoinMonitor monitor = new SkewedMergeJoinMonitor(-1L, false, 1L, 4);
    op.skewedMergeJoinMonitor = monitor;

    op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, Long.MAX_VALUE, KEY_COLS_A, TABLE_ALIAS_A);
    Assert.assertFalse(monitor.isActive());
    Assert.assertFalse("tag 0 should still be clear", monitor.isFlagged(0));
  }

  @Test
  public void testBelowThresholdIsOk() throws HiveException {
    SkewedMergeJoinMonitor monitor = new SkewedMergeJoinMonitor(1000L, false, 1L, 4);
    op.skewedMergeJoinMonitor = monitor;

    op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, 999L, KEY_COLS_A, TABLE_ALIAS_A);
    Assert.assertFalse("tag 0 should still be clear", monitor.isFlagged(0));
  }

  @Test
  public void testAtThresholdWarnOnce() throws HiveException {
    SkewedMergeJoinMonitor monitor = new SkewedMergeJoinMonitor(500L, false, 1L, 4);
    op.skewedMergeJoinMonitor = monitor;

    op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, 500L, KEY_COLS_A, TABLE_ALIAS_A);

    Assert.assertTrue("skewedKeyFlagged[0] must be set after the first crossing", monitor.isFlagged(0));
  }

  @Test
  public void testFlagsAreIndependentPerTag() throws HiveException {
    SkewedMergeJoinMonitor monitor = new SkewedMergeJoinMonitor(100L, false, 1L, 4);
    op.skewedMergeJoinMonitor = monitor;

    op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, 200L, KEY_COLS_A, TABLE_ALIAS_A);
    Assert.assertTrue("tag 0 should be flagged", monitor.isFlagged(0));
    Assert.assertFalse("tag 1 should still be clear", monitor.isFlagged(1));

    op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 1, 150L, KEY_COLS_A, TABLE_ALIAS_A);
    Assert.assertTrue("tag 1 should now be flagged", monitor.isFlagged(1));
  }

  @Test
  public void testAbortModeBelowThresholdNoThrow() throws HiveException {
    op.skewedMergeJoinMonitor = new SkewedMergeJoinMonitor(100L, true, 1L, 4);
    op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, 99L, KEY_COLS_A, TABLE_ALIAS_A);
  }

  @Test
  public void testAbortModeThrowsHiveException() {
    op.skewedMergeJoinMonitor = new SkewedMergeJoinMonitor(100L, true, 1L, 4);

    try {
      op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, 200L, KEY_COLS_A, TABLE_ALIAS_A);
      Assert.fail("Expected HiveException to be thrown in abort mode");
    } catch (HiveException e) {
      String msg = e.getMessage();
      Assert.assertNotNull(msg);
      Assert.assertTrue("Message should mention row count 200", msg.contains("200"));
      Assert.assertTrue("Message should mention join column name", msg.contains(KEY_COLS_A));
      Assert.assertTrue("Message should mention table alias", msg.contains(TABLE_ALIAS_A));
    }
  }

  @Test
  public void testIntervalSkipsCheckOnNonBoundaryRows() throws HiveException {
    SkewedMergeJoinMonitor monitor = new SkewedMergeJoinMonitor(500L, false, 10000L, 4);
    op.skewedMergeJoinMonitor = monitor;

    op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, 100L, KEY_COLS_A, TABLE_ALIAS_A);
    Assert.assertFalse("Should not be flagged at non-boundary row below threshold", monitor.isFlagged(0));
  }

  @Test
  public void testIntervalChecksOnBoundaryRow() throws HiveException {
    SkewedMergeJoinMonitor monitor = new SkewedMergeJoinMonitor(50L, false, 100L, 4);
    op.skewedMergeJoinMonitor = monitor;

    op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, 100L, KEY_COLS_A, TABLE_ALIAS_A);
    Assert.assertTrue("Should be flagged at boundary row that exceeds threshold", monitor.isFlagged(0));
  }

  @Test
  public void testIntervalAlwaysChecksWhenRowCountExceedsThreshold() throws HiveException {
    SkewedMergeJoinMonitor monitor = new SkewedMergeJoinMonitor(500L, false, 10000L, 4);
    op.skewedMergeJoinMonitor = monitor;

    op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, 600L, KEY_COLS_A, TABLE_ALIAS_A);
    Assert.assertTrue("Should be flagged when rowCount >= threshold even at non-boundary row", monitor.isFlagged(0));
  }

  @Test
  public void testIsDueForCheckBoundaryAndThreshold() {
    SkewedMergeJoinMonitor monitor = new SkewedMergeJoinMonitor(500L, false, 100L, 4);

    Assert.assertTrue("Row 100 is a boundary (100 % 100 == 0)", monitor.isDueForCheck(100L));
    Assert.assertFalse("Row 50 is not a boundary and below threshold", monitor.isDueForCheck(50L));
    Assert.assertTrue("Row 500 equals threshold, always due", monitor.isDueForCheck(500L));
    Assert.assertTrue("Row 700 exceeds threshold, always due", monitor.isDueForCheck(700L));
    Assert.assertTrue("Row 200 is a boundary (200 % 100 == 0)", monitor.isDueForCheck(200L));
  }

  @Test
  public void testSkewMessageContainsJoinKeyColumnsAndRowCount() {
    op.skewedMergeJoinMonitor = new SkewedMergeJoinMonitor(10L, true, 1L, 4);

    try {
      op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, 50L, KEY_COLS_A, TABLE_ALIAS_A);
      Assert.fail("Expected HiveException");
    } catch (HiveException e) {
      String msg = e.getMessage();
      Assert.assertTrue("Message must contain join key column name", msg.contains(KEY_COLS_A));
      Assert.assertTrue("Message must contain row count", msg.contains("50"));
      Assert.assertTrue("Message must contain table alias", msg.contains(TABLE_ALIAS_A));
    }
  }
}
