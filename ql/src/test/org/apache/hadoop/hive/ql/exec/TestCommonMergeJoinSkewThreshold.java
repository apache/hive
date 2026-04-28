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

public class TestCommonMergeJoinSkewThreshold {

  private CommonMergeJoinOperator op;

  @Before
  public void setUp() {
    op = new CommonMergeJoinOperator();
  }

  @Test
  public void testDisabled_noWarnNoThrow() throws HiveException {
    op.skewedMergeJoinMonitor = new SkewedMergeJoinMonitor(
            -1L, false, 4);

    op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, Long.MAX_VALUE);
  }

  @Test
  public void testBelowThreshold_isOk() throws HiveException {
    op.skewedMergeJoinMonitor = new SkewedMergeJoinMonitor(
            1000L, false, 4);
    op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, 999L);
  }

  @Test
  public void testAtThreshold_warnOnce() throws HiveException {
    op.skewedMergeJoinMonitor = new SkewedMergeJoinMonitor(
            500L, false, 4);

    // should warn without throwing
    op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, 500L);

    Assert.assertTrue("skewedKeyFlagged[0] must be set after the first crossing",
        op.skewedMergeJoinMonitor.isFlagged(0));

  }

  @Test
  public void testFlagsAreIndependentPerTag() throws HiveException {
    op.skewedMergeJoinMonitor = new SkewedMergeJoinMonitor(
            100L, false, 4);

    op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, 200L);
    Assert.assertTrue("tag 0 should be flagged", op.skewedMergeJoinMonitor.isFlagged(0));
    Assert.assertFalse("tag 1 should still be clear", op.skewedMergeJoinMonitor.isFlagged(1));

    op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 1, 150L);
    Assert.assertTrue("tag 1 should now be flagged", op.skewedMergeJoinMonitor.isFlagged(1));
  }

  @Test
  public void testAbortMode_belowThreshold_noThrow() throws HiveException {
    op.skewedMergeJoinMonitor = new SkewedMergeJoinMonitor(
            100L, true, 4);

    op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, 99L);
  }

  @Test
  public void testAbortMode_throwsHiveException() {
    op.skewedMergeJoinMonitor = new SkewedMergeJoinMonitor(
            100L, true, 4);

    try {
      op.skewedMergeJoinMonitor.checkMergeJoinSkew((byte) 0, 200L);
      Assert.fail("Expected HiveException to be thrown in abort mode");
    } catch (HiveException e) {
      String msg = e.getMessage();
      Assert.assertNotNull(msg);
      Assert.assertTrue("Message should mention row count 200", msg.contains("200"));
    }
  }
}

