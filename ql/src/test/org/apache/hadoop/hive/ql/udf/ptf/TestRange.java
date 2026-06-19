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
package org.apache.hadoop.hive.ql.udf.ptf;

import org.junit.Test;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.PTFPartition;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.junit.Assert;

public class TestRange {

  @Test
  public void testRangeHashCode() {
    Range range1 = new Range(0, 10, null);
    Range range2 = new Range(0, 10, null);
    Assert.assertTrue(range1.hashCode() == range2.hashCode());
  }

  @Test
  public void testRangeDifferentHashCode() {
    Range range1 = new Range(0, 10, null);
    Range range2 = new Range(0, 20, null);
    Assert.assertFalse(range1.hashCode() == range2.hashCode());
  }

  @Test
  public void testRangeEqualsNoRange() {
    Range range1 = new Range(0, 10, null);
    Range range2 = new Range(0, 10, null);
    Assert.assertTrue(range1.equals(range2));
  }

  @Test
  public void testRangeEqualsOnlyIfSamePartition() throws HiveException {
    // in the scope of this test, rolling/not rolling doesn't count, it's used only because easier initialization
    PTFPartition partition1 =
        PTFPartition.createRolling(new Configuration(), null, null, null, 0, 0);
    PTFPartition partition2 =
        PTFPartition.createRolling(new Configuration(), null, null, null, 0, 0);

    Range range1 = new Range(0, 10, partition1);
    Range range2 = new Range(0, 10, partition2);
    Range range3 = new Range(0, 10, partition2);

    Assert.assertFalse(range1.equals(range2));
    Assert.assertTrue(range2.equals(range3));
  }

  @Test
  public void testRangeDoesntEqual() {
    Range range1 = new Range(0, 10, null);
    Range range2 = new Range(0, 20, null);
    Assert.assertFalse(range1.equals(range2));
  }

  @Test
  public void testRangeDoesntEqualNull() {
    Range range1 = new Range(0, 10, null);
    Assert.assertFalse(range1.equals(null));
  }
}
