/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.ql.parse;

import org.junit.Test;
import static org.junit.Assert.*;

public class TestSplitSample {

  private static final int SEED_NUM = 123;
  private static final double PERCENT = 2.0;
  private static final long TOTAL_LENGTH = 1000L;
  private static final int ROW_COUNT = 5;
  private SplitSample splitSample;

  @Test
  public void testGetTargetSizeTotalLength() {
    splitSample = new SplitSample(TOTAL_LENGTH, SEED_NUM);
    assertEquals(TOTAL_LENGTH, splitSample.getTargetSize(1000));
    assertEquals(TOTAL_LENGTH, splitSample.getTargetSize(100));
  }

  @Test
  public void testGetTargetSizePercent() {
    splitSample = new SplitSample(PERCENT, SEED_NUM);
    assertEquals(20, splitSample.getTargetSize(1000));
  }

  @Test
  public void testEstimateSourceSizeTotalLength() {
    splitSample = new SplitSample(TOTAL_LENGTH, SEED_NUM);
    assertEquals(10, splitSample.estimateSourceSize(10));
  }

  @Test
  public void testEstimateSourceSizeRowCount() {
    splitSample = new SplitSample(ROW_COUNT);
    assertEquals(123, splitSample.estimateSourceSize(123));
  }

  @Test
  public void testEstimateSourceSizePercent() {
    splitSample = new SplitSample(PERCENT, SEED_NUM);
    assertEquals(500, splitSample.estimateSourceSize(10));
  }
}
