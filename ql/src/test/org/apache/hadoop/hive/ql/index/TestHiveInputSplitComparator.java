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
package org.apache.hadoop.hive.ql.index;

import java.util.Arrays;

import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplitComparator;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.junit.runners.Parameterized.Parameter;
import org.junit.runners.Parameterized.Parameters;

import static org.apache.hadoop.hive.ql.index.MockHiveInputSplits.createMockSplit;
import static org.junit.Assert.assertEquals;

@RunWith(Parameterized.class)
public class TestHiveInputSplitComparator {

  @Parameter(0)
  public HiveInputSplit split1;
  @Parameter(1)
  public HiveInputSplit split2;
  @Parameter(2)
  public Integer expected;

  @Parameters(name = "{index}: {0}<=>{1} ")
  public static Iterable<Object[]> data() {
    return Arrays.asList(new Object[][]{
      {createMockSplit("A", 0, 100), createMockSplit("A", 1000, 100), -1},
      {createMockSplit("A", 1000, 100), createMockSplit("A", 100, 100), 1},
      {createMockSplit("A", 0, 100), createMockSplit("A", 0, 100), 0},
      {createMockSplit("A", 0, 100), createMockSplit("B", 0, 100), -1},
      {createMockSplit("A", 100, 100), createMockSplit("B", 0, 100), -1},
      {createMockSplit("A", 100, 100), createMockSplit("B", 0, 100), -1}}
    );
  }

  @Test
  public void testCompare() {
    HiveInputSplitComparator cmp = new HiveInputSplitComparator();
    int actual =  cmp.compare(split1, split2);
    assertCompareResult(expected, actual);
  }

  private void assertCompareResult(int expected, int actual) {
    assertEquals(expected, (int) Math.signum(actual));
  }
}
