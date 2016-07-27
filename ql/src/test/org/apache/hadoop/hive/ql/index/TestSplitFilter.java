/**
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

import java.io.IOException;
import org.junit.Test;

import static org.apache.hadoop.hive.ql.index.MockHiveInputSplits.createMockSplit;
import static org.apache.hadoop.io.SequenceFile.SYNC_INTERVAL;
import static org.apache.hadoop.hive.ql.index.SplitFilterTestCase.DEFAULT_SPLIT_SIZE;
import static org.apache.hadoop.hive.ql.index.SplitFilterTestCase.SMALL_SPLIT_SIZE;

public class TestSplitFilter {
  private SplitFilterTestCase testCase;

  @Test
  public void testOneSelectedSplitsInMiddle() throws Exception {
    testCase = SplitFilterTestCase.builder()
        .inputFiles(
            MockInputFile.builder()
                .path("A")
                .split()
                .selectedSplit()
                .split()
                .build()
        )
        .expectedSplits(
            createMockSplit("A", DEFAULT_SPLIT_SIZE - SYNC_INTERVAL, DEFAULT_SPLIT_SIZE + SYNC_INTERVAL)
        )
        .build();

    testCase.executeAndValidate();
  }

  @Test
  public void testSelectedFirstSplit() throws Exception {
    testCase = SplitFilterTestCase.builder()
        .inputFiles(
            MockInputFile.builder()
                .path("A")
                .selectedSplit()
                .split()
                .split()
                .build()
        )
        .expectedSplits(
            createMockSplit("A", 0, DEFAULT_SPLIT_SIZE)
        )
        .build();

    testCase.executeAndValidate();
  }

  @Test
  public void testSelectedLastSplit() throws Exception {
    int lastSplitSize = 1234;

    testCase = SplitFilterTestCase.builder()
        .inputFiles(
            MockInputFile.builder()
                .path("A")
                .split()
                .selectedSplit(lastSplitSize)
                .build()
        )
        .expectedSplits(
            createMockSplit("A", DEFAULT_SPLIT_SIZE - SYNC_INTERVAL, lastSplitSize + SYNC_INTERVAL)
        )
        .build();

    testCase.executeAndValidate();
  }

  @Test
  public void testSelectedTwoAdjacentSplits() throws Exception {

    testCase = SplitFilterTestCase.builder()
        .inputFiles(
            MockInputFile.builder()
                .path("A")
                .selectedSplit()
                .selectedSplit()
                .split()
                .build()
        )
        .expectedSplits(
            createMockSplit("A", 0, DEFAULT_SPLIT_SIZE),
            createMockSplit("A", DEFAULT_SPLIT_SIZE, DEFAULT_SPLIT_SIZE)
        )
        .build();

    testCase.executeAndValidate();
  }

  @Test
  public void testSelectedThreeAdjacentSplits() throws Exception {

    testCase = SplitFilterTestCase.builder()
        .inputFiles(
            MockInputFile.builder()
                .path("A")
                .selectedSplit()
                .selectedSplit()
                .selectedSplit()
                .split()
                .build()
        )
        .expectedSplits(
            createMockSplit("A", 0, DEFAULT_SPLIT_SIZE),
            createMockSplit("A", DEFAULT_SPLIT_SIZE, DEFAULT_SPLIT_SIZE),
            createMockSplit("A", DEFAULT_SPLIT_SIZE * 2, DEFAULT_SPLIT_SIZE)
        )
        .build();

    testCase.executeAndValidate();
  }

  @Test
  public void testSelectedSplitsInTwoFiles() throws Exception {

    testCase = SplitFilterTestCase.builder()
        .inputFiles(
            MockInputFile.builder()
                .path("A")
                .selectedSplit()
                .split()
                .build(),
            MockInputFile.builder()
                .path("B")
                .selectedSplit()
                .build()
        )
        .expectedSplits(
            createMockSplit("A", 0, DEFAULT_SPLIT_SIZE),
            createMockSplit("B", 0, DEFAULT_SPLIT_SIZE)
        )
        .build();

    testCase.executeAndValidate();
  }

  @Test
  public void testOverlapWithPreviousFile() throws Exception {

    testCase = SplitFilterTestCase.builder()
        .inputFiles(
            MockInputFile.builder()
                .path("A")
                .selectedSplit()
                .build(),
            MockInputFile.builder()
                .path("B")
                .split()
                .selectedSplit()
                .build()
        )
        .expectedSplits(
            createMockSplit("A", 0, DEFAULT_SPLIT_SIZE),
            createMockSplit("B", DEFAULT_SPLIT_SIZE - SYNC_INTERVAL, DEFAULT_SPLIT_SIZE + SYNC_INTERVAL)
        )
        .build();

    testCase.executeAndValidate();
  }

  @Test
  public void testOverlapInSecondFile() throws Exception {

    testCase = SplitFilterTestCase.builder()
        .inputFiles(
            MockInputFile.builder()
                .path("A")
                .selectedSplit()
                .build(),
            MockInputFile.builder()
                .path("B")
                .split()
                .selectedSplit()
                .selectedSplit()
                .build()
        )
        .expectedSplits(
            createMockSplit("A", 0, DEFAULT_SPLIT_SIZE),
            createMockSplit("B", DEFAULT_SPLIT_SIZE - SYNC_INTERVAL, DEFAULT_SPLIT_SIZE + SYNC_INTERVAL),
            createMockSplit("B", DEFAULT_SPLIT_SIZE * 2, DEFAULT_SPLIT_SIZE)
        )
        .build();

    testCase.executeAndValidate();
  }

  @Test
  public void testSmallSplitsLengthAdjustment() throws Exception {

    testCase = SplitFilterTestCase.builder()
        .inputFiles(
            MockInputFile.builder()
                .path("A")
                .defaultSplitLength(SMALL_SPLIT_SIZE)
                .split()
                .selectedSplit()
                .build()
        )
        .expectedSplits(
            createMockSplit("A", 0, SMALL_SPLIT_SIZE * 2)
        )
        .build();

    testCase.executeAndValidate();
  }

  @Test
  public void testSmallSplitsOverlap() throws Exception {

    testCase = SplitFilterTestCase.builder()
        .inputFiles(
            MockInputFile.builder()
                .path("A")
                .defaultSplitLength(SMALL_SPLIT_SIZE)
                .selectedSplit()
                .split()
                .selectedSplit()
                .split()
                .selectedSplit()
                .build()
        )
        .expectedSplits(
            createMockSplit("A", 0, SMALL_SPLIT_SIZE),
            createMockSplit("A", SMALL_SPLIT_SIZE * 2, SMALL_SPLIT_SIZE),
            createMockSplit("A", SMALL_SPLIT_SIZE * 4, SMALL_SPLIT_SIZE)
        )
        .build();

    testCase.executeAndValidate();
  }

  @Test
  public void testMaxSplitsSizePositive() throws Exception {

    testCase = SplitFilterTestCase.builder()
        .maxInputSize(DEFAULT_SPLIT_SIZE * 3 + SYNC_INTERVAL * 2)
        .inputFiles(
            MockInputFile.builder()
                .path("A")
                .selectedSplit()
                .split()
                .selectedSplit()
                .split()
                .selectedSplit()
                .build()
        )
        .expectedSplits(
            createMockSplit("A", 0, DEFAULT_SPLIT_SIZE),
            createMockSplit("A", DEFAULT_SPLIT_SIZE * 2 - SYNC_INTERVAL, DEFAULT_SPLIT_SIZE + SYNC_INTERVAL),
            createMockSplit("A", DEFAULT_SPLIT_SIZE * 4 - SYNC_INTERVAL, DEFAULT_SPLIT_SIZE + SYNC_INTERVAL)
        )
        .build();

    testCase.executeAndValidate();
  }

  @Test(expected = IOException.class)
  public void testMaxSplitsSizeNegative() throws Exception {
    testCase = SplitFilterTestCase.builder()
        .maxInputSize(DEFAULT_SPLIT_SIZE * 3)
        .inputFiles(
            MockInputFile.builder()
                .path("A")
                .selectedSplit()
                .split()
                .selectedSplit()
                .split()
                .selectedSplit()
                .build()
        )
        .expectedSplits()
        .build();

    testCase.executeAndValidate();
  }
}
