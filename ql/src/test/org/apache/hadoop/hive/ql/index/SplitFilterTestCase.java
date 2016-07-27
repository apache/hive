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

import com.google.common.collect.ImmutableSet;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.fail;

public final class SplitFilterTestCase {
  public static final long DEFAULT_SPLIT_SIZE = 1024 * 1024;
  public static final long SMALL_SPLIT_SIZE = 500;

  private final Set<HiveInputSplit> allSplits;
  private final Set<HiveInputSplit> selectedSplits;
  private final Set<HiveInputSplit> expectedSplits;
  private final long maxInputSize;

  private SplitFilterTestCase(Iterable<HiveInputSplit> allSplits,
      Iterable<HiveInputSplit> selectedSplits, Iterable<HiveInputSplit> expectedSplits,
      long maxInputSize) {

    this.allSplits = ImmutableSet.copyOf(allSplits);
    this.selectedSplits = ImmutableSet.copyOf(selectedSplits);
    this.expectedSplits = ImmutableSet.copyOf(expectedSplits);
    this.maxInputSize = maxInputSize;
  }

  private HiveInputSplit[] toArray(Collection<HiveInputSplit> splits) {
    return splits.toArray(new HiveInputSplit[splits.size()]);
  }

  public void executeAndValidate() throws IOException {
    SplitFilter filter = new SplitFilter(new MockIndexResult(selectedSplits), maxInputSize);
    List<HiveInputSplit> actualSplits = filter.filter(toArray(allSplits));
    assertSplits(expectedSplits, actualSplits);
  }

  private void assertSplits(Collection<HiveInputSplit> expectedSplits,
      Collection<HiveInputSplit> actualSplits) {
    SplitFilter.HiveInputSplitComparator hiveInputSplitComparator =
        new SplitFilter.HiveInputSplitComparator();

    List<HiveInputSplit> sortedExpectedSplits = new ArrayList<>(expectedSplits);
    Collections.sort(sortedExpectedSplits, hiveInputSplitComparator);

    List<HiveInputSplit> sortedActualSplits = new ArrayList<>(actualSplits);
    Collections.sort(sortedActualSplits, hiveInputSplitComparator);

    assertEquals("Number of selected splits.", sortedExpectedSplits.size(),
        sortedActualSplits.size());

    for (int i = 0; i < sortedExpectedSplits.size(); i++) {
      HiveInputSplit expectedSplit = sortedExpectedSplits.get(i);
      HiveInputSplit actualSplit = sortedActualSplits.get(i);

      String splitName = "Split #" + i;

      assertEquals(splitName + " path.", expectedSplit.getPath(), actualSplit.getPath());
      assertEquals(splitName + " start.", expectedSplit.getStart(), actualSplit.getStart());
      assertEquals(splitName + " length.", expectedSplit.getLength(), actualSplit.getLength());
    }
  }

  public static MaxInputSizeStep builder() {
    return new SplitFilterTestCaseBuilder();
  }

  public static interface MaxInputSizeStep extends InputFilesStep {
    InputFilesStep maxInputSize(long maxInputSize);
  }

  public static interface InputFilesStep {
    ExpectedSplitsStep inputFiles(MockInputFile... inputFiles);
  }

  public static interface ExpectedSplitsStep {
    BuildStep expectedSplits(HiveInputSplit... expectedSplits);
  }

  public static interface BuildStep {
    SplitFilterTestCase build();
  }

  private static final class SplitFilterTestCaseBuilder implements MaxInputSizeStep, InputFilesStep,
      ExpectedSplitsStep, BuildStep {

    private long maxInputSize = Long.MAX_VALUE;
    private List<MockInputFile> inputFiles;
    private List<HiveInputSplit> expectedSplits;

    @Override
    public InputFilesStep maxInputSize(long maxInputSize) {
      this.maxInputSize = maxInputSize;
      return this;
    }

    @Override
    public ExpectedSplitsStep inputFiles(MockInputFile... inputFiles) {
      this.inputFiles = Arrays.asList(inputFiles);
      return this;
    }

    @Override
    public BuildStep expectedSplits(HiveInputSplit... expectedSplits) {
      this.expectedSplits = Arrays.asList(expectedSplits);
      return this;
    }

    @Override
    public SplitFilterTestCase build() {
      List<HiveInputSplit> allSplits = new ArrayList<>();
      List<HiveInputSplit> selectedSplits = new ArrayList<>();
      Set<String> seenPaths = new HashSet<String>();

      for (MockInputFile inputFile : inputFiles) {
        if (seenPaths.add(inputFile.getPath())) {
          allSplits.addAll(inputFile.getSplits());
          selectedSplits.addAll(inputFile.getSelectedSplits());
        } else {
          fail(String.format("Cannot add 2 input files with the same path to a test case. " +
              "The duplicated path is '%s'.", inputFile.getPath()));
        }
      }

      return new SplitFilterTestCase(allSplits, selectedSplits, expectedSplits, maxInputSize);
    }
  }
}
