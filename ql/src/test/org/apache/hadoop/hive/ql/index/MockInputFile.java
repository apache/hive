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

import com.google.common.collect.ImmutableList;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;

public final class MockInputFile {
  private final String path;
  private final ImmutableList<HiveInputSplit> splits;
  private final ImmutableList<HiveInputSplit> selectedSplits;

  private MockInputFile(String path, List<HiveInputSplit> splits,
      List<HiveInputSplit> selectedSplits) {
    this.path = path;
    this.splits = ImmutableList.copyOf(splits);
    this.selectedSplits = ImmutableList.copyOf(selectedSplits);
  }

  public String getPath() {
    return path;
  }

  public List<HiveInputSplit> getSplits() {
    return splits;
  }

  public List<HiveInputSplit> getSelectedSplits() {
    return selectedSplits;
  }

  public static PathStep builder() {
    return new MockInputFileBuilder();
  }

  public static interface PathStep {
    DefaultSplitLengthStep path(String path);
  }

  public static interface DefaultSplitLengthStep extends SplitStep {
    SplitStep defaultSplitLength(long defaultSplitLength);
  }

  public static interface SplitStep {
    SplitStep split();
    SplitStep selectedSplit();
    LastSplitStep split(long lastSplitSize);
    LastSplitStep selectedSplit(long lastSplitSize);
    MockInputFile build();
  }

  public static interface LastSplitStep {
    MockInputFile build();
  }

  private static final class MockInputFileBuilder implements PathStep, SplitStep, LastSplitStep,
      DefaultSplitLengthStep {

    private String path;
    private long defaultSplitSize = SplitFilterTestCase.DEFAULT_SPLIT_SIZE;;
    private final List<HiveInputSplit> splits = new ArrayList<>();
    private final List<HiveInputSplit> selectedSplits = new ArrayList<>();
    private long position = 0;

    @Override
    public DefaultSplitLengthStep path(String path) {
      this.path = path;
      return this;
    }

    @Override
    public SplitStep split() {
      nextSplit(defaultSplitSize);
      return this;
    }

    @Override
    public LastSplitStep split(long lastSplitSize) {
      nextSplit(lastSplitSize);
      return this;
    }

    @Override
    public SplitStep selectedSplit() {
      selectedSplits.add(nextSplit(defaultSplitSize));
      return this;
    }

    @Override
    public LastSplitStep selectedSplit(long lastSplitSize) {
      selectedSplits.add(nextSplit(lastSplitSize));
      return this;
    }

    @Override
    public SplitStep defaultSplitLength(long defaultSplitLength) {
      this.defaultSplitSize = defaultSplitLength;
      return this;
    }

    private HiveInputSplit nextSplit(long splitSize) {
      HiveInputSplit split = MockHiveInputSplits.createMockSplit(path, position, splitSize);
      position += splitSize;
      splits.add(split);
      return split;
    }

    @Override
    public MockInputFile build() {
      return new MockInputFile(path, splits, selectedSplits);
    }
  }
}
