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

import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.Iterables;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.io.SequenceFile;
import org.apache.hadoop.mapred.FileSplit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class SplitFilter {
  public static final Logger LOG = LoggerFactory.getLogger(SplitFilter.class);

  private final IndexResult indexResult;
  private final long maxInputSize;

  public SplitFilter(IndexResult indexResult, long maxInputSize) {
    this.indexResult = indexResult;
    this.maxInputSize = maxInputSize;
  }

  public List<HiveInputSplit> filter(HiveInputSplit[] splits) throws IOException {
    long sumSplitLengths = 0;
    List<HiveInputSplit> newSplits = new ArrayList<>();

    Arrays.sort(splits, new HiveInputSplitComparator());

    for (HiveInputSplit split : splits) {
      LOG.info("split start : " + split.getStart());
      LOG.info("split end : " + (split.getStart() + split.getLength()));

      try {
        if (indexResult.contains(split)) {
          HiveInputSplit newSplit = split;
          if (isAdjustmentRequired(newSplits, split)) {
            newSplit = adjustSplit(split);
          }
          sumSplitLengths += newSplit.getLength();
          if (sumSplitLengths > maxInputSize) {
            String messageTemplate = "Size of data to read during a compact-index-based query " +
                "exceeded the maximum of %d set in %s";
            throw new IOException(String.format(messageTemplate, maxInputSize,
                HiveConf.ConfVars.HIVE_INDEX_COMPACT_QUERY_MAX_SIZE.varname));
          }
          newSplits.add(newSplit);
        }
      } catch (HiveException e) {
        throw new RuntimeException("Unable to get metadata for input table split " +
            split.getPath(), e);
      }
    }
    LOG.info("Number of input splits: {}, new input splits: {}, sum of split lengths: {}",
        splits.length, newSplits.size(), sumSplitLengths);
    return newSplits;
  }

  private boolean isAdjustmentRequired(List<HiveInputSplit> newSplits, HiveInputSplit split) {
    return (split.inputFormatClassName().contains("RCFile") ||
        split.inputFormatClassName().contains("SequenceFile")) && split.getStart() > 0 &&
        !doesOverlap(newSplits, split.getPath(), adjustStart(split.getStart()));
  }

  private boolean doesOverlap(List<HiveInputSplit> newSplits, Path path, long start) {
    if (newSplits.isEmpty()) {
      return false;
    }
    HiveInputSplit lastSplit = Iterables.getLast(newSplits);
    if (lastSplit.getPath().equals(path)) {
      return lastSplit.getStart() + lastSplit.getLength() > start;
    }
    return false;
  }

  private long adjustStart(long start) {
    return start > SequenceFile.SYNC_INTERVAL ? start - SequenceFile.SYNC_INTERVAL : 0;
  }

  private HiveInputSplit adjustSplit(HiveInputSplit split) throws IOException {
    long adjustedStart = adjustStart(split.getStart());
    return new HiveInputSplit(new FileSplit(split.getPath(), adjustedStart,
        split.getStart() - adjustedStart + split.getLength(), split.getLocations()),
        split.inputFormatClassName());
  }

  @VisibleForTesting
  static final class HiveInputSplitComparator implements Comparator<HiveInputSplit> {
    @Override
    public int compare(HiveInputSplit o1, HiveInputSplit o2) {
      int pathCompare = comparePath(o1.getPath(), o2.getPath());
      if (pathCompare != 0) {
        return pathCompare;
      }
      return Long.compare(o1.getStart(), o2.getStart());
    }

    private int comparePath(Path p1, Path p2) {
      return p1.compareTo(p2);
    }
  }
}
