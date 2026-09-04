/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.iceberg.mr.hive.stats;

import org.apache.iceberg.mr.hive.stats.IcebergColStatsWritePolicy.Inputs;
import org.junit.Test;

import static org.apache.iceberg.mr.hive.stats.IcebergColStatsWritePolicy.MERGE;
import static org.apache.iceberg.mr.hive.stats.IcebergColStatsWritePolicy.REPLACE;
import static org.apache.iceberg.mr.hive.stats.IcebergColStatsWritePolicy.SKIP;
import static org.apache.iceberg.mr.hive.stats.IcebergColStatsWritePolicy.resolve;
import static org.junit.Assert.assertEquals;

/** Every case of the decision, stated as one, without a session to make it in. */
public class TestIcebergColStatsWritePolicy {

  @Test
  public void analyzeOfTheWholeTableReplacesEveryPartition() {
    assertEquals(REPLACE, resolve(partitionLevel().analyze().build()));
  }

  @Test
  public void analyzeOfNamedPartitionsKeepsTheOthers() {
    assertEquals(MERGE, resolve(partitionLevel().analyzePartition().build()));
  }

  @Test
  public void analyzeIgnoresWhatTheSnapshotItReadsHolds() {
    // an ANALYZE inherits the last write's snapshot; what that covered says nothing about its own
    assertEquals(REPLACE, resolve(partitionLevel().analyze().holdsOnlyAddedRows().build()));
    assertEquals(MERGE, resolve(partitionLevel().analyzePartition().holdsOnlyAddedRows().build()));
  }

  @Test
  public void analyzeRecomputesEvenWhenTheStoredStatisticsAreCurrent() {
    assertEquals(REPLACE, resolve(partitionLevel().analyze().statsAccurate().wroteNoRows().build()));
  }

  @Test
  public void aWriteThatProducedEveryRowReplaces() {
    assertEquals(REPLACE, resolve(partitionLevel().holdsOnlyAddedRows().build()));
    assertEquals(REPLACE, resolve(partitionLevel().emptySnapshot().build()));
  }

  @Test
  public void aWriteThatReplacedSomePartitionsStandsForThem() {
    assertEquals(MERGE, resolve(partitionLevel().replacePartitions().build()));
    // a plain insert only added to partitions, and rows of part of a partition describe none of it
    assertEquals(SKIP, resolve(partitionLevel().build()));
  }

  @Test
  public void aWriteWithNothingToRecordLeavesThePartitionStatisticsAlone() {
    assertEquals(SKIP, resolve(partitionLevel().wroteNoRows().build()));
    assertEquals(SKIP, resolve(partitionLevel().statsAccurate().build()));
  }

  @Test
  public void tableWideStatisticsOfATableThatKeepsThemPerPartitionGoNowhere() {
    assertEquals(SKIP, resolve(tableWideStats().keepsStatsPerPartition().analyze().build()));
  }

  @Test
  public void onlyAWholeTableCompactionRefreshesStaleStatistics() {
    assertEquals(REPLACE, resolve(tableWideStats().fullTableMajorCompaction().build()));
    assertEquals(SKIP, resolve(tableWideStats().compaction().build()));
    assertEquals(SKIP, resolve(tableWideStats().fullTableMajorCompaction().statsAccurate().build()));
    // a partitioned table's compaction never rewrites the whole table, so it cannot refresh it
    assertEquals(SKIP, resolve(tableWideStats().majorCompaction().build()));
  }

  @Test
  public void aCompactionSubstitutesOnlyThePartitionsItReadWhole() {
    // the numbers of a partition it rewrote entirely stand for it, whatever it held before
    assertEquals(MERGE, resolve(partitionLevel().majorCompaction().singlePartitionRewrite().build()));
    assertEquals(MERGE,
        resolve(partitionLevel().majorCompaction().singlePartitionRewrite().statsAccurate().build()));
    // one that skipped files by size measured part of a partition, which describes none of it
    assertEquals(SKIP, resolve(partitionLevel().compaction().singlePartitionRewrite().build()));
    // and one clearing an older spec rewrites into partitions holding rows it never read
    assertEquals(SKIP, resolve(partitionLevel().majorCompaction().build()));
    assertEquals(SKIP, resolve(partitionLevel().compaction().build()));
  }

  @Test
  public void tableWideAnalyzeReplaces() {
    assertEquals(REPLACE, resolve(tableWideStats().analyze().build()));
  }

  @Test
  public void anEmptiedTableLosesTheStatisticsOfTheRowsItHeld() {
    assertEquals(REPLACE, resolve(tableWideStats().emptySnapshot().wroteNoRows().build()));
  }

  @Test
  public void anInsertAddsItsRowsToTheStoredStatistics() {
    assertEquals(MERGE, resolve(tableWideStats().build()));
  }

  @Test
  public void anOverwriteOfSomePartitionsCannotDescribeTheTable() {
    assertEquals(SKIP, resolve(tableWideStats().replacePartitions().partitioned().build()));
    assertEquals(REPLACE, resolve(tableWideStats().replacePartitions().build()));
  }

  /** A write of per-partition statistics that neither covered the table nor left it empty. */
  private static Builder partitionLevel() {
    return new Builder();
  }

  /** A write of statistics describing the table as a whole. */
  private static Builder tableWideStats() {
    return new Builder().tableWideStats();
  }

  /** Names each fact, so a case reads as the statement it stands for. */
  private static final class Builder {
    private boolean tableWideStats;
    private boolean keepsStatsPerPartition;
    private boolean analyze;
    private boolean analyzePartition;
    private boolean compaction;
    private boolean majorCompaction;
    private boolean fullTableRewrite;
    private boolean singlePartitionRewrite;
    private boolean holdsOnlyAddedRows;
    private boolean emptySnapshot;
    private boolean wroteNoRows;
    private boolean replacePartitions;
    private boolean partitioned;
    private boolean statsAccurate;

    private Builder tableWideStats() {
      tableWideStats = true;
      return this;
    }

    private Builder keepsStatsPerPartition() {
      keepsStatsPerPartition = true;
      return this;
    }

    private Builder analyze() {
      analyze = true;
      return this;
    }

    private Builder analyzePartition() {
      analyzePartition = true;
      return analyze();
    }

    private Builder compaction() {
      compaction = true;
      return this;
    }

    /** A compaction that read every file of what it was pointed at. */
    private Builder majorCompaction() {
      majorCompaction = true;
      return compaction();
    }

    /** A major compaction pointed at the whole table, which only an unpartitioned one is. */
    private Builder fullTableMajorCompaction() {
      fullTableRewrite = true;
      return majorCompaction();
    }

    /** A compaction pointed at one named partition, rather than at what an older spec left. */
    private Builder singlePartitionRewrite() {
      singlePartitionRewrite = true;
      return compaction();
    }

    private Builder holdsOnlyAddedRows() {
      holdsOnlyAddedRows = true;
      return this;
    }

    private Builder emptySnapshot() {
      emptySnapshot = true;
      return this;
    }

    private Builder wroteNoRows() {
      wroteNoRows = true;
      return this;
    }

    private Builder replacePartitions() {
      replacePartitions = true;
      return this;
    }

    private Builder partitioned() {
      partitioned = true;
      return this;
    }

    private Builder statsAccurate() {
      statsAccurate = true;
      return this;
    }

    private Inputs build() {
      return new Inputs(tableWideStats, keepsStatsPerPartition, partitioned, () -> statsAccurate,
          analyze, analyzePartition, compaction, majorCompaction, fullTableRewrite,
          singlePartitionRewrite, holdsOnlyAddedRows, emptySnapshot, wroteNoRows, replacePartitions);
    }
  }
}
