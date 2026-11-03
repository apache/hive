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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.llap.io.decode;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.llap.cache.BufferUsageManager;
import org.apache.hadoop.hive.llap.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.counters.QueryFragmentCounters;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.encoded.ParquetEncodedDataReader;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonIOMetrics;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatchCtx;
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.hive.ql.metadata.VirtualColumn;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

public class ParquetColumnVectorProducer implements ColumnVectorProducer {
  private final LowLevelCache lowLevelCache;
  private final BufferUsageManager bufferManager;
  private final Configuration conf;
  private final LlapDaemonCacheMetrics cacheMetrics;
  private final LlapDaemonIOMetrics ioMetrics;

  public ParquetColumnVectorProducer(LowLevelCache lowLevelCache, BufferUsageManager bufferManager,
      Configuration conf, LlapDaemonCacheMetrics cacheMetrics, LlapDaemonIOMetrics ioMetrics) {
    this.lowLevelCache = lowLevelCache;
    this.bufferManager = bufferManager;
    this.conf = conf;
    this.cacheMetrics = cacheMetrics;
    this.ioMetrics = ioMetrics;
  }

  @Override
  public ReadPipeline createReadPipeline(Consumer<ColumnVectorBatch> consumer, FileSplit split,
      Includes includes, SearchArgument sarg, QueryFragmentCounters counters,
      SchemaEvolutionFactory sef, InputFormat<?, ?> sourceInputFormat, Deserializer sourceSerDe,
      Reporter reporter, JobConf job, Map<Path, PartitionDesc> parts) {
    // Before the footer read, so a decline costs no IO. This pipeline cannot fill a row-lineage
    // slot, and an unfilled one is not a visible failure: the -1 it leaves is the sentinel
    // HiveBatchIterator reads as "synthesize the id from the manifest", so the query would answer
    // with plausible wrong ids. Only the fallback reader augments its schema for these columns.
    if (projectsRowLineage(job)) {
      LlapIoImpl.LOG.debug("Parquet native cache: falling back to normal reader for {} due to "
          + "row-lineage virtual columns", split.getPath());
      return null;
    }
    try {
      ParquetEncodedDataConsumer edc =
          new ParquetEncodedDataConsumer(consumer, includes, counters, ioMetrics, job);
      ParquetEncodedDataReader reader = new ParquetEncodedDataReader(
          lowLevelCache, bufferManager, conf, job, split, edc, counters);
      reader.readFooter();
      // Null makes LlapInputFormat fall back to the normal VectorizedParquetRecordReader. The
      // fallback still caches (footer via LlapProxy, data via LlapCacheAwareFs); what's lost for
      // this split is vectored reads, row-group lookahead, and its LLAP IO summary counters.
      if (reader.projectsNestedTypes()) {
        // DEBUG, not INFO: this is the planned outcome for a whole class of queries, and it fires
        // once per split, so at INFO a nested-type scan fills the daemon log with a line per split
        // saying nothing went wrong.
        LlapIoImpl.LOG.debug("Parquet native cache: falling back to normal reader for {} due to "
            + "nested types in the projection", split.getPath());
        return null;
      }
      // Counted here rather than on entry: a split that declines above, or that throws on the way
      // to this point, never reads the cache at all, and counting it would make the daemon's cache
      // hit ratio a ratio over requests that were never made.
      cacheMetrics.incrCacheReadRequests();
      edc.init(reader, reader);
      return edc;
    } catch (IOException e) {
      // WARN with the throwable, unlike the decline above: the query still answers, but reading the
      // footer failed and nobody downstream will report it, so the stack is the only record of why
      // this split lost its vectored reads and its IO counters. e.toString() dropped it.
      LlapIoImpl.LOG.warn("Parquet native cache: falling back to normal reader for {}",
          split.getPath(), e);
      return null;
    }
  }

  /**
   * Whether the query projects {@code ROW__LINEAGE__ID} or
   * {@code LAST__UPDATED__SEQUENCE__NUMBER}. The projection only, not whether the file carries the
   * columns - that would need the footer, to recover a split both readers answer the same way.
   *
   * <p>Only Iceberg tables can project these, and their Parquet reads do not come here yet.
   * HIVE-30103 changes that, and should replace this with real support rather than drop it.
   */
  private static boolean projectsRowLineage(JobConf job) {
    VectorizedRowBatchCtx rbCtx = Utilities.getVectorizedRowBatchCtx(job);
    return rbCtx != null
        && (rbCtx.isVirtualColumnNeeded(VirtualColumn.ROW_LINEAGE_ID.getName())
            || rbCtx.isVirtualColumnNeeded(VirtualColumn.LAST_UPDATED_SEQUENCE_NUMBER.getName()));
  }
}
