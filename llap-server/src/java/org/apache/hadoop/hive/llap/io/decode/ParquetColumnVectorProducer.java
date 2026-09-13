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
import org.apache.hadoop.hive.ql.io.orc.encoded.Consumer;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
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
      Reporter reporter, JobConf job, Map<Path, PartitionDesc> parts) throws IOException {
    try {
      ParquetEncodedDataConsumer edc =
          new ParquetEncodedDataConsumer(consumer, includes, counters, ioMetrics, job);
      ParquetEncodedDataReader reader = new ParquetEncodedDataReader(
          lowLevelCache, bufferManager, conf, job, split, includes, edc, counters);
      reader.loadFooter();
      // Null makes LlapInputFormat fall back to the normal VectorizedParquetRecordReader.
      if (reader.projectsNestedTypes()) {
        LlapIoImpl.LOG.info("Parquet native cache: falling back to normal reader for {} due to "
            + "nested types in the projection", split.getPath());
        return null;
      }
      edc.init(reader, reader);
      return edc;
    } catch (IOException e) {
      LlapIoImpl.LOG.info("Parquet native cache: falling back to normal reader for {} due to {}",
          split.getPath(), e.toString());
      return null;
    }
  }
}
