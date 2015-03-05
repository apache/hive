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

package org.apache.hadoop.hive.llap.io.decode;

import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.Consumer;
import org.apache.hadoop.hive.llap.cache.Cache;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheImpl;
import org.apache.hadoop.hive.llap.io.api.cache.LowLevelCache;
import org.apache.hadoop.hive.llap.io.api.impl.ColumnVectorBatch;
import org.apache.hadoop.hive.llap.io.api.impl.LlapIoImpl;
import org.apache.hadoop.hive.llap.io.api.orc.OrcCacheKey;
import org.apache.hadoop.hive.llap.io.encoded.OrcEncodedDataReader;
import org.apache.hadoop.hive.llap.io.metadata.OrcMetadataCache;
import org.apache.hadoop.hive.llap.metrics.LlapDaemonCacheMetrics;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.mapred.InputSplit;

public class OrcColumnVectorProducer implements ColumnVectorProducer {

  private final OrcMetadataCache metadataCache;
  private final Cache<OrcCacheKey> cache;
  private final LowLevelCache lowLevelCache;
  private final Configuration conf;
  private boolean _skipCorrupt; // TODO: get rid of this
  private LlapDaemonCacheMetrics metrics;
 
  public OrcColumnVectorProducer(OrcMetadataCache metadataCache,
      LowLevelCacheImpl lowLevelCache, Cache<OrcCacheKey> cache, Configuration conf,
      LlapDaemonCacheMetrics metrics) {
    if (LlapIoImpl.LOGL.isInfoEnabled()) {
      LlapIoImpl.LOG.info("Initializing ORC column vector producer");
    }

    this.metadataCache = metadataCache;
    this.lowLevelCache = lowLevelCache;
    this.cache = cache;
    this.conf = conf;
    this._skipCorrupt = HiveConf.getBoolVar(conf, HiveConf.ConfVars.HIVE_ORC_SKIP_CORRUPT_DATA);
    this.metrics = metrics;
  }

  @Override
  public ReadPipeline createReadPipeline(
      Consumer<ColumnVectorBatch> consumer, InputSplit split,
      List<Integer> columnIds, SearchArgument sarg, String[] columnNames) {
    metrics.incrCacheReadRequests();
    OrcEncodedDataConsumer edc = new OrcEncodedDataConsumer(
        consumer, columnIds.size(), _skipCorrupt);
    OrcEncodedDataReader reader = new OrcEncodedDataReader(
        lowLevelCache, cache, metadataCache, conf, split, columnIds, sarg, columnNames, edc);
    edc.init(reader, reader);
    return edc;
  }
}
