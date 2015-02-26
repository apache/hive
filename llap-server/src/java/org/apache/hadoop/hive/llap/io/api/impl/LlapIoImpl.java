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

package org.apache.hadoop.hive.llap.io.api.impl;

import java.io.IOException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.LogLevels;
import org.apache.hadoop.hive.llap.cache.Allocator;
import org.apache.hadoop.hive.llap.cache.BuddyAllocator;
import org.apache.hadoop.hive.llap.cache.Cache;
import org.apache.hadoop.hive.llap.cache.LowLevelCacheImpl;
import org.apache.hadoop.hive.llap.cache.LowLevelCachePolicyBase;
import org.apache.hadoop.hive.llap.cache.LowLevelFifoCachePolicy;
import org.apache.hadoop.hive.llap.cache.LowLevelLrfuCachePolicy;
import org.apache.hadoop.hive.llap.cache.NoopCache;
import org.apache.hadoop.hive.llap.io.api.LlapIo;
import org.apache.hadoop.hive.llap.io.api.orc.OrcCacheKey;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.decode.OrcColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.encoded.OrcEncodedDataProducer;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;

public class LlapIoImpl implements LlapIo<VectorizedRowBatch> {
  public static final Log LOG = LogFactory.getLog(LlapIoImpl.class);
  public static final LogLevels LOGL = new LogLevels(LOG);

  private final OrcColumnVectorProducer cvp;
  private final OrcEncodedDataProducer edp;

  private LlapIoImpl(Configuration conf) throws IOException {
    boolean useLowLevelCache = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_LOW_LEVEL_CACHE);
    // High-level cache not supported yet.
    if (LOGL.isInfoEnabled()) {
      LOG.info("Initializing LLAP IO" + (useLowLevelCache ? " with low level cache" : ""));
    }
    Cache<OrcCacheKey> cache = useLowLevelCache ? null : new NoopCache<OrcCacheKey>();
    LowLevelCacheImpl orcCache = null;
    if (useLowLevelCache) {
      boolean useLrfu = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_USE_LRFU);
      LowLevelCachePolicyBase cachePolicy =
          useLrfu ? new LowLevelLrfuCachePolicy(conf) : new LowLevelFifoCachePolicy(conf);
      Allocator allocator = new BuddyAllocator(conf, cachePolicy);
      orcCache = new LowLevelCacheImpl(conf, cachePolicy, allocator);
    }
    // TODO: arbitrary thread pool
    ExecutorService threadPool = Executors.newFixedThreadPool(10);
    // TODO: this should depends on input format and be in a map, or something.
    this.edp = new OrcEncodedDataProducer(orcCache, cache, conf);
    this.cvp = new OrcColumnVectorProducer(threadPool, edp, conf);
    if (LOGL.isInfoEnabled()) {
      LOG.info("LLAP IO initialized");
    }
  }

  @Override
  public InputFormat<NullWritable, VectorizedRowBatch> getInputFormat(
      InputFormat sourceInputFormat) {
    return new LlapInputFormat(this, sourceInputFormat);
  }

  public ColumnVectorProducer<?> getCvp() {
    return cvp;
  }
}
