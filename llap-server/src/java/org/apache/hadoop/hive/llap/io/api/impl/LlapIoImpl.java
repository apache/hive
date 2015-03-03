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
import org.apache.hadoop.hive.llap.cache.LowLevelCacheMemoryManager;
import org.apache.hadoop.hive.llap.cache.LowLevelCachePolicy;
import org.apache.hadoop.hive.llap.cache.LowLevelFifoCachePolicy;
import org.apache.hadoop.hive.llap.cache.LowLevelLrfuCachePolicy;
import org.apache.hadoop.hive.llap.cache.NoopCache;
import org.apache.hadoop.hive.llap.io.api.LlapIo;
import org.apache.hadoop.hive.llap.io.api.orc.OrcCacheKey;
import org.apache.hadoop.hive.llap.io.decode.ColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.decode.OrcColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.metadata.OrcMetadataCache;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;

import com.google.common.util.concurrent.ListeningExecutorService;
import com.google.common.util.concurrent.MoreExecutors;

public class LlapIoImpl implements LlapIo<VectorizedRowBatch> {
  public static final Log LOG = LogFactory.getLog(LlapIoImpl.class);
  public static final LogLevels LOGL = new LogLevels(LOG);

  private final ColumnVectorProducer cvp;
  private final ListeningExecutorService executor;

  private LlapIoImpl(Configuration conf) throws IOException {
    boolean useLowLevelCache = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_LOW_LEVEL_CACHE);
    // High-level cache not supported yet.
    if (LOGL.isInfoEnabled()) {
      LOG.info("Initializing LLAP IO" + (useLowLevelCache ? " with low level cache" : ""));
    }
    Cache<OrcCacheKey> cache = useLowLevelCache ? null : new NoopCache<OrcCacheKey>();
    LowLevelCacheImpl orcCache = createLowLevelCache(conf, useLowLevelCache);
    OrcMetadataCache metadataCache = OrcMetadataCache.getInstance();
    // Arbitrary thread pool. Listening is used for unhandled errors for now (TODO: remove?)
    executor = MoreExecutors.listeningDecorator(Executors.newFixedThreadPool(10));

    // TODO: this should depends on input format and be in a map, or something.
    this.cvp = new OrcColumnVectorProducer(metadataCache, orcCache, cache, conf);
    if (LOGL.isInfoEnabled()) {
      LOG.info("LLAP IO initialized");
    }
  }

  private LowLevelCacheImpl createLowLevelCache(Configuration conf, boolean useLowLevelCache) {
    if (!useLowLevelCache) return null;
    boolean useLrfu = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_USE_LRFU);
    LowLevelCachePolicy cachePolicy =
        useLrfu ? new LowLevelLrfuCachePolicy(conf) : new LowLevelFifoCachePolicy(conf);
    // Memory manager uses cache policy to trigger evictions.
    LowLevelCacheMemoryManager memManager = new LowLevelCacheMemoryManager(conf, cachePolicy);
    // Allocator uses memory manager to request memory.
    Allocator allocator = new BuddyAllocator(conf, memManager);
    // Cache uses allocator to allocate and deallocate.
    LowLevelCacheImpl orcCache = new LowLevelCacheImpl(conf, cachePolicy, allocator);
    // And finally cache policy uses cache to notify it of eviction. The cycle is complete!
    cachePolicy.setEvictionListener(orcCache);
    orcCache.init();
    return orcCache;
  }

  @SuppressWarnings("rawtypes")
  @Override
  public InputFormat<NullWritable, VectorizedRowBatch> getInputFormat(
      InputFormat sourceInputFormat) {
    return new LlapInputFormat(sourceInputFormat, cvp, executor);
  }
}
