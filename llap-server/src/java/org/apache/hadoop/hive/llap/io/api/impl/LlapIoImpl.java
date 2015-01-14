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
import java.util.List;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.llap.cache.Cache;
import org.apache.hadoop.hive.llap.cache.LowLevelBuddyCache;
import org.apache.hadoop.hive.llap.cache.NoopCache;
import org.apache.hadoop.hive.llap.io.api.LlapIo;
import org.apache.hadoop.hive.llap.io.api.VectorReader;
import org.apache.hadoop.hive.llap.io.api.orc.OrcCacheKey;
import org.apache.hadoop.hive.llap.io.decode.OrcColumnVectorProducer;
import org.apache.hadoop.hive.llap.io.encoded.OrcEncodedDataProducer;
import org.apache.hadoop.hive.ql.exec.vector.VectorizedRowBatch;
import org.apache.hadoop.hive.ql.io.sarg.SearchArgument;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;

public class LlapIoImpl implements LlapIo, Configurable {
  public static final Log LOG = LogFactory.getLog(LlapIoImpl.class);
  private final Configuration conf;
  // TODO: For now, keep I/O subsystem here as singleton.
  //       When overall code becomes more clear it can be created during server initialization.
  private static final Object instanceLock = new Object();
  private static LlapIoImpl ioImpl = null;

  private final OrcColumnVectorProducer cvp;
  private final OrcEncodedDataProducer edp;

  private LlapIoImpl(Configuration conf) throws IOException {
    this.conf = conf;
    boolean useLowLevelCache = HiveConf.getBoolVar(conf, HiveConf.ConfVars.LLAP_LOW_LEVEL_CACHE);
    // High-level cache not supported yet.
    Cache<OrcCacheKey> cache = useLowLevelCache ? null : new NoopCache<OrcCacheKey>();
    LowLevelBuddyCache orcCache = useLowLevelCache ? new LowLevelBuddyCache(conf) : null;
    this.edp = new OrcEncodedDataProducer(orcCache, cache, conf);
    this.cvp = new OrcColumnVectorProducer(edp, conf);
  }

  @Override
  public void setConf(Configuration conf) {
    getOrCreateInstance(conf);
  }

  // TODO: Add "create" method in a well-defined place when server is started
  public static LlapIo getOrCreateInstance(Configuration conf) {
    if (ioImpl != null) return ioImpl;
    synchronized (instanceLock) {
      if (ioImpl != null) return ioImpl;
      try {
        return (ioImpl = new LlapIoImpl(conf));
      } catch (IOException e) {
        throw new RuntimeException("Cannot initialize local server", e);
      }
    }
  }

  @Override
  public Configuration getConf() {
    return ioImpl == null ? null : ioImpl.conf;
  }

  @Override
  public VectorReader getReader(InputSplit split, List<Integer> columnIds, SearchArgument sarg) {
    return new VectorReaderImpl(split, columnIds, sarg, cvp);
  }

  @Override
  public InputFormat<NullWritable, VectorizedRowBatch> getInputFormat() {
    return new LlapInputFormat(this);
  }
}
