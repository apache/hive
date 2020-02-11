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

package org.apache.hadoop.hive.ql.plan.mapper;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.hadoop.hive.metastore.api.RuntimeStat;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.optimizer.signature.OpTreeSignature;
import org.apache.hadoop.hive.ql.optimizer.signature.RelTreeSignature;
import org.apache.hadoop.hive.ql.optimizer.signature.RuntimeStatsPersister;
import org.apache.hadoop.hive.ql.stats.OperatorStats;
import org.apache.thrift.TException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Charsets;

/**
 * Decorates a StatSource to be loaded and persisted in the metastore as well.
 */
class MetastoreStatsConnector implements StatsSource {

  private static final Logger LOG = LoggerFactory.getLogger(MetastoreStatsConnector.class);

  private final StatsSource ss;

  private ExecutorService executor;

  MetastoreStatsConnector(int cacheSize, int batchSize, StatsSource ss) {
    this.ss = ss;
    executor = Executors.newSingleThreadExecutor(
        new BasicThreadFactory.Builder()
            .namingPattern("Metastore-RuntimeStats-Loader-%d")
            .daemon(true)
            .build());

    executor.submit(new RuntimeStatsLoader(cacheSize, batchSize));
  }

  private class RuntimeStatsLoader implements Runnable {

    private int maxEntriesToLoad;
    private int batchSize;

    public RuntimeStatsLoader(int maxEntriesToLoad, int batchSize) {
      this.maxEntriesToLoad = maxEntriesToLoad;
      if (batchSize <= 0) {
        this.batchSize = -1;
      } else {
        this.batchSize = batchSize;
      }
    }

    @Override
    public void run() {
      int lastCreateTime = Integer.MAX_VALUE;
      int loadedEntries = 0;
      try {
        do {
          List<RuntimeStat> rs = Hive.get().getMSC().getRuntimeStats(batchSize, lastCreateTime);
          if (rs.size() == 0) {
            break;
          }
          for (RuntimeStat thriftStat : rs) {
            loadedEntries += thriftStat.getWeight();
            lastCreateTime = Math.min(lastCreateTime, thriftStat.getCreateTime() - 1);
            try {
              ss.load(decode(thriftStat));
            } catch (IOException e) {
              logException("Exception while loading runtime stats", e);
            }
          }
        } while (batchSize > 0 && loadedEntries < maxEntriesToLoad);
      } catch (TException | HiveException e) {
        logException("Exception while reading metastore runtime stats", e);
      }
    }
  }

  @Override
  public boolean canProvideStatsFor(Class<?> clazz) {
    return ss.canProvideStatsFor(clazz);
  }

  @Override
  public Optional<OperatorStats> lookup(OpTreeSignature treeSig) {
    return ss.lookup(treeSig);
  }

  @Override
  public Optional<OperatorStats> lookup(RelTreeSignature treeSig) {
    return ss.lookup(treeSig);
  }

  @Override
  public void load(List<PersistedRuntimeStats> statList) {
    if (statList.size() == 0) {
      return;
    }
    ss.load(statList);
    executor.submit(new RuntimeStatsSubmitter(statList));
  }

  class RuntimeStatsSubmitter implements Runnable {

    private List<PersistedRuntimeStats> list;

    public RuntimeStatsSubmitter(List<PersistedRuntimeStats> statList) {
      this.list = statList;
    }

    @Override
    public void run() {
      try {
        RuntimeStat rec = encode(list);
        Hive.get().getMSC().addRuntimeStat(rec);
      } catch (TException | HiveException | IOException e) {
        logException("Exception while persisting runtime stat", e);
      }
    }
  }

  private RuntimeStat encode(List<PersistedRuntimeStats> list) throws IOException {
    String payload = RuntimeStatsPersister.INSTANCE.encode(new ArrayList<PersistedRuntimeStats>(list));
    RuntimeStat rs = new RuntimeStat();
    rs.setWeight(list.size());
    rs.setPayload(ByteBuffer.wrap(payload.getBytes(Charsets.UTF_8)));
    return rs;
  }

  private List<PersistedRuntimeStats> decode(RuntimeStat rs) throws IOException {
    List<PersistedRuntimeStats> rsm = RuntimeStatsPersister.INSTANCE.decode(rs.getPayload(), List.class);
    return rsm;
  }

  public void destroy() {
    executor.shutdown();
  }

  static void logException(String msg, Exception e) {
    if (LOG.isDebugEnabled()) {
      LOG.debug(msg, e);
    } else {
      LOG.info(msg + ": " + e.getMessage());
    }
  }

}