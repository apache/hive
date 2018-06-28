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

package org.apache.hadoop.hive.ql.stats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.StatsSetupConst;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.ql.plan.Statistics;
import org.apache.hadoop.hive.ql.plan.Statistics.State;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.Lists;
import com.google.common.util.concurrent.MoreExecutors;
import com.google.common.util.concurrent.ThreadFactoryBuilder;

public class BasicStats {

  private static final Logger LOG = LoggerFactory.getLogger(BasicStats.class.getName());

  public static class Factory {

    private final List<IStatsEnhancer> enhancers = new LinkedList<>();

    public Factory(IStatsEnhancer... enhancers) {
      this.enhancers.addAll(Arrays.asList(enhancers));
    }

    public void addEnhancer(IStatsEnhancer enhancer) {
      enhancers.add(enhancer);
    }

    public BasicStats build(Partish p) {
      BasicStats ret = new BasicStats(p);
      for (IStatsEnhancer enhancer : enhancers) {
        ret.apply(enhancer);
      }
      return ret;
    }

    public List<BasicStats> buildAll(HiveConf conf, Collection<Partish> parts) {
      LOG.info("Number of partishes : " + parts.size());

      final List<BasicStats> ret = new ArrayList<>(parts.size());
      if (parts.size() <= 1) {
        for (Partish partish : parts) {
          ret.add(build(partish));
        }
        return ret;
      }

      List<Future<BasicStats>> futures = new ArrayList<>();

      int threads = conf.getIntVar(ConfVars.METASTORE_FS_HANDLER_THREADS_COUNT);

      final ExecutorService pool;
      if (threads <= 1) {
        pool = MoreExecutors.sameThreadExecutor();
      } else {
        pool = Executors.newFixedThreadPool(threads, new ThreadFactoryBuilder().setDaemon(true).setNameFormat("Get-Partitions-Size-%d").build());
      }

      for (final Partish part : parts) {
        futures.add(pool.submit(new Callable<BasicStats>() {
          @Override
          public BasicStats call() throws Exception {
            return build(part);
          }
        }));
      }

      try {
        for (int i = 0; i < futures.size(); i++) {
          ret.add(i, futures.get(i).get());
        }
      } catch (InterruptedException | ExecutionException e) {
        LOG.warn("Exception in processing files ", e);
      } finally {
        pool.shutdownNow();
      }
      return ret;
    }
  }

  public static interface IStatsEnhancer {
    void apply(BasicStats stats);
  }

  public static class SetMinRowNumber implements IStatsEnhancer {

    @Override
    public void apply(BasicStats stats) {
      if (stats.getNumRows() == 0) {
        stats.setNumRows(1);
      }
    }
  }

  public static class SetMinRowNumber01 implements IStatsEnhancer {

    @Override
    public void apply(BasicStats stats) {
      if (stats.getNumRows() == 0 || stats.getNumRows() == -1) {
        stats.setNumRows(1);
      }
    }
  }

  public static class RowNumEstimator implements IStatsEnhancer {

    private long avgRowSize;

    public RowNumEstimator(long avgRowSize) {
      this.avgRowSize = avgRowSize;
      if (avgRowSize > 0) {
        if (LOG.isDebugEnabled()) {
          LOG.debug("Estimated average row size: " + avgRowSize);
        }
      }
    }

    @Override
    public void apply(BasicStats stats) {
      // FIXME: there were different logic for part/table; merge these logics later
      if (stats.partish.getPartition() == null) {
        if (stats.getNumRows() < 0 && avgRowSize > 0) {
          stats.setNumRows(stats.getDataSize() / avgRowSize);
        }
      } else {
        if (avgRowSize > 0) {
          long rc = stats.getNumRows();
          long s = stats.getDataSize();
          if (rc <= 0 && s > 0) {
            rc = s / avgRowSize;
            stats.setNumRows(rc);
          }

          if (s <= 0 && rc > 0) {
            s = StatsUtils.safeMult(rc, avgRowSize);
            stats.setDataSize(s);
          }
        }
      }
      if (stats.getNumRows() > 0) {
        // FIXME: this promotion process should be removed later
        if (State.PARTIAL.morePreciseThan(stats.state)) {
          stats.state = State.PARTIAL;
        }
      }
    }
  }

  public static class DataSizeEstimator implements IStatsEnhancer {

    private HiveConf conf;
    private float deserFactor;

    public DataSizeEstimator(HiveConf conf) {
      this.conf = conf;
      deserFactor = HiveConf.getFloatVar(conf, HiveConf.ConfVars.HIVE_STATS_DESERIALIZATION_FACTOR);
    }

    @Override
    public void apply(BasicStats stats) {
      long ds = stats.getRawDataSize();
      if (ds <= 0) {
        ds = stats.getTotalSize();

        // if data size is still 0 then get file size
        if (ds <= 0) {
          Path path = stats.partish.getPath();
          try {
            ds = getFileSizeForPath(path);
          } catch (IOException e) {
            ds = 0L;
          }
        }
        ds = (long) (ds * deserFactor);

        stats.setDataSize(ds);
      }

    }

    private long getFileSizeForPath(Path path) throws IOException {
      FileSystem fs = path.getFileSystem(conf);
      return fs.getContentSummary(path).getLength();
    }

  }

  private Partish partish;

  private long rowCount;
  private long totalSize;
  private long rawDataSize;

  private long currentNumRows;
  private long currentDataSize;
  private Statistics.State state;

  public BasicStats(Partish p) {
    partish = p;

    rowCount = parseLong(StatsSetupConst.ROW_COUNT);
    rawDataSize = parseLong(StatsSetupConst.RAW_DATA_SIZE);
    totalSize = parseLong(StatsSetupConst.TOTAL_SIZE);

    currentNumRows = rowCount;
    currentDataSize = rawDataSize;

    if (currentNumRows > 0) {
      state = State.COMPLETE;
    } else {
      state = State.NONE;
    }
  }


  public BasicStats(List<BasicStats> partStats) {
    partish = null;
    List<Long> nrIn = Lists.newArrayList();
    List<Long> dsIn = Lists.newArrayList();
    state = (partStats.size() == 0) ? State.COMPLETE : null;
    for (BasicStats ps : partStats) {
      nrIn.add(ps.getNumRows());
      dsIn.add(ps.getDataSize());

      if (state == null) {
        state = ps.getState();
      } else {
        state = state.merge(ps.getState());
      }
    }
    currentNumRows = StatsUtils.getSumIgnoreNegatives(nrIn);
    currentDataSize = StatsUtils.getSumIgnoreNegatives(dsIn);

  }

  public long getNumRows() {
    return currentNumRows;
  }

  public long getDataSize() {
    return currentDataSize;
  }

  public Statistics.State getState() {
    return state;
  }

  void apply(IStatsEnhancer estimator) {
    estimator.apply(this);
  }

  protected void setNumRows(long l) {
    currentNumRows = l;
  }

  protected void setDataSize(long ds) {
    currentDataSize = ds;
  }

  protected long getTotalSize() {
    return totalSize;
  }

  protected long getRawDataSize() {
    return rawDataSize;
  }

  private long parseLong(String fieldName) {
    Map<String, String> params = partish.getPartParameters();
    long result = -1;

    if (params != null) {
      try {
        result = Long.parseLong(params.get(fieldName));
      } catch (NumberFormatException e) {
        result = -1;
      }
    }
    return result;
  }

  public static BasicStats buildFrom(List<BasicStats> partStats) {
    return new BasicStats(partStats);
  }

  @Override
  public String toString() {
    return String.format("BasicStats: %d, %d %s", getNumRows(), getDataSize(), getState());
  }
}
