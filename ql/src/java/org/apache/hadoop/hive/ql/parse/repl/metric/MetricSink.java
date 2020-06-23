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
package org.apache.hadoop.hive.ql.parse.repl.metric;

import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.api.ReplicationMetricList;
import org.apache.hadoop.hive.metastore.api.ReplicationMetrics;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.metastore.utils.Retry;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.ReplicationMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * MetricSink.
 * Scheduled thread to poll from Metric Collector and persists to DB
 */
public class MetricSink {
  private static final Logger LOG = LoggerFactory.getLogger(MetricSink.class);
  private ScheduledExecutorService executorService;
  private static volatile MetricSink instance;
  private boolean isInitialised = false;
  private HiveConf conf;

  private MetricSink() {
    this.executorService = Executors.newSingleThreadScheduledExecutor();
  }

  public static MetricSink getInstance() {
    if (instance == null) {
      synchronized (MetricSink.class) {
        if (instance == null) {
          instance = new MetricSink();
        }
      }
    }
    return instance;
  }

  public synchronized void init(HiveConf conf) {
    if (!isInitialised) {
      this.conf = conf;
      this.executorService.schedule(new MetricSinkWriter(conf), getFrequencyInSecs(), TimeUnit.SECONDS);
      isInitialised = true;
    }
  }

  long getFrequencyInSecs() {
    //Metastore conf is in minutes
    return MetastoreConf.getLongVar(conf, MetastoreConf.ConfVars.REPL_METRICS_UPDATE_FREQUENCY) * 60;
  }

  public synchronized void tearDown() {
    if (isInitialised) {
      try {
        this.executorService.shutdown();
      } finally {
        if (!this.executorService.isShutdown()) {
          this.executorService.shutdownNow();
        }
      }
      isInitialised = false;
    }
  }

  static class MetricSinkWriter implements Runnable {
    private MetricCollector collector;
    private HiveConf conf;

    // writer instance

    MetricSinkWriter(HiveConf conf) {
      this.collector = MetricCollector.getInstance();
      this.conf = conf;
    }

    @Override
    public void run() {
      ReplicationMetricList metricList = new ReplicationMetricList();
      try {
        // get metrics
        LinkedList<ReplicationMetric> metrics = collector.getMetrics();
        //Move metrics to thrift list
        if (metrics.size() > 0) {
          int totalMetricsSize = metrics.size();
          List<ReplicationMetrics> replicationMetricsList = new ArrayList<>(totalMetricsSize);
          for (int index = 0; index < totalMetricsSize; index++) {
            ReplicationMetric metric = metrics.removeFirst();
            ReplicationMetrics persistentMetric = new ReplicationMetrics();
            persistentMetric.setDumpExecutionId(metric.getDumpExecutionId());
            persistentMetric.setScheduledExecutionId(metric.getScheduledExecutionId());
            persistentMetric.setPolicy(metric.getPolicy());
            ObjectMapper mapper = new ObjectMapper();
            persistentMetric.setProgress(mapper.writeValueAsString(metric.getProgress()));
            persistentMetric.setMetadata(mapper.writeValueAsString(metric.getMetadata()));
            replicationMetricsList.add(persistentMetric);
          }
          metricList.setReplicationMetricList(replicationMetricsList);
        }
      } catch (Exception e) {
        throw new RuntimeException("Metrics are not getting persisted", e);
      }
      // write metrics and retry if fails
      Retry<Void> retriable = new Retry<Void>(Exception.class) {
        @Override
        public Void execute() throws Exception {
            //write
          if (metricList.getReplicationMetricListSize() > 0) {
            Hive.get(conf).getMSC().addReplicationMetrics(metricList);
          }
          return null;
        }
      };
      try {
        retriable.run();
      } catch (Exception e) {
        throw new RuntimeException("Metrics are not getting persisted to HMS", e);
      }
    }
  }
}
