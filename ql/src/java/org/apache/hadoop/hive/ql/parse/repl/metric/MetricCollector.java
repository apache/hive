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

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.hive.ql.parse.SemanticException;
import org.apache.hadoop.hive.ql.parse.repl.metric.event.ReplicationMetric;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;
import java.util.HashMap;
import java.util.LinkedList;

/**
 * MetricCollector.
 * In memory collection of metrics
 */
public final class MetricCollector {
  private static final Logger LOG = LoggerFactory.getLogger(MetricCollector.class);
  private final Map<Long, ReplicationMetric> metricMap = new HashMap<>();
  private long maxSize = 0;
  private boolean isInited = false;
  private static volatile MetricCollector instance;

  private MetricCollector(){
  }

  public static MetricCollector getInstance() {
    if (instance == null) {
      synchronized (MetricCollector.class) {
        if (instance == null) {
          instance = new MetricCollector();
        }
      }
    }
    return instance;
  }

  public synchronized MetricCollector init(HiveConf conf) {
    //Can initialize the cache only once with a value.
    if (!isInited) {
      maxSize = getMaxSize(conf);
      isInited = true;
    }
    return instance;
  }

  long getMaxSize(HiveConf conf) {
    return MetastoreConf.getLongVar(conf, MetastoreConf.ConfVars.REPL_METRICS_CACHE_MAXSIZE);
  }

  public synchronized void addMetric(ReplicationMetric replicationMetric) throws SemanticException {
    if (metricMap.size() >= maxSize) {
      throw new SemanticException("Metrics are not getting collected. ");
    } else {
      if (metricMap.size() > 0.8 * maxSize) { //soft limit
        LOG.warn("Metrics cache is more than 80 % full. Will start dropping metrics once full. ");
      }
      metricMap.put(replicationMetric.getScheduledExecutionId(), replicationMetric);
    }
  }

  public synchronized LinkedList<ReplicationMetric> getMetrics() {
    LinkedList<ReplicationMetric> metricList = new LinkedList<>();
    for (ReplicationMetric metric : metricMap.values()) {
      metricList.add(new ReplicationMetric(metric));
    }
    metricMap.clear();
    return metricList;
  }

  //For testing
  synchronized void deinit() {
    if (isInited) {
      isInited = false;
      metricMap.clear();
      resetInstance();
    }
  }

  private static void resetInstance() {
    instance = null;
  }
}
