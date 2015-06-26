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

package org.apache.hadoop.hive.common.metrics.metrics2;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.ExponentiallyDecayingReservoir;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.MetricSet;
import com.codahale.metrics.Timer;
import com.codahale.metrics.json.MetricsModule;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Splitter;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.collect.Lists;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Closeable;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.management.ManagementFactory;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TimerTask;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * Codahale-backed Metrics implementation.
 */
public class CodahaleMetrics implements org.apache.hadoop.hive.common.metrics.common.Metrics {
  public static final String API_PREFIX = "api_";
  public static final Log LOGGER = LogFactory.getLog(CodahaleMetrics.class);

  public final MetricRegistry metricRegistry = new MetricRegistry();
  private final Lock timersLock = new ReentrantLock();
  private final Lock countersLock = new ReentrantLock();

  private LoadingCache<String, Timer> timers;
  private LoadingCache<String, Counter> counters;

  private HiveConf conf;
  private final Set<Closeable> reporters = new HashSet<Closeable>();

  private final ThreadLocal<HashMap<String, MetricsScope>> threadLocalScopes
    = new ThreadLocal<HashMap<String,MetricsScope>>() {
    @Override
    protected HashMap<String,MetricsScope> initialValue() {
      return new HashMap<String,MetricsScope>();
    }
  };

  public static class MetricsScope {

    final String name;
    final Timer timer;
    Timer.Context timerContext;
    CodahaleMetrics metrics;

    private boolean isOpen = false;

    /**
     * Instantiates a named scope - intended to only be called by Metrics, so locally scoped.
     * @param name - name of the variable
     * @throws IOException
     */
    private MetricsScope(String name, CodahaleMetrics metrics) throws IOException {
      this.name = name;
      this.metrics = metrics;
      this.timer = metrics.getTimer(name);
      open();
    }

    /**
     * Opens scope, and makes note of the time started, increments run counter
     * @throws IOException
     *
     */
    public void open() throws IOException {
      if (!isOpen) {
        isOpen = true;
        this.timerContext = timer.time();
      } else {
        throw new IOException("Scope named " + name + " is not closed, cannot be opened.");
      }
    }

    /**
     * Closes scope, and records the time taken
     * @throws IOException
     */
    public void close() throws IOException {
      if (isOpen) {
        timerContext.close();

      } else {
        throw new IOException("Scope named " + name + " is not open, cannot be closed.");
      }
      isOpen = false;
    }
  }

  public CodahaleMetrics(HiveConf conf) throws Exception {
    this.conf = conf;
    //Codahale artifacts are lazily-created.
    timers = CacheBuilder.newBuilder().build(
      new CacheLoader<String, com.codahale.metrics.Timer>() {
        @Override
        public com.codahale.metrics.Timer load(String key) throws Exception {
          Timer timer = new Timer(new ExponentiallyDecayingReservoir());
          metricRegistry.register(key, timer);
          return timer;
        }
      }
    );
    counters = CacheBuilder.newBuilder().build(
      new CacheLoader<String, Counter>() {
        @Override
        public Counter load(String key) throws Exception {
          Counter counter = new Counter();
          metricRegistry.register(key, counter);
          return counter;
        }
      }
    );

    //register JVM metrics
    registerAll("gc", new GarbageCollectorMetricSet());
    registerAll("buffers", new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
    registerAll("memory", new MemoryUsageGaugeSet());
    registerAll("threads", new ThreadStatesGaugeSet());
    registerAll("classLoading", new ClassLoadingGaugeSet());

    //Metrics reporter
    Set<MetricsReporting> finalReporterList = new HashSet<MetricsReporting>();
    List<String> metricsReporterNames = Lists.newArrayList(
      Splitter.on(",").trimResults().omitEmptyStrings().split(conf.getVar(HiveConf.ConfVars.HIVE_METRICS_REPORTER)));

    if(metricsReporterNames != null) {
      for (String metricsReportingName : metricsReporterNames) {
        try {
          MetricsReporting reporter = MetricsReporting.valueOf(metricsReportingName.trim().toUpperCase());
          finalReporterList.add(reporter);
        } catch (IllegalArgumentException e) {
          LOGGER.warn("Metrics reporter skipped due to invalid configured reporter: " + metricsReportingName);
        }
      }
    }
    initReporting(finalReporterList);
  }


  public void close() throws Exception {
    if (reporters != null) {
      for (Closeable reporter : reporters) {
        reporter.close();
      }
    }
    for (Map.Entry<String, Metric> metric : metricRegistry.getMetrics().entrySet()) {
      metricRegistry.remove(metric.getKey());
    }
    timers.invalidateAll();
    counters.invalidateAll();
  }

  public void startScope(String name) throws IOException {
    name = API_PREFIX + name;
    if (threadLocalScopes.get().containsKey(name)) {
      threadLocalScopes.get().get(name).open();
    } else {
      threadLocalScopes.get().put(name, new MetricsScope(name, this));
    }
  }

  public void endScope(String name) throws IOException {
    name = API_PREFIX + name;
    if (threadLocalScopes.get().containsKey(name)) {
      threadLocalScopes.get().get(name).close();
    }
  }

  public Long incrementCounter(String name) throws IOException {
    return incrementCounter(name, 1);
  }

  public Long incrementCounter(String name, long increment) throws IOException {
    String key = name;
    try {
      countersLock.lock();
      counters.get(key).inc(increment);
      return counters.get(key).getCount();
    } catch(ExecutionException ee) {
      throw new RuntimeException(ee);
    } finally {
      countersLock.unlock();
    }
  }

  // This method is necessary to synchronize lazy-creation to the timers.
  private Timer getTimer(String name) throws IOException {
    String key = name;
    try {
      timersLock.lock();
      Timer timer = timers.get(key);
      return timer;
    } catch (ExecutionException e) {
      throw new IOException(e);
    } finally {
      timersLock.unlock();
    }
  }

  private void registerAll(String prefix, MetricSet metricSet) {
    for (Map.Entry<String, Metric> entry : metricSet.getMetrics().entrySet()) {
      if (entry.getValue() instanceof MetricSet) {
        registerAll(prefix + "." + entry.getKey(), (MetricSet) entry.getValue());
      } else {
        metricRegistry.register(prefix + "." + entry.getKey(), entry.getValue());
      }
    }
  }

  @VisibleForTesting
  public MetricRegistry getMetricRegistry() {
    return metricRegistry;
  }

  /**
   * Should be only called once to initialize the reporters
   */
  private void initReporting(Set<MetricsReporting> reportingSet) throws Exception {
    for (MetricsReporting reporting : reportingSet) {
      switch(reporting) {
        case CONSOLE:
          final ConsoleReporter consoleReporter = ConsoleReporter.forRegistry(metricRegistry)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
          consoleReporter.start(1, TimeUnit.SECONDS);
          reporters.add(consoleReporter);
          break;
        case JMX:
          final JmxReporter jmxReporter = JmxReporter.forRegistry(metricRegistry)
            .convertRatesTo(TimeUnit.SECONDS)
            .convertDurationsTo(TimeUnit.MILLISECONDS)
            .build();
          jmxReporter.start();
          reporters.add(jmxReporter);
          break;
        case JSON_FILE:
          final JsonFileReporter jsonFileReporter = new JsonFileReporter();
          jsonFileReporter.start();
          reporters.add(jsonFileReporter);
          break;
      }
    }
  }

  class JsonFileReporter implements Closeable {
    private ObjectMapper jsonMapper = null;
    private java.util.Timer timer = null;

    public void start() {
      this.jsonMapper = new ObjectMapper().registerModule(new MetricsModule(TimeUnit.MILLISECONDS, TimeUnit.MILLISECONDS, false));
      this.timer = new java.util.Timer(true);

      long time = conf.getTimeVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_INTERVAL, TimeUnit.MILLISECONDS);
      final String pathString = conf.getVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_LOCATION);

      timer.schedule(new TimerTask() {
        @Override
        public void run() {
          BufferedWriter bw = null;
          try {
            String json = jsonMapper.writerWithDefaultPrettyPrinter().writeValueAsString(metricRegistry);
            Path tmpPath = new Path(pathString + ".tmp");
            FileSystem fs = FileSystem.get(conf);
            fs.delete(tmpPath, true);
            bw = new BufferedWriter(new OutputStreamWriter(fs.create(tmpPath, true)));
            bw.write(json);
            bw.close();

            Path path = new Path(pathString);
            fs.rename(tmpPath, path);
            fs.setPermission(path, FsPermission.createImmutable((short) 0644));
          } catch (Exception e) {
            LOGGER.warn("Error writing JSON Metrics to file", e);
          } finally {
            try {
              if (bw != null) {
                bw.close();
              }
            } catch (IOException e) {
              //Ignore.
            }
          }


        }
      }, 0, time);
    }

    public void close() {
      if (timer != null) {
        this.timer.cancel();
      }
    }
  }
}
