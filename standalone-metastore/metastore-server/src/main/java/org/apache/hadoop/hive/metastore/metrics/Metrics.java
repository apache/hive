/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.metastore.metrics;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.JmxReporter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Reporter;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.jvm.BufferPoolMetricSet;
import com.codahale.metrics.jvm.ClassLoadingGaugeSet;
import com.codahale.metrics.jvm.GarbageCollectorMetricSet;
import com.codahale.metrics.jvm.MemoryUsageGaugeSet;
import com.codahale.metrics.jvm.ThreadStatesGaugeSet;
import com.github.joshelser.dropwizard.metrics.hadoop.HadoopMetrics2Reporter;
import com.google.common.annotations.VisibleForTesting;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.lang.management.ManagementFactory;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class Metrics {
  private static final Logger LOGGER = LoggerFactory.getLogger(Metrics.class);

  private static Metrics self;
  private static final AtomicInteger singletonAtomicInteger = new AtomicInteger();

  private final MetricRegistry registry;
  private List<Reporter> reporters;
  private List<ScheduledReporter> scheduledReporters;
  private Map<String, AtomicInteger> gaugeAtomics;
  private boolean hadoopMetricsStarted;

  public static synchronized Metrics initialize(Configuration conf) {
    if (self == null) {
      self = new Metrics(conf);
    }
    return self;
  }

  public static MetricRegistry getRegistry() {
    if (self == null) return null;
    return self.registry;
  }

  public static void shutdown() {
    if (self != null) {
      for (ScheduledReporter reporter : self.scheduledReporters) {
        reporter.stop();
        reporter.close();
      }
      if (self.hadoopMetricsStarted) DefaultMetricsSystem.shutdown();
      self = null;
    }
  }

  /**
   * Get an existing counter or create a new one if the requested one does not yet exist.  Creation
   * is synchronized to assure that only one instance of the counter is created.
   * @param name name of the counter
   * @return new Counter, or existing one if it already exists.  This will return null if the
   * metrics have not been initialized.
   */
  public static Counter getOrCreateCounter(String name) {
    if (self == null) return null;
    Map<String, Counter> counters = self.registry.getCounters();
    Counter counter = counters.get(name);
    if (counter != null) return counter;
    // Looks like it doesn't exist.  Lock so that two threads don't create it at once.
    synchronized (Metrics.class) {
      // Recheck to make sure someone didn't create it while we waited.
      counters = self.registry.getCounters();
      counter = counters.get(name);
      if (counter != null) return counter;
      return self.registry.counter(name);
    }
  }

  /**
   * Get an existing timer or create a new one if the requested one does not yet exist.  Creation
   * is synchronized to assure that only one instance of the counter is created.
   * @param name timer name
   * @return new Timer, or existing one if it already exists, null if the metrics have not been
   * initialized.
   */
  public static Timer getOrCreateTimer(String name) {
    if (self == null) return null;
    Map<String, Timer> timers = self.registry.getTimers();
    Timer timer = timers.get(name);
    if (timer != null) return timer;
    synchronized (Metrics.class) {
      timers = self.registry.getTimers();
      timer = timers.get(name);
      if (timer != null) return timer;
      return self.registry.timer(name);
    }
  }

  /**
   * Get the AtomicInteger behind an existing gauge, or create a new gauge if it does not already
   * exist.
   * @param name Name of gauge.  This should come from MetricConstants
   * @return AtomicInteger underlying this gauge.
   */
  public static AtomicInteger getOrCreateGauge(String name) {
    // We return a garbage value if metrics haven't been initialized so that callers don't have
    // to keep checking if the resulting value is null.
    if (self == null) return singletonAtomicInteger;
    AtomicInteger ai = self.gaugeAtomics.get(name);
    if (ai != null) return ai;
    synchronized (Metrics.class) {
      ai = self.gaugeAtomics.get(name);
      if (ai != null) return ai;
      ai = new AtomicInteger();
      final AtomicInteger forGauge = ai;
      self.gaugeAtomics.put(name, ai);
      self.registry.register(name, new Gauge<Integer>() {
        @Override
        public Integer getValue() {
          return forGauge.get();
        }
      });
      return ai;
    }
  }

  @VisibleForTesting
  static List<Reporter> getReporters() {
    return self.reporters;
  }

  private Metrics(Configuration conf) {
    registry = new MetricRegistry();

    registry.registerAll(new GarbageCollectorMetricSet());
    registry.registerAll(new BufferPoolMetricSet(ManagementFactory.getPlatformMBeanServer()));
    registry.registerAll(new MemoryUsageGaugeSet());
    registry.registerAll(new ThreadStatesGaugeSet());
    registry.registerAll(new ClassLoadingGaugeSet());

    /*
     * This is little complicated.  First we look for our own config values on this.  If those
     * aren't set we use the Hive ones.  But Hive also has multiple ways to do this, so we need to
     * look in both of theirs as well.  We can't use theirs directly because they wrap the
     * codahale reporters in their own and we do not.
     */
    // Check our config value first.  I'm explicitly avoiding getting the default value for now,
    // as I don't want our default to override a Hive set value.
    String reportersToStart = conf.get(MetastoreConf.ConfVars.METRICS_REPORTERS.getVarname());
    if (reportersToStart == null) {
      // Now look in the current Hive config value.  Again, avoiding getting defaults
      reportersToStart =
          conf.get(MetastoreConf.ConfVars.HIVE_CODAHALE_METRICS_REPORTER_CLASSES.getHiveName());
      if (reportersToStart == null) {
        // Last chance, look in the old Hive config value.  Still avoiding defaults.
        reportersToStart =
            conf.get(MetastoreConf.ConfVars.HIVE_METRICS_REPORTER.getHiveName());
        if (reportersToStart == null) {
          // Alright fine, we'll use our defaults
          reportersToStart = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.METRICS_REPORTERS);
        }
      }
    }

    reporters = new ArrayList<>();
    scheduledReporters = new ArrayList<>();
    if (reportersToStart != null && reportersToStart.length() > 0) {
      String[] reporterNames = reportersToStart.toLowerCase().split(",");
      for (String reporterName : reporterNames) {
        if (reporterName.equals("console") || reporterName.endsWith("consolemetricsreporter")) {
          ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .build();
          reporter.start(15, TimeUnit.SECONDS);
          reporters.add(reporter);
          scheduledReporters.add(reporter);
        } else if (reporterName.equals("jmx") || reporterName.endsWith("jmxmetricsreporter")) {
          JmxReporter reporter = JmxReporter.forRegistry(registry)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .build();
          reporter.start();
          reporters.add(reporter);
        } else if (reporterName.startsWith("json") || reporterName.endsWith("jsonfilemetricsreporter")) {
          // We have to initialize the thread pool before we start this one, as it uses it
          JsonReporter reporter = JsonReporter.forRegistry(registry, conf)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .build();
          reporter.start(MetastoreConf.getTimeVar(conf,
              MetastoreConf.ConfVars.METRICS_JSON_FILE_INTERVAL, TimeUnit.SECONDS), TimeUnit.SECONDS);
          reporters.add(reporter);
          scheduledReporters.add(reporter);
        } else if (reporterName.startsWith("hadoop") || reporterName.endsWith("metrics2reporter")) {
          String applicationName = MetastoreConf.getVar(conf, MetastoreConf.ConfVars.METRICS_HADOOP2_COMPONENT_NAME);
          HadoopMetrics2Reporter reporter = HadoopMetrics2Reporter.forRegistry(registry)
              .convertRatesTo(TimeUnit.SECONDS)
              .convertDurationsTo(TimeUnit.MILLISECONDS)
              .build(DefaultMetricsSystem.initialize(applicationName), applicationName, "Runtime metadata" +
                  " catalog", "General");
          reporter.start(1, TimeUnit.MINUTES);
          reporters.add(reporter);
          scheduledReporters.add(reporter);
          hadoopMetricsStarted = true;
        } else {
          throw new RuntimeException("Unknown metric type " + reporterName);
        }
      }
    } else {
      LOGGER.warn("No metrics reporters configured.");
    }

    // Create map for tracking gauges
    gaugeAtomics = new HashMap<>();
  }
}
