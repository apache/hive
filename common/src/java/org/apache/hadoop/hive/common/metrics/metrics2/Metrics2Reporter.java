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

import com.codahale.metrics.MetricRegistry;
import com.github.joshelser.dropwizard.metrics.hadoop.HadoopMetrics2Reporter;
import java.io.Closeable;
import java.util.concurrent.TimeUnit;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.metrics2.lib.DefaultMetricsSystem;
import com.codahale.metrics.Reporter;

/**
 * A wrapper around Codahale HadoopMetrics2Reporter to make it a pluggable/configurable Hive Metrics reporter.
 */
public class Metrics2Reporter implements CodahaleReporter {

  private final MetricRegistry metricRegistry;
  private final HiveConf conf;
  private final HadoopMetrics2Reporter reporter;

  public Metrics2Reporter(MetricRegistry registry, HiveConf conf) {
    this.metricRegistry = registry;
    this.conf = conf;
    String applicationName = conf.get(HiveConf.ConfVars.HIVE_METRICS_HADOOP2_COMPONENT_NAME.varname);

    reporter = HadoopMetrics2Reporter.forRegistry(metricRegistry)
        .convertRatesTo(TimeUnit.SECONDS)
        .convertDurationsTo(TimeUnit.MILLISECONDS)
        .build(DefaultMetricsSystem.initialize(applicationName), // The application-level name
            applicationName, // Component name
            applicationName, // Component description
            "General"); // Name for each metric record
  }

  @Override
  public void start() {
    long reportingInterval =
        HiveConf.toTime(conf.get(HiveConf.ConfVars.HIVE_METRICS_HADOOP2_INTERVAL.varname), TimeUnit.SECONDS, TimeUnit.SECONDS);
    reporter.start(reportingInterval, TimeUnit.SECONDS);
  }

  @Override
  public void close() {
    reporter.close();
  }
}
