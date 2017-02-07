/**
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
package org.apache.hadoop.hive.ql.hooks;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.junit.Assert.assertThat;

import com.codahale.metrics.Counter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import org.apache.hadoop.fs.CommonConfigurationKeysPublic;
import org.apache.hadoop.hive.common.metrics.common.MetricsConstant;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.common.metrics.metrics2.MetricsReporting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.junit.Before;
import org.junit.Test;

public class TestMetricsQueryLifeTimeHook {

  private MetricsQueryLifeTimeHook hook;
  private QueryLifeTimeHookContext ctx;
  private MetricRegistry metricRegistry;

  @Before
  public void before() throws Exception {
    HiveConf conf = new HiveConf();

    conf.set(CommonConfigurationKeysPublic.FS_DEFAULT_NAME_KEY, "local");
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_CLASS, CodahaleMetrics.class.getCanonicalName());
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_REPORTER, MetricsReporting.JSON_FILE.name()
        + "," + MetricsReporting.JMX.name());
    conf.setVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_INTERVAL, "100000s");

    MetricsFactory.init(conf);
    metricRegistry = ((CodahaleMetrics) MetricsFactory.getInstance()).getMetricRegistry();

    hook = new MetricsQueryLifeTimeHook();
    ctx = new QueryLifeTimeHookContextImpl();
  }

  @Test
  public void testCompilationQueryMetric() {
    Timer timer = metricRegistry.getTimers().get(MetricsConstant.HS2_COMPILING_QUERIES);
    Counter counter = metricRegistry.getCounters()
        .get(MetricsConstant.ACTIVE_CALLS + MetricsConstant.HS2_COMPILING_QUERIES);
    assertThat(timer, nullValue());
    assertThat(counter, nullValue());

    hook.beforeCompile(ctx);
    timer = metricRegistry.getTimers().get(MetricsConstant.HS2_COMPILING_QUERIES);
    counter = metricRegistry.getCounters()
        .get(MetricsConstant.ACTIVE_CALLS + MetricsConstant.HS2_COMPILING_QUERIES);
    assertThat(timer.getCount(), equalTo(0l));
    assertThat(counter.getCount(), equalTo(1l));

    hook.afterCompile(ctx, false);
    timer = metricRegistry.getTimers().get(MetricsConstant.HS2_COMPILING_QUERIES);
    counter = metricRegistry.getCounters()
        .get(MetricsConstant.ACTIVE_CALLS + MetricsConstant.HS2_COMPILING_QUERIES);
    assertThat(timer.getCount(), equalTo(1l));
    assertThat(counter.getCount(), equalTo(0l));
  }

  @Test
  public void testExecutionQueryMetric() {
    Timer timer = metricRegistry.getTimers().get(MetricsConstant.HS2_EXECUTING_QUERIES);
    Counter counter = metricRegistry.getCounters()
        .get(MetricsConstant.ACTIVE_CALLS + MetricsConstant.HS2_EXECUTING_QUERIES);
    assertThat(timer, nullValue());
    assertThat(counter, nullValue());

    hook.beforeExecution(ctx);
    timer = metricRegistry.getTimers().get(MetricsConstant.HS2_EXECUTING_QUERIES);
    counter = metricRegistry.getCounters()
        .get(MetricsConstant.ACTIVE_CALLS + MetricsConstant.HS2_EXECUTING_QUERIES);
    assertThat(timer.getCount(), equalTo(0l));
    assertThat(counter.getCount(), equalTo(1l));

    hook.afterExecution(ctx, false);
    timer = metricRegistry.getTimers().get(MetricsConstant.HS2_EXECUTING_QUERIES);
    counter = metricRegistry.getCounters()
        .get(MetricsConstant.ACTIVE_CALLS + MetricsConstant.HS2_EXECUTING_QUERIES);
    assertThat(timer.getCount(), equalTo(1l));
    assertThat(counter.getCount(), equalTo(0l));
  }

  @Test
  public void testNoErrorOnDisabledMetrics() throws Exception {
    MetricsFactory.close();
    MetricsQueryLifeTimeHook emptyhook = new MetricsQueryLifeTimeHook();

    assertThat(MetricsFactory.getInstance(), nullValue());

    emptyhook.beforeCompile(ctx);
    emptyhook.afterCompile(ctx, false);
    emptyhook.beforeExecution(ctx);
    emptyhook.afterExecution(ctx, false);
  }
}
