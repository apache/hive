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
package org.apache.hadoop.hive.common.metrics.metrics2;

import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.common.metrics.MetricsTestUtils;
import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.common.MetricsVariable;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

/**
 * Unit test for the RatioGauge implementation.
 */
public class TestMetricVariableRatioGauge {

  public static MetricRegistry metricRegistry;

  @Before
  public void before() throws Exception {
    Configuration conf = new Configuration();
    conf.set(MetastoreConf.ConfVars.METRICS_CLASS.getHiveName(), CodahaleMetrics.class.getCanonicalName());
    // disable json file writing
    conf.set(MetastoreConf.ConfVars.METRICS_JSON_FILE_INTERVAL.getHiveName(), "60000m");

    MetricsFactory.init(conf);
    metricRegistry = ((CodahaleMetrics) MetricsFactory.getInstance()).getMetricRegistry();
  }

  @After
  public void after() throws Exception {
    MetricsFactory.close();
  }

  @Test
  public void testRatioIsCalculated() throws Exception {
    NumericVariable num = new NumericVariable(10);
    NumericVariable ord = new NumericVariable(5);

    MetricsFactory.getInstance().addRatio("rat", num, ord);
    String json = ((CodahaleMetrics) MetricsFactory.getInstance()).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, "rat", 2d);
  }

  @Test
  public void testRatioIsCalculatedNonExact() throws Exception {
    NumericVariable num = new NumericVariable(20);
    NumericVariable ord = new NumericVariable(3);

    MetricsFactory.getInstance().addRatio("rat", num, ord);
    String json = ((CodahaleMetrics) MetricsFactory.getInstance()).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, "rat", 6.6666d, 1e-4);
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingNumeratorRatio() throws Exception {
    MetricsFactory.getInstance().addRatio("rat", null, new NumericVariable(5));
  }

  @Test(expected = IllegalArgumentException.class)
  public void testMissingDenominatorRatio() throws Exception {
    MetricsFactory.getInstance().addRatio("rat", new NumericVariable(5), null);
  }

  @Test
  public void testEmptyRatio() throws Exception {
    NumericVariable num = new NumericVariable(null);
    NumericVariable ord = new NumericVariable(null);

    MetricsFactory.getInstance().addRatio("rat", num, ord);
    String json = ((CodahaleMetrics) MetricsFactory.getInstance()).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, "rat", "NaN");
  }

  @Test
  public void testZeroRatio() throws Exception {
    NumericVariable num = new NumericVariable(10);
    NumericVariable ord = new NumericVariable(0);

    MetricsFactory.getInstance().addRatio("rat", num, ord);
    String json = ((CodahaleMetrics) MetricsFactory.getInstance()).dumpJson();
    MetricsTestUtils.verifyMetricsJson(json, MetricsTestUtils.GAUGE, "rat", "NaN");
  }

  private class NumericVariable implements MetricsVariable<Integer> {

    private final Integer value;

    public NumericVariable(Integer value) {
      this.value = value;
    }

    @Override
    public Integer getValue() {
      return value;
    }
  }
}
