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

package org.apache.hadoop.hive.llap.metrics;

import com.google.common.collect.Lists;
import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;
import org.apache.hadoop.metrics2.lib.Interns;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * Mock metrics collector for this test only.
 * This <code>MetricsCollector</code> implementation is used to get the actual
 * <code>MetricsSource</code> data, collected by the <code>
 * ReadWriteLockMetrics</code>.
 */
public class MockMetricsCollector implements MetricsCollector {
  private ArrayList<MockRecord> records = new ArrayList<>();

  /**
   * Single metrics record mock implementation.
   */
  public static class MockRecord {
    private final String recordLabel;                   ///< record tag/label
    private final Map<MetricsInfo, Number> metrics;     ///< metrics within record
    private String context;                             ///< collector context ID

    /**
     * @param label metrics record label.
     */
    public MockRecord(String label) {
      recordLabel = label;
      metrics = new HashMap<>();
    }

    /**
     * @return The record's tag/label.
     */
    public String getLabel() {
      return recordLabel;
    }

    /**
     * @return The context of the collector.
     */
    public String getContext() {
      return context;
    }

    /**
     * @return Map of identifier/metric value pairs.
     */
    public Map<MetricsInfo, Number> getMetrics() {
      return metrics;
    }
  }

  /**
   * Record builder mock implementation.
   */
  private class MockMetricsRecordBuilder extends MetricsRecordBuilder {
    private MockRecord target = null;   ///< the record that is populated
    private final List<MetricsTag> tags;

    /**
     * Used by outer class to provide a new <code>MetricsRecordBuilder</code>
     * for a single metrics record.
     *
     * @param t The record to build.
     */
    MockMetricsRecordBuilder(MockRecord t) {
      target = t;
      tags = Lists.newArrayList();
    }

    @Override
    public MetricsRecordBuilder add(MetricsTag arg0) {
      throw new AssertionError("Not implemented for test");
    }

    @Override
    public MetricsRecordBuilder add(AbstractMetric arg0) {
      throw new AssertionError("Not implemented for test");
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo arg0, int arg1) {
      target.getMetrics().put(arg0, arg1);
      return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo arg0, long arg1) {
      target.getMetrics().put(arg0, arg1);
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo arg0, int arg1) {
      target.getMetrics().put(arg0, arg1);
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo arg0, long arg1) {
      target.getMetrics().put(arg0, arg1);
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo arg0, float arg1) {
      throw new AssertionError("Not implemented for test");
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo arg0, double arg1) {
      target.getMetrics().put(arg0, arg1);
      return this;
    }

    @Override
    public MetricsCollector parent() {
      return MockMetricsCollector.this;
    }

    @Override
    public MetricsRecordBuilder setContext(String arg0) {
      target.context = arg0;
      return this;
    }

    @Override
    public MetricsRecordBuilder tag(MetricsInfo info, String value) {
      tags.add(Interns.tag(info, value));
      return this;
    }
  }

  @Override
  public MetricsRecordBuilder addRecord(String arg0) {
    MockRecord tr = new MockRecord(arg0);
    records.add(tr);
    return new MockMetricsRecordBuilder(tr);
  }

  @Override
  public MetricsRecordBuilder addRecord(MetricsInfo arg0) {
    MockRecord tr = new MockRecord(arg0.name());
    records.add(tr);
    return new MockMetricsRecordBuilder(tr);
  }

  /**
   * @return A list of all built metrics records.
   */
  public List<MockRecord> getRecords() {
    return records;
  }
}
