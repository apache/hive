/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hive.llap.daemon.impl;

import org.apache.hadoop.metrics2.AbstractMetric;
import org.apache.hadoop.metrics2.MetricsCollector;
import org.apache.hadoop.metrics2.MetricsInfo;
import org.apache.hadoop.metrics2.MetricsRecordBuilder;
import org.apache.hadoop.metrics2.MetricsTag;

import java.util.Map;

public final class DumpingMetricsCollector implements MetricsCollector {
  private MetricsRecordBuilder mrb;

  public DumpingMetricsCollector(Map<String, Long> data) {
    mrb = new DumpingMetricsRecordBuilder(data);
  }

  @Override
  public MetricsRecordBuilder addRecord(String s) {
    return mrb;
  }

  @Override
  public MetricsRecordBuilder addRecord(MetricsInfo metricsInfo) {
    return mrb;
  }

  private final class DumpingMetricsRecordBuilder extends MetricsRecordBuilder {
    private Map<String, Long> data;

    private DumpingMetricsRecordBuilder(Map<String, Long> data) {
      this.data = data;
    }

    @Override
    public MetricsCollector parent() {
      throw new UnsupportedOperationException();
    }

    @Override
    public MetricsRecordBuilder tag(MetricsInfo metricsInfo, String s) {
      return this;
    }

    @Override
    public MetricsRecordBuilder add(MetricsTag metricsTag) {
      return this;
    }

    @Override
    public MetricsRecordBuilder add(AbstractMetric abstractMetric) {
      throw new UnsupportedOperationException();
    }

    @Override
    public MetricsRecordBuilder setContext(String s) {
      return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo metricsInfo, int i) {
      data.put(metricsInfo.name(), Long.valueOf(i));
      return this;
    }

    @Override
    public MetricsRecordBuilder addCounter(MetricsInfo metricsInfo, long l) {
      data.put(metricsInfo.name(), l);
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, int i) {
      data.put(metricsInfo.name(), Long.valueOf(i));
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, long l) {
      data.put(metricsInfo.name(), l);
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, float v) {
      data.put(metricsInfo.name(), Long.valueOf(Math.round(v)));
      return this;
    }

    @Override
    public MetricsRecordBuilder addGauge(MetricsInfo metricsInfo, double v) {
      data.put(metricsInfo.name(), Math.round(v));
      return this;
    }
  }
}
