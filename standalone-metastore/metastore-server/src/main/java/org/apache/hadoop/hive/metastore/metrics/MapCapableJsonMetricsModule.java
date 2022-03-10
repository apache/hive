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
package org.apache.hadoop.hive.metastore.metrics;

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Metric;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.Timer;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.core.Version;
import com.fasterxml.jackson.databind.SerializerProvider;
import com.fasterxml.jackson.databind.module.SimpleSerializers;
import com.fasterxml.jackson.databind.ser.std.StdSerializer;
import com.google.common.collect.ImmutableList;

import java.io.IOException;
import java.util.Iterator;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.TimeUnit;

public class MapCapableJsonMetricsModule extends MetricsModule {

  public MapCapableJsonMetricsModule(TimeUnit rateUnit, TimeUnit durationUnit, boolean showSamples) {
    super(rateUnit, durationUnit, showSamples, MetricFilter.ALL);
  }

  public void setupModule(SetupContext context) {
    super.setupModule(context);
    context.addSerializers(
        new SimpleSerializers(
            ImmutableList.of(
                new MetricRegistrySerializer(version(), MetricFilter.ALL),
                new MapMetricsBeanSerializer())));
  }

  private static class MetricRegistrySerializer extends StdSerializer<MetricRegistry> {
    private final Version version;
    private final MetricFilter filter;

    private MetricRegistrySerializer(Version version, MetricFilter filter) {
      super(MetricRegistry.class);
      this.version = version;
      this.filter = filter;
    }

    public void serialize(MetricRegistry registry, JsonGenerator json, SerializerProvider provider) throws IOException {
      json.writeStartObject();
      SortedMetrics metrics = new SortedMetrics(registry, filter);
      json.writeStringField("version", version.toString());
      json.writeObjectField("gauges", metrics.getGauges());
      json.writeObjectField("counters", metrics.getCounters());
      json.writeObjectField("histograms", metrics.getHistograms());
      json.writeObjectField("meters", metrics.getMeters());
      json.writeObjectField("timers", metrics.getTimers());
      json.writeObjectField("mbeans", metrics.getMaps());
      json.writeEndObject();
    }
  }

  private static class MapMetricsBeanSerializer extends StdSerializer<MapMetrics> {

    private MapMetricsBeanSerializer() {
      super(MapMetrics.class);
    }

    public void serialize(MapMetrics map, JsonGenerator json, SerializerProvider provider) throws IOException {
      json.writeStartObject();
      for (Map.Entry<String, Integer> entry : map.get().entrySet()) {
        json.writeObjectField(entry.getKey(), entry.getValue());
      }
      json.writeEndObject();
    }
  }

  private static class SortedMetrics {
    private final TreeMap<String, Metric> gauges = new TreeMap<>();
    private final TreeMap<String, Metric> counters = new TreeMap<>();
    private final TreeMap<String, Metric> histograms = new TreeMap<>();
    private final TreeMap<String, Metric> meters = new TreeMap<>();
    private final TreeMap<String, Metric> timers = new TreeMap<>();
    private final TreeMap<String, Metric> maps = new TreeMap<>();

    public SortedMetrics(MetricRegistry registry, MetricFilter filter) {
      Iterator<Map.Entry<String, Metric>> iterator = registry.getMetrics().entrySet().iterator();
      while (iterator.hasNext()) {
        Map.Entry<String, Metric> entry = iterator.next();
        String name = entry.getKey();
        Metric metric = entry.getValue();
        if(filter != null && !filter.matches(name, metric)) {
          continue;
        }
        if (metric instanceof Gauge) {
          gauges.put(name, metric);
        } else if (metric instanceof Counter) {
          counters.put(name, metric);
        } else if (metric instanceof Histogram) {
          histograms.put(name, metric);
        } else if (metric instanceof Timer) {
          timers.put(name, metric);
        } else if (metric instanceof MapMetrics) {
          maps.put(name, metric);
        }
      }
    }

    public TreeMap<String, Metric> getGauges() {
      return gauges;
    }

    public TreeMap<String, Metric> getCounters() {
      return counters;
    }

    public TreeMap<String, Metric> getHistograms() {
      return histograms;
    }

    public TreeMap<String, Metric> getMeters() {
      return meters;
    }

    public TreeMap<String, Metric> getTimers() {
      return timers;
    }

    public TreeMap<String, Metric> getMaps() {
      return maps;
    }
  }
}
