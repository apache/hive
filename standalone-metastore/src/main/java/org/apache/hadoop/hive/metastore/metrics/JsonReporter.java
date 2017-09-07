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

import com.codahale.metrics.Counter;
import com.codahale.metrics.Gauge;
import com.codahale.metrics.Histogram;
import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricFilter;
import com.codahale.metrics.MetricRegistry;
import com.codahale.metrics.ScheduledReporter;
import com.codahale.metrics.Timer;
import com.codahale.metrics.json.MetricsModule;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectWriter;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

public class JsonReporter extends ScheduledReporter {
  private static final Logger LOG = LoggerFactory.getLogger(JsonReporter.class);

  private final Configuration conf;
  private final MetricRegistry registry;
  private ObjectWriter jsonWriter;
  private Path path;
  private Path tmpPath;
  private FileSystem fs;

  private JsonReporter(MetricRegistry registry, String name, MetricFilter filter,
                       TimeUnit rateUnit, TimeUnit durationUnit, Configuration conf) {
    super(registry, name, filter, rateUnit, durationUnit);
    this.conf = conf;
    this.registry = registry;
  }

  @Override
  public void start(long period, TimeUnit unit) {
    jsonWriter = new ObjectMapper().registerModule(new MetricsModule(TimeUnit.MILLISECONDS,
        TimeUnit.MILLISECONDS, false)).writerWithDefaultPrettyPrinter();
    String pathString = MetastoreConf.getVar(conf, MetastoreConf.ConfVars .METRICS_JSON_FILE_LOCATION);
    path = new Path(pathString);

    tmpPath = new Path(pathString + ".tmp");
    URI tmpPathURI = tmpPath.toUri();
    try {
      if (tmpPathURI.getScheme() == null && tmpPathURI.getAuthority() == null) {
        //default local
        fs = FileSystem.getLocal(conf);
      } else {
        fs = FileSystem.get(tmpPathURI, conf);
      }
    }
    catch (IOException e) {
      LOG.error("Unable to access filesystem for path " + tmpPath + ". Aborting reporting", e);
      return;
    }
    super.start(period, unit);
  }

  @Override
  public void report(SortedMap<String, Gauge> sortedMap, SortedMap<String, Counter> sortedMap1,
                     SortedMap<String, Histogram> sortedMap2, SortedMap<String, Meter> sortedMap3,
                     SortedMap<String, Timer> sortedMap4) {

    String json;
    try {
      json = jsonWriter.writeValueAsString(registry);
    } catch (JsonProcessingException e) {
      LOG.error("Unable to convert json to string ", e);
      return;
    }

    BufferedWriter bw = null;
    try {
      fs.delete(tmpPath, true);
      bw = new BufferedWriter(new OutputStreamWriter(fs.create(tmpPath, true)));
      bw.write(json);
      fs.setPermission(tmpPath, FsPermission.createImmutable((short) 0644));
    } catch (IOException e) {
      LOG.error("Unable to write to temp file " + tmpPath, e);
      return;
    } finally {
      if (bw != null) {
        try {
          bw.close();
        } catch (IOException e) {
          // Not much we can do
          LOG.error("Caught error closing json metric reporter file", e);
        }
      }
    }

    try {
      fs.rename(tmpPath, path);
      fs.setPermission(path, FsPermission.createImmutable((short) 0644));
    } catch (IOException e) {
      LOG.error("Unable to rename temp file " + tmpPath + " to " + path, e);
    }
  }

  public static Builder forRegistry(MetricRegistry registry, Configuration conf) {
    return new Builder(registry, conf);
  }

  public static class Builder {
    private final MetricRegistry registry;
    private final Configuration conf;
    private TimeUnit rate = TimeUnit.SECONDS;
    private TimeUnit duration = TimeUnit.MILLISECONDS;
    private MetricFilter filter = MetricFilter.ALL;

    private Builder(MetricRegistry registry, Configuration conf) {
      this.registry = registry;
      this.conf = conf;
    }

    public Builder convertRatesTo(TimeUnit rate) {
      this.rate = rate;
      return this;
    }

    public Builder convertDurationsTo(TimeUnit duration) {
      this.duration = duration;
      return this;
    }

    public Builder filter(MetricFilter filter) {
      this.filter = filter;
      return this;
    }

    public JsonReporter build() {
      return new JsonReporter(registry, "json-reporter", filter, rate, duration, conf);
    }

  }

}
