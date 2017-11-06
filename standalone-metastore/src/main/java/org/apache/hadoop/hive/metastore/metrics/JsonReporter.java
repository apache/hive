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
import org.apache.hadoop.hive.metastore.conf.MetastoreConf;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.FileAttribute;
import java.nio.file.attribute.PosixFilePermission;
import java.nio.file.attribute.PosixFilePermissions;
import java.util.Set;
import java.util.SortedMap;
import java.util.concurrent.TimeUnit;

/**
 * A metrics reporter for Metrics that dumps metrics periodically into
 * a file in JSON format.
 */
public class JsonReporter extends ScheduledReporter {
  //
  // Implementation notes.
  //
  // 1. Since only local file systems are supported, there is no need to use Hadoop
  //    version of Path class.
  // 2. java.nio package provides modern implementation of file and directory operations
  //    which is better then the traditional java.io, so we are using it here.
  //    In particular, it supports atomic creation of temporary files with specified
  //    permissions in the specified directory. This also avoids various attacks possible
  //    when temp file name is generated first, followed by file creation.
  //    See http://www.oracle.com/technetwork/articles/javase/nio-139333.html for
  //    the description of NIO API and
  //    http://docs.oracle.com/javase/tutorial/essential/io/legacy.html for the
  //    description of interoperability between legacy IO api vs NIO API.
  // 3. To avoid race conditions with readers of the metrics file, the implementation
  //    dumps metrics to a temporary file in the same directory as the actual metrics
  //    file and then renames it to the destination. Since both are located on the same
  //    filesystem, this rename is likely to be atomic (as long as the underlying OS
  //    support atomic renames.
  //
  // NOTE: This reporter is very similar to
  //       org.apache.hadoop.hive.common.metrics.metrics2.JsonFileMetricsReporter.
  //       org.apache.hadoop.hive.metastore.metrics.JsonReporter.
  //       It would be good to unify the two.
  //
  private static final Logger LOG = LoggerFactory.getLogger(JsonReporter.class);

  private static final FileAttribute<Set<PosixFilePermission>> FILE_ATTRS =
          PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rw-r--r--"));
  // Permissions for metric directory
  private static final FileAttribute<Set<PosixFilePermission>> DIR_ATTRS =
      PosixFilePermissions.asFileAttribute(PosixFilePermissions.fromString("rwxr-xr-x"));

  private final MetricRegistry registry;
  private ObjectWriter jsonWriter;
  // Location of JSON file
  private final Path path;
  // Directory where path resides
  private final Path metricsDir;

  private JsonReporter(MetricRegistry registry, String name, MetricFilter filter,
                       TimeUnit rateUnit, TimeUnit durationUnit, Configuration conf) {
    super(registry, name, filter, rateUnit, durationUnit);
    String pathString = MetastoreConf.getVar(conf, MetastoreConf.ConfVars .METRICS_JSON_FILE_LOCATION);
    path = Paths.get(pathString).toAbsolutePath();
    LOG.info("Reporting metrics to {}", path);
    // We want to use metricsDir in the same directory as the destination file to support atomic
    // move of temp file to the destination metrics file
    metricsDir = path.getParent();
    this.registry = registry;
  }

  @Override
  public void start(long period, TimeUnit unit) {
    // Create metrics directory if it is not present
    if (!metricsDir.toFile().exists()) {
      LOG.warn("Metrics directory {} does not exist, creating one", metricsDir);
      try {
        // createDirectories creates all non-existent parent directories
        Files.createDirectories(metricsDir, DIR_ATTRS);
      } catch (IOException e) {
        LOG.warn("Failed to initialize JSON reporter: failed to create directory {}: {}", metricsDir, e.getMessage());
        return;
      }
    }
    jsonWriter = new ObjectMapper().registerModule(new MetricsModule(TimeUnit.MILLISECONDS,
        TimeUnit.MILLISECONDS, false)).writerWithDefaultPrettyPrinter();
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

    // Metrics are first dumped to a temp file which is then renamed to the destination
    Path tmpFile = null;
    try {
      tmpFile = Files.createTempFile(metricsDir, "hmsmetrics", "json", FILE_ATTRS);
    } catch (IOException e) {
      LOG.error("failed to create temp file for JSON metrics", e);
      return;
    } catch (SecurityException e) {
      // This shouldn't ever happen
      LOG.error("failed to create temp file for JSON metrics: no permissions", e);
      return;
    } catch (UnsupportedOperationException e) {
      // This shouldn't ever happen
      LOG.error("failed to create temp file for JSON metrics: operartion not supported", e);
      return;
    }

    // Use try .. finally to cleanup temp file if something goes wrong
    try {
      // Write json to the temp file
      try (BufferedWriter bw = new BufferedWriter(new FileWriter(tmpFile.toFile()))) {
        bw.write(json);
      } catch (IOException e) {
        LOG.error("Unable to write to temp file {}" + tmpFile, e);
        return;
      }

      // Atomically move temp file to the destination file
      try {
        Files.move(tmpFile, path, StandardCopyOption.ATOMIC_MOVE);
      } catch (Exception e) {
        LOG.error("Unable to rename temp file {} to {}", tmpFile, path);
        LOG.error("Exception during rename", e);
      }
    } finally {
      // If something happened and we were not able to rename the temp file, attempt to remove it
      if (tmpFile.toFile().exists()) {
        // Attempt to delete temp file, if this fails, not much can be done about it.
        try {
          Files.delete(tmpFile);
        } catch (Exception e) {
          LOG.error("failed to delete temporary metrics file " + tmpFile, e);
        }
      }
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
