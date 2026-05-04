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

package org.apache.iceberg.rest.metrics;

import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.metrics.MetricsReport;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.time.Instant;

/**
 * A metrics reporter that logs events.
 */
public class LoggingMetricsReporter implements IcebergMetricsReporter {
  private static final Logger LOG = LoggerFactory.getLogger(LoggingMetricsReporter.class);

  public LoggingMetricsReporter(Configuration conf) {
  }

  @Override
  public void report(String catalog, TableIdentifier identifier, MetricsReport report, Instant receivedAt) {
    LOG.info("Event reported at {}: catalog={}, table={}, report={}", receivedAt, catalog, identifier, report);
  }

  @Override
  public void close() {
    LOG.info("Closing {}", getClass().getName());
  }
}
