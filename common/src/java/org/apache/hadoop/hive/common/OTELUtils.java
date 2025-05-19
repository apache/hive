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
package org.apache.hadoop.hive.common;

import java.time.Duration;
import java.util.concurrent.TimeUnit;

import io.opentelemetry.api.OpenTelemetry;
import io.opentelemetry.api.common.AttributeKey;
import io.opentelemetry.api.common.Attributes;
import io.opentelemetry.exporter.otlp.metrics.OtlpGrpcMetricExporter;
import io.opentelemetry.exporter.otlp.trace.OtlpGrpcSpanExporter;
import io.opentelemetry.sdk.OpenTelemetrySdk;
import io.opentelemetry.sdk.common.export.RetryPolicy;
import io.opentelemetry.sdk.metrics.SdkMeterProvider;
import io.opentelemetry.sdk.metrics.export.MetricExporter;
import io.opentelemetry.sdk.metrics.export.PeriodicMetricReader;
import io.opentelemetry.sdk.resources.Resource;
import io.opentelemetry.sdk.trace.SdkTracerProvider;
import io.opentelemetry.sdk.trace.export.BatchSpanProcessor;
import io.opentelemetry.sdk.trace.export.SpanExporter;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;

import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_OTEL_COLLECTOR_ENDPOINT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_OTEL_EXPORTER_TIMEOUT;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_OTEL_RETRY_BACKOFF_MULTIPLIER;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_OTEL_RETRY_INITIAL_BACKOFF;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_OTEL_RETRY_MAX_BACKOFF;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.HIVE_OTEL_METRICS_FREQUENCY_SECONDS;

public class OTELUtils {

  private static OpenTelemetry otel =  null;
  public static OpenTelemetry getOpenTelemetry(Configuration conf) {
    if (otel == null || otel == OpenTelemetry.noop()) {

      String endPoint = HiveConf.getVar(conf, HIVE_OTEL_COLLECTOR_ENDPOINT);
      long otelExporterFrequency = HiveConf.getTimeVar(conf, HIVE_OTEL_METRICS_FREQUENCY_SECONDS, TimeUnit.SECONDS);
      long timeOut = HiveConf.getTimeVar(conf, HIVE_OTEL_EXPORTER_TIMEOUT, TimeUnit.SECONDS);
      long initialBackOff = HiveConf.getTimeVar(conf, HIVE_OTEL_RETRY_INITIAL_BACKOFF, TimeUnit.SECONDS);
      long maxBackOff = HiveConf.getTimeVar(conf, HIVE_OTEL_RETRY_MAX_BACKOFF, TimeUnit.SECONDS);
      double backoff = HiveConf.getFloatVar(conf, HIVE_OTEL_RETRY_BACKOFF_MULTIPLIER);
      RetryPolicy retryPolicy = RetryPolicy.builder()
          .setInitialBackoff(Duration.ofSeconds(initialBackOff))
          .setMaxAttempts(5)
          .setMaxBackoff(Duration.ofSeconds(maxBackOff))
          .setBackoffMultiplier(backoff)
          .build();

      SpanExporter otlpExporter =
          OtlpGrpcSpanExporter.builder()
              .setEndpoint(endPoint)
              .setTimeout(timeOut, TimeUnit.SECONDS)
              .setRetryPolicy(retryPolicy).build();

      // Set up BatchSpanProcessor (with retry logic) to handle telemetry data
      BatchSpanProcessor batchSpanProcessor =
          BatchSpanProcessor.builder(otlpExporter).setExporterTimeout(timeOut, TimeUnit.SECONDS).build();

      MetricExporter otlpMetricExporter =
          OtlpGrpcMetricExporter.builder()
              .setEndpoint(endPoint)
              .setTimeout(timeOut, TimeUnit.SECONDS)
              .build();

      PeriodicMetricReader metricReader =
          PeriodicMetricReader.builder(otlpMetricExporter)
              .setInterval(Duration.ofSeconds(otelExporterFrequency))
              .build();

      otel = OpenTelemetrySdk.builder().setTracerProvider(
              SdkTracerProvider.builder().addSpanProcessor(batchSpanProcessor)
                  .addResource(Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), "hive_otel"))).build())
          .setMeterProvider(SdkMeterProvider.builder().registerMetricReader(metricReader)
              .addResource(Resource.create(Attributes.of(AttributeKey.stringKey("service.name"), "hive_otel"))).build())
          .buildAndRegisterGlobal();
    }
    return otel;
  }
}
