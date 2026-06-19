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

import io.opentelemetry.api.OpenTelemetry;

import org.apache.hadoop.hive.common.OTELJavaMetrics;

public class LLAPOTELExporter extends Thread {

  private static final String JVM_SCOPE = OTELJavaMetrics.class.getName();
  private final OTELJavaMetrics jvmMetrics;
  private final long frequency;

  public LLAPOTELExporter(OpenTelemetry openTelemetry, long frequency, String addr) {
    this.jvmMetrics = new OTELJavaMetrics(openTelemetry.getMeter(JVM_SCOPE + ":" + addr));
    this.frequency = frequency;
  }

  @Override
  public void run() {
    while (true) {
      jvmMetrics.setJvmMetrics();
      try {
        Thread.sleep(frequency);
      } catch (InterruptedException e) {
        throw new RuntimeException(e);
      }
    }
  }
}
