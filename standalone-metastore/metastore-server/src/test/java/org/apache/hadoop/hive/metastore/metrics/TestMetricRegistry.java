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

import com.codahale.metrics.MetricRegistry;
import org.apache.hadoop.hive.metastore.annotation.MetastoreUnitTest;
import org.junit.Assert;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(MetastoreUnitTest.class)
public class TestMetricRegistry {

  @Test
  public void testCheckRegisterFunctionThrowsIllegalArgumentWithASpecificMessage() {
    MetricRegistry metricRegistry = new MetricRegistry();
    try {
      metricRegistry.register("my-name", new MapMetrics());
    } catch (IllegalArgumentException e) {
      if (!e.getMessage().equals("Unknown metric type")) {
        Assert.fail("As part of the HIVE-25959 we expect the message in the exception to be 'Unknown metric type'.\n"
            + "If this has changed please adjust the code of the Metrics#getOrCreateMapMetrics.");
      }
    }
  }
}
