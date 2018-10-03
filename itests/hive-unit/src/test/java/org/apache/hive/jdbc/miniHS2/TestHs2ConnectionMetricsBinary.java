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
package org.apache.hive.jdbc.miniHS2;

import java.io.IOException;

import java.util.concurrent.TimeUnit;

import org.apache.hadoop.hive.common.metrics.common.MetricsFactory;
import org.apache.hadoop.hive.common.metrics.metrics2.CodahaleMetrics;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.beeline.BeeLine;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * Test the connection metrics using BeeLine client, when HS2 is started in binary mode.
 */
public class TestHs2ConnectionMetricsBinary extends Hs2ConnectionMetrics {

  @BeforeClass
  public static void setup() throws Exception {
    confOverlay.clear();
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_TRANSPORT_MODE.varname, "binary");
    confOverlay.put(HiveConf.ConfVars.HIVE_SERVER2_ENABLE_DOAS.varname, "false");
    Hs2ConnectionMetrics.setup();
  }

  @AfterClass
  public static void tearDown() {
    Hs2ConnectionMetrics.tearDown();
  }


  @Test
  public void testOpenConnectionMetrics() throws Exception {

    CodahaleMetrics metrics = (CodahaleMetrics) MetricsFactory.getInstance();
    String[] beelineArgs = {
        "-u", miniHS2.getBaseJdbcURL() + "default",
        "-n", USERNAME,
        "-p", PASSWORD,
        "-e", "show tables;"};
    BeeLine beeLine = openBeeLineConnection(beelineArgs);

    // wait a couple of sec to make sure the connection is open
    TimeUnit.SECONDS.sleep(3);
    verifyConnectionMetrics(metrics.dumpJson(), 1, 1);
    beeLine.close();
    TimeUnit.SECONDS.sleep(3);
    verifyConnectionMetrics(metrics.dumpJson(), 0, 1);

    beeLine = openBeeLineConnection(beelineArgs);
    TimeUnit.SECONDS.sleep(3);
    verifyConnectionMetrics(metrics.dumpJson(), 1, 2);
    beeLine.close();
    TimeUnit.SECONDS.sleep(3);
    verifyConnectionMetrics(metrics.dumpJson(), 0, 2);


  }

  private BeeLine openBeeLineConnection(String[] beelineArgs) throws IOException {
    BeeLine beeLine = new BeeLine();
    beeLine.begin(beelineArgs, null);
    return beeLine;
  }

}
