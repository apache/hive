/**
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
package org.apache.hadoop.hive.metastore;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import junit.framework.TestCase;
import org.apache.hadoop.hive.cli.CliSessionState;
import org.apache.hadoop.hive.common.metrics.metrics2.MetricsReporting;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.metadata.Hive;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hive.service.server.HiveServer2;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Map;

/**
 * Tests Hive Metastore Metrics.
 *
 */
public class TestMetaStoreMetrics {

  private static File workDir = new File(System.getProperty("test.tmp.dir"));
  private static File jsonReportFile;

  private static HiveConf hiveConf;
  private static Driver driver;

  @BeforeClass
  public static void before() throws Exception {

    int port = MetaStoreUtils.findFreePort();

    jsonReportFile = new File(workDir, "json_reporting");
    jsonReportFile.delete();

    hiveConf = new HiveConf(TestMetaStoreMetrics.class);
    hiveConf.setVar(HiveConf.ConfVars.METASTOREURIS, "thrift://localhost:" + port);
    hiveConf.setIntVar(HiveConf.ConfVars.METASTORETHRIFTCONNECTIONRETRIES, 3);
    hiveConf.setBoolVar(HiveConf.ConfVars.METASTORE_METRICS, true);
    hiveConf.setBoolVar(HiveConf.ConfVars.HIVE_SUPPORT_CONCURRENCY, false);
    hiveConf.setVar(HiveConf.ConfVars.HIVE_METRICS_REPORTER, MetricsReporting.JSON_FILE.name() + "," + MetricsReporting.JMX.name());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_LOCATION, jsonReportFile.toString());
    hiveConf.setVar(HiveConf.ConfVars.HIVE_METRICS_JSON_FILE_INTERVAL, "100ms");

    MetaStoreUtils.startMetaStore(port, ShimLoader.getHadoopThriftAuthBridge(), hiveConf);

    SessionState.start(new CliSessionState(hiveConf));
    driver = new Driver(hiveConf);
  }

  @Test
  public void testMetricsFile() throws Exception {
    driver.run("show databases");

    //give timer thread a chance to print the metrics
    Thread.sleep(2000);

    //As the file is being written, try a few times.
    //This can be replaced by CodahaleMetrics's JsonServlet reporter once it is exposed.
    byte[] jsonData = Files.readAllBytes(Paths.get(jsonReportFile.getAbsolutePath()));
    ObjectMapper objectMapper = new ObjectMapper();

    JsonNode rootNode = objectMapper.readTree(jsonData);
    JsonNode timersNode = rootNode.path("timers");
    JsonNode methodCounterNode = timersNode.path("api_get_all_databases");
    JsonNode methodCountNode = methodCounterNode.path("count");
    Assert.assertTrue(methodCountNode.asInt() > 0);
  }


  @Test
  public void testConnections() throws Exception {
    byte[] jsonData = Files.readAllBytes(Paths.get(jsonReportFile.getAbsolutePath()));
    ObjectMapper objectMapper = new ObjectMapper();
    JsonNode rootNode = objectMapper.readTree(jsonData);
    JsonNode countersNode = rootNode.path("counters");
    JsonNode openCnxNode = countersNode.path("open_connections");
    JsonNode openCnxCountNode = openCnxNode.path("count");
    Assert.assertTrue(openCnxCountNode.asInt() == 1);

    //create a second connection
    HiveMetaStoreClient msc = new HiveMetaStoreClient(hiveConf);
    HiveMetaStoreClient msc2 = new HiveMetaStoreClient(hiveConf);
    Thread.sleep(2000);

    jsonData = Files.readAllBytes(Paths.get(jsonReportFile.getAbsolutePath()));
    rootNode = objectMapper.readTree(jsonData);
    countersNode = rootNode.path("counters");
    openCnxNode = countersNode.path("open_connections");
    openCnxCountNode = openCnxNode.path("count");
    Assert.assertTrue(openCnxCountNode.asInt() == 3);

    msc.close();
    Thread.sleep(2000);

    jsonData = Files.readAllBytes(Paths.get(jsonReportFile.getAbsolutePath()));
    rootNode = objectMapper.readTree(jsonData);
    countersNode = rootNode.path("counters");
    openCnxNode = countersNode.path("open_connections");
    openCnxCountNode = openCnxNode.path("count");
    Assert.assertTrue(openCnxCountNode.asInt() == 2);

    msc2.close();
    Thread.sleep(2000);

    jsonData = Files.readAllBytes(Paths.get(jsonReportFile.getAbsolutePath()));
    rootNode = objectMapper.readTree(jsonData);
    countersNode = rootNode.path("counters");
    openCnxNode = countersNode.path("open_connections");
    openCnxCountNode = openCnxNode.path("count");
    Assert.assertTrue(openCnxCountNode.asInt() == 1);
  }
}
