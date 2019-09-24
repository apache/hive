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
package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Driver;
import org.apache.hadoop.hive.ql.QueryState;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.processors.CommandProcessorException;
import org.apache.hadoop.hive.ql.session.SessionState;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;

public class TestSparkSessionTimeout {

  @Test
  public void testSparkSessionTimeout()
      throws HiveException, InterruptedException, MalformedURLException, CommandProcessorException {
    String confDir = "../../data/conf/spark/standalone/hive-site.xml";
    HiveConf.setHiveSiteLocation(new File(confDir).toURI().toURL());

    HiveConf conf = new HiveConf();
    conf.set("spark.local.dir", Paths.get(System.getProperty("test.tmp.dir"),
            "TestSparkSessionTimeout-testSparkSessionTimeout-local-dir").toString());

    SessionState.start(conf);

    runTestSparkSessionTimeout(conf, 1);
  }

  @Test
  public void testMultiSessionSparkSessionTimeout() throws InterruptedException,
          ExecutionException {
    List<Future<Void>> futures = new ArrayList<>();
    ExecutorService es = Executors.newFixedThreadPool(10);
    for (int i = 0; i < 10; i++) {
      futures.add(es.submit(() -> {
        String confDir = "../../data/conf/spark/local/hive-site.xml";
        HiveConf.setHiveSiteLocation(new File(confDir).toURI().toURL());

        HiveConf conf = new HiveConf();
        conf.setBoolVar(HiveConf.ConfVars.SPARK_OPTIMIZE_SHUFFLE_SERDE, false);
        conf.set("spark.local.dir", Paths.get(System.getProperty("test.tmp.dir"),
                "TestSparkSessionTimeout-testMultiSessionSparkSessionTimeout-local-dir").toString());

        SessionState.start(conf);

        runTestSparkSessionTimeout(conf, 1);
        return null;
      }));
    }
    for (Future<Void> future : futures) {
      future.get();
    }
  }

  @Test
  public void testSparkSessionMultipleTimeout()
      throws HiveException, InterruptedException, MalformedURLException, CommandProcessorException {
    String confDir = "../../data/conf/spark/standalone/hive-site.xml";
    HiveConf.setHiveSiteLocation(new File(confDir).toURI().toURL());

    HiveConf conf = new HiveConf();
    conf.set("spark.local.dir", Paths.get(System.getProperty("test.tmp.dir"),
            "TestSparkSessionTimeout-testSparkSessionMultipleTimeout-local-dir").toString());

    SessionState.start(conf);

    runTestSparkSessionTimeout(conf, 2);
  }

  private void runTestSparkSessionTimeout(HiveConf conf, int sleepRunIteration)
      throws HiveException, InterruptedException, CommandProcessorException {
    conf.setVar(HiveConf.ConfVars.SPARK_SESSION_TIMEOUT, "5s");
    conf.setVar(HiveConf.ConfVars.SPARK_SESSION_TIMEOUT_PERIOD, "1s");

    String tableName = "test" + UUID.randomUUID().toString().replace("-", "");

    Driver driver = null;

    try {
      driver = new Driver(new QueryState.Builder()
              .withGenerateNewQueryId(true)
              .withHiveConf(conf).build(),
              null, null);

      SparkSession sparkSession = SparkUtilities.getSparkSession(conf, SparkSessionManagerImpl
              .getInstance());

      driver.run("create table " + tableName + " (col int)");
      driver.run("select * from " + tableName + " order by col");

      for (int i = 0; i < sleepRunIteration; i++) {
        Thread.sleep(10000);

        Assert.assertFalse(sparkSession.isOpen());

        driver.run("select * from " + tableName + " order by col");
      }
    } finally {
      if (driver != null) {
        driver.run("drop table if exists " + tableName);
        driver.destroy();
      }
    }
  }
}
