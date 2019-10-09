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
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionImpl;
import org.apache.hadoop.hive.ql.exec.spark.session.SparkSessionManagerImpl;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.spark.SparkContext;
import org.apache.spark.api.java.JavaSparkContext;
import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.lang.reflect.Field;
import java.net.MalformedURLException;
import java.nio.file.Paths;
import java.util.List;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.junit.Assert.fail;

/**
 * With local spark context, all user sessions share the same spark context.
 */
public class TestLocalHiveSparkClient {

  private final CyclicBarrier barrier = new CyclicBarrier(2);

  @Test
  public void testMultiSessionSparkContextReUse() throws MalformedURLException {
    String confDir = "../data/conf/spark/local/hive-site.xml";
    HiveConf.setHiveSiteLocation(new File(confDir).toURI().toURL());

    ExecutorService executor = Executors.newFixedThreadPool(barrier.getParties());

    List<CompletableFuture<Void>> futures =
        IntStream.range(0, barrier.getParties()).boxed()
            .map(i -> CompletableFuture.supplyAsync(() -> execute(i), executor))
            .collect(Collectors.toList());

    futures.forEach(CompletableFuture::join);
  }

  private Void execute(Integer threadId) {
    HiveConf conf = new HiveConf();

    conf.setBoolVar(HiveConf.ConfVars.SPARK_OPTIMIZE_SHUFFLE_SERDE, false);
    conf.set("spark.local.dir", Paths.get(System.getProperty("test.tmp.dir"),
        "TestLocalHiveSparkClient-testMultiSessionSparkContextReuse-local-dir").toString());

    SessionState.start(conf);
    try{
      runSparkTestSession(conf, threadId);
    } catch (Exception ex){
      fail(ex.getMessage());
    }
    return null;
  }

  private void runSparkTestSession(HiveConf conf, int threadId) throws Exception {
    conf.setVar(HiveConf.ConfVars.SPARK_SESSION_TIMEOUT, "10s");
    conf.setVar(HiveConf.ConfVars.SPARK_SESSION_TIMEOUT_PERIOD, "1s");

    Driver driver = null;
    try {
      driver = new Driver(new QueryState.Builder()
          .withGenerateNewQueryId(true)
          .withHiveConf(conf).build(), null, null);

      SparkSession sparkSession = SparkUtilities.getSparkSession(conf,
          SparkSessionManagerImpl.getInstance());

      driver.run("show tables");
      barrier.await();

      SparkContext sparkContext = getSparkContext(sparkSession);
      Assert.assertFalse(sparkContext.isStopped());

      if(threadId == 1) {
        barrier.await();
        closeSparkSession(sparkSession);
        Assert.assertTrue(sparkContext.isStopped());

      } else {
        closeSparkSession(sparkSession);
        Assert.assertFalse(sparkContext.isStopped());
        barrier.await();
      }
    } finally {
      if (driver != null) {
        driver.destroy();
      }
    }
  }

  private void closeSparkSession(SparkSession session) throws ReflectiveOperationException {
    Assert.assertTrue(session.isOpen());
    session.close();

    Assert.assertFalse(session.isOpen());
  }

  private SparkContext getSparkContext(SparkSession sparkSession) throws ReflectiveOperationException {
    HiveSparkClient sparkClient = getSparkClient(sparkSession);
    Assert.assertNotNull(sparkClient);

    return getSparkContext(sparkClient).sc();
  }

  private JavaSparkContext getSparkContext(HiveSparkClient sparkClient) throws ReflectiveOperationException {
    Field sparkContextField = LocalHiveSparkClient.class.getDeclaredField("sc");
    sparkContextField.setAccessible(true);

    return (JavaSparkContext) sparkContextField.get(sparkClient);
  }

  private HiveSparkClient getSparkClient(SparkSession sparkSession) throws ReflectiveOperationException {
    Field clientField = SparkSessionImpl.class.getDeclaredField("hiveSparkClient");
    clientField.setAccessible(true);

    return (HiveSparkClient) clientField.get(sparkSession);
  }
}
