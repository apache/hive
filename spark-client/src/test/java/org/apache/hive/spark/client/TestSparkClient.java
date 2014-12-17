/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.hive.spark.client;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.net.URL;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import org.apache.hive.spark.counter.SparkCounters;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;

public class TestSparkClient {

  // Timeouts are bad... mmmkay.
  private static final long TIMEOUT = 10;

  private Map<String, String> createConf(boolean local) {
    Map<String, String> conf = new HashMap<String, String>();
    if (local) {
      conf.put(SparkClientFactory.CONF_KEY_IN_PROCESS, "true");
      conf.put("spark.master", "local");
      conf.put("spark.app.name", "SparkClientSuite Local App");
    } else {
      String classpath = System.getProperty("java.class.path");
      conf.put("spark.master", "local");
      conf.put("spark.app.name", "SparkClientSuite Remote App");
      conf.put("spark.driver.extraClassPath", classpath);
      conf.put("spark.executor.extraClassPath", classpath);
    }

    if (!Strings.isNullOrEmpty(System.getProperty("spark.home"))) {
      conf.put("spark.home", System.getProperty("spark.home"));
    }

    return conf;
  }

  @Test
  public void testJobSubmission() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        JobHandle<String> handle = client.submit(new SimpleJob());
        assertEquals("hello", handle.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testSimpleSparkJob() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        JobHandle<Long> handle = client.submit(new SparkJob());
        assertEquals(Long.valueOf(5L), handle.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testErrorJob() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
      JobHandle<String> handle = client.submit(new SimpleJob());
        try {
          handle.get(TIMEOUT, TimeUnit.SECONDS);
        } catch (ExecutionException ee) {
          assertTrue(ee.getCause() instanceof IllegalStateException);
        }
      }
    });
  }

  @Test
  public void testRemoteClient() throws Exception {
    runTest(false, new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        JobHandle<Long> handle = client.submit(new SparkJob());
        assertEquals(Long.valueOf(5L), handle.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testMetricsCollection() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        JobHandle<Integer> future = client.submit(new AsyncSparkJob());
        future.get(TIMEOUT, TimeUnit.SECONDS);
        MetricsCollection metrics = future.getMetrics();
        assertEquals(1, metrics.getJobIds().size());
        assertTrue(metrics.getAllMetrics().executorRunTime > 0L);

        JobHandle<Integer> future2 = client.submit(new AsyncSparkJob());
        future2.get(TIMEOUT, TimeUnit.SECONDS);
        MetricsCollection metrics2 = future2.getMetrics();
        assertEquals(1, metrics2.getJobIds().size());
        assertFalse(Objects.equal(metrics.getJobIds(), metrics2.getJobIds()));
        assertTrue(metrics2.getAllMetrics().executorRunTime > 0L);
      }
    });
  }

  @Test
  public void testAddJarsAndFiles() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        File jar = null;
        File file = null;

        try {
          // Test that adding a jar to the remote context makes it show up in the classpath.
          jar = File.createTempFile("test", ".jar");

          JarOutputStream jarFile = new JarOutputStream(new FileOutputStream(jar));
          jarFile.putNextEntry(new ZipEntry("test.resource"));
          jarFile.write("test resource".getBytes("UTF-8"));
          jarFile.closeEntry();
          jarFile.close();

          client.addJar(new URL("file:" + jar.getAbsolutePath()))
            .get(TIMEOUT, TimeUnit.SECONDS);

          // Need to run a Spark job to make sure the jar is added to the class loader. Monitoring
          // SparkContext#addJar() doesn't mean much, we can only be sure jars have been distributed
          // when we run a task after the jar has been added.
          String result = client.submit(new JarJob()).get(TIMEOUT, TimeUnit.SECONDS);
          assertEquals("test resource", result);

          // Test that adding a file to the remote context makes it available to executors.
          file = File.createTempFile("test", ".file");

          FileOutputStream fileStream = new FileOutputStream(file);
          fileStream.write("test file".getBytes("UTF-8"));
          fileStream.close();

          client.addJar(new URL("file:" + file.getAbsolutePath()))
            .get(TIMEOUT, TimeUnit.SECONDS);

          // The same applies to files added with "addFile". They're only guaranteed to be available
          // to tasks started after the addFile() call completes.
          result = client.submit(new FileJob(file.getName()))
            .get(TIMEOUT, TimeUnit.SECONDS);
          assertEquals("test file", result);
        } finally {
          if (jar != null) {
            jar.delete();
          }
          if (file != null) {
            file.delete();
          }
        }
      }
    });
  }

  @Test
  public void testCounters() throws Exception {
    runTest(true, new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        JobHandle<?> job = client.submit(new CounterIncrementJob());
        job.get(TIMEOUT, TimeUnit.SECONDS);

        SparkCounters counters = job.getSparkCounters();
        assertNotNull(counters);

        long expected = 1 + 2 + 3 + 4 + 5;
        assertEquals(expected, counters.getCounter("group1", "counter1").getValue());
        assertEquals(expected, counters.getCounter("group2", "counter2").getValue());
      }
    });
  }

  private void runTest(boolean local, TestFunction test) throws Exception {
    Map<String, String> conf = createConf(local);
    SparkClientFactory.initialize(conf);
    SparkClient client = null;
    try {
      test.config(conf);
      client = SparkClientFactory.createClient(conf);
      test.call(client);
    } finally {
      if (client != null) {
        client.stop();
      }
      SparkClientFactory.stop();
    }
  }

  private static class SimpleJob implements Job<String> {

    @Override
    public String call(JobContext jc) {
      return "hello";
    }

  }

  private static class SparkJob implements Job<Long> {

    @Override
    public Long call(JobContext jc) {
      JavaRDD<Integer> rdd = jc.sc().parallelize(Arrays.asList(1, 2, 3, 4, 5));
      return rdd.count();
    }

  }

  private static class AsyncSparkJob implements Job<Integer> {

    @Override
    public Integer call(JobContext jc) throws Exception {
      JavaRDD<Integer> rdd = jc.sc().parallelize(Arrays.asList(1, 2, 3, 4, 5));
      JavaFutureAction<?> future = jc.monitor(rdd.foreachAsync(new VoidFunction<Integer>() {
        @Override
        public void call(Integer l) throws Exception {

        }
      }), null, null);

      future.get(TIMEOUT, TimeUnit.SECONDS);

      return 1;
    }

  }

  private static class ErrorJob implements Job<String> {

    @Override
    public String call(JobContext jc) {
      throw new IllegalStateException("This job does not work.");
    }

  }

  private static class JarJob implements Job<String>, Function<Integer, String> {

    @Override
    public String call(JobContext jc) {
      return jc.sc().parallelize(Arrays.asList(1)).map(this).collect().get(0);
    }

    @Override
    public String call(Integer i) throws Exception {
      ClassLoader ccl = Thread.currentThread().getContextClassLoader();
      InputStream in = ccl.getResourceAsStream("test.resource");
      byte[] bytes = ByteStreams.toByteArray(in);
      in.close();
      return new String(bytes, 0, bytes.length, "UTF-8");
    }

  }

  private static class FileJob implements Job<String>, Function<Integer, String> {

    private final String fileName;

    FileJob() {
      this(null);
    }

    FileJob(String fileName) {
      this.fileName = fileName;
    }

    @Override
    public String call(JobContext jc) {
      return jc.sc().parallelize(Arrays.asList(1)).map(this).collect().get(0);
    }

    @Override
    public String call(Integer i) throws Exception {
      InputStream in = new FileInputStream(SparkFiles.get(fileName));
      byte[] bytes = ByteStreams.toByteArray(in);
      in.close();
      return new String(bytes, 0, bytes.length, "UTF-8");
    }

  }

  private static class CounterIncrementJob implements Job<String>, VoidFunction<Integer> {

    private SparkCounters counters;

    @Override
    public String call(JobContext jc) {
      counters = new SparkCounters(jc.sc());
      counters.createCounter("group1", "counter1");
      counters.createCounter("group2", "counter2");

      jc.monitor(jc.sc().parallelize(Arrays.asList(1, 2, 3, 4, 5), 5).foreachAsync(this),
          counters, null);

      return null;
    }

    @Override
    public void call(Integer l) throws Exception {
      counters.getCounter("group1", "counter1").increment(l.longValue());
      counters.getCounter("group2", "counter2").increment(l.longValue());
    }

  }

  private static abstract class TestFunction {
    abstract void call(SparkClient client) throws Exception;
    void config(Map<String, String> conf) { }
  }

}
