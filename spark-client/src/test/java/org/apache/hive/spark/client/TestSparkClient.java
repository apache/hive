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

import com.google.common.collect.Lists;
import org.apache.hive.spark.client.JobHandle.Listener;

import org.slf4j.Logger;

import org.slf4j.LoggerFactory;

import org.mockito.invocation.InvocationOnMock;

import org.mockito.stubbing.Answer;

import org.mockito.Mockito;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.InputStream;
import java.io.Serializable;
import java.net.MalformedURLException;
import java.net.URI;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.jar.JarOutputStream;
import java.util.zip.ZipEntry;

import com.google.common.base.Objects;
import com.google.common.base.Strings;
import com.google.common.io.ByteStreams;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hive.spark.counter.SparkCounters;
import org.apache.spark.SparkContext$;
import org.apache.spark.SparkFiles;
import org.apache.spark.api.java.JavaFutureAction;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.VoidFunction;
import org.junit.Test;
import static org.junit.Assert.*;
import static org.mockito.Mockito.*;

public class TestSparkClient {

  // Timeouts are bad... mmmkay.
  private static final long TIMEOUT = 20;
  private static final HiveConf HIVECONF = new HiveConf();

  static {
    String confDir = "../data/conf/spark/standalone/hive-site.xml";
    try {
      HiveConf.setHiveSiteLocation(new File(confDir).toURI().toURL());
    } catch (MalformedURLException e) {
      throw new RuntimeException(e);
    }
    HIVECONF.setBoolVar(HiveConf.ConfVars.HIVE_IN_TEST, true);
    HIVECONF.setVar(HiveConf.ConfVars.SPARK_CLIENT_TYPE, HiveConf.HIVE_SPARK_LAUNCHER_CLIENT);
  }

  private Map<String, String> createConf() {
    Map<String, String> conf = new HashMap<String, String>();

    String classpath = System.getProperty("java.class.path");
    conf.put("spark.master", "local");
    conf.put("spark.app.name", "SparkClientSuite Remote App");
    conf.put("spark.driver.extraClassPath", classpath);
    conf.put("spark.executor.extraClassPath", classpath);
    conf.put("spark.testing", "true");

    if (!Strings.isNullOrEmpty(System.getProperty("spark.home"))) {
      conf.put("spark.home", System.getProperty("spark.home"));
    }

    conf.put("spark.local.dir", Paths.get(System.getProperty("test.tmp.dir"),
            "TestSparkClient-local-dir").toString());

    return conf;
  }

  @Test
  public void testJobSubmission() throws Exception {
    runTest(new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        JobHandle.Listener<String> listener = newListener();
        List<JobHandle.Listener<String>> listeners = Lists.newArrayList(listener);;
        JobHandle<String> handle = client.submit(new SimpleJob(), listeners);
        assertEquals("hello", handle.get(TIMEOUT, TimeUnit.SECONDS));

        // Try an invalid state transition on the handle. This ensures that the actual state
        // change we're interested in actually happened, since internally the handle serializes
        // state changes.
        assertFalse(((JobHandleImpl<String>)handle).changeState(JobHandle.State.SENT));

        verify(listener).onJobStarted(handle);
        verify(listener).onJobSucceeded(same(handle), eq(handle.get()));
      }
    });
  }

  @Test
  public void testSimpleSparkJob() throws Exception {
    runTest(new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        JobHandle<Long> handle = client.submit(new SparkJob());
        assertEquals(Long.valueOf(5L), handle.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testErrorJob() throws Exception {
    runTest(new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        JobHandle.Listener<String> listener = newListener();
        List<JobHandle.Listener<String>> listeners = Lists.newArrayList(listener);
        JobHandle<String> handle = client.submit(new ErrorJob(), listeners);
        try {
          handle.get(TIMEOUT, TimeUnit.SECONDS);
          fail("Should have thrown an exception.");
        } catch (ExecutionException ee) {
          assertTrue(ee.getCause() instanceof IllegalStateException);
          assertTrue(ee.getCause().getMessage().contains("Hello"));
        }

        // Try an invalid state transition on the handle. This ensures that the actual state
        // change we're interested in actually happened, since internally the handle serializes
        // state changes.
        assertFalse(((JobHandleImpl<String>)handle).changeState(JobHandle.State.SENT));

        verify(listener).onJobQueued(handle);
        verify(listener).onJobStarted(handle);
        verify(listener).onJobFailed(same(handle), any(Throwable.class));
      }
    });
  }

  @Test
  public void testErrorJobNotSerializable() throws Exception {
    runTest(new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        JobHandle.Listener<String> listener = newListener();
        List<JobHandle.Listener<String>> listeners = Lists.newArrayList(listener);
        JobHandle<String> handle = client.submit(new ErrorJobNotSerializable(), listeners);
        try {
          handle.get(TIMEOUT, TimeUnit.SECONDS);
          fail("Should have thrown an exception.");
        } catch (ExecutionException ee) {
          assertTrue(ee.getCause() instanceof RuntimeException);
          assertTrue(ee.getCause().getMessage().contains("Hello"));
        }

        // Try an invalid state transition on the handle. This ensures that the actual state
        // change we're interested in actually happened, since internally the handle serializes
        // state changes.
        assertFalse(((JobHandleImpl<String>)handle).changeState(JobHandle.State.SENT));

        verify(listener).onJobQueued(handle);
        verify(listener).onJobStarted(handle);
        verify(listener).onJobFailed(same(handle), any(Throwable.class));
      }
    });
  }

  @Test
  public void testSyncRpc() throws Exception {
    runTest(new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        Future<String> result = client.run(new SyncRpc());
        assertEquals("Hello", result.get(TIMEOUT, TimeUnit.SECONDS));
      }
    });
  }

  @Test
  public void testMetricsCollection() throws Exception {
    runTest(new TestFunction() {
      @Override
      public void call(SparkClient client) throws Exception {
        JobHandle.Listener<Integer> listener = newListener();
        List<JobHandle.Listener<Integer>> listeners = Lists.newArrayList(listener);
        JobHandle<Integer> future = client.submit(new AsyncSparkJob(), listeners);
        future.get(TIMEOUT, TimeUnit.SECONDS);
        MetricsCollection metrics = future.getMetrics();
        assertEquals(1, metrics.getJobIds().size());
        assertTrue(metrics.getAllMetrics().executorRunTime >= 0L);
        verify(listener).onSparkJobStarted(same(future),
          eq(metrics.getJobIds().iterator().next()));

        JobHandle.Listener<Integer> listener2 = newListener();
        List<JobHandle.Listener<Integer>> listeners2 = Lists.newArrayList(listener2);
        JobHandle<Integer> future2 = client.submit(new AsyncSparkJob(), listeners2);
        future2.get(TIMEOUT, TimeUnit.SECONDS);
        MetricsCollection metrics2 = future2.getMetrics();
        assertEquals(1, metrics2.getJobIds().size());
        assertFalse(Objects.equal(metrics.getJobIds(), metrics2.getJobIds()));
        assertTrue(metrics2.getAllMetrics().executorRunTime >= 0L);
        verify(listener2).onSparkJobStarted(same(future2),
          eq(metrics2.getJobIds().iterator().next()));
      }
    });
  }

  @Test
  public void testAddJarsAndFiles() throws Exception {
    runTest(new TestFunction() {
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

          client.addJar(new URI("file:" + jar.getAbsolutePath()))
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

          client.addJar(new URI("file:" + file.getAbsolutePath()))
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
    runTest(new TestFunction() {
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

  @Test
  public void testErrorParsing() {
    assertTrue(SparkClientUtilities.containsErrorKeyword("Error.. Test"));
    assertTrue(SparkClientUtilities.containsErrorKeyword("This line has error.."));
    assertTrue(SparkClientUtilities.containsErrorKeyword("Test that line has ExcePtion.."));
    assertTrue(SparkClientUtilities.containsErrorKeyword("Here is eRRor in line.."));
    assertTrue(SparkClientUtilities.containsErrorKeyword("Here is ExceptioNn in line.."));
    assertTrue(SparkClientUtilities.containsErrorKeyword("Here is ERROR and Exception in line.."));
    assertFalse(SparkClientUtilities.containsErrorKeyword("No problems in this line"));
  }

  private static final Logger LOG = LoggerFactory.getLogger(TestSparkClient.class);

  private <T extends Serializable> JobHandle.Listener<T> newListener() {
    @SuppressWarnings("unchecked")
    JobHandle.Listener<T> listener = mock(JobHandle.Listener.class);
    answerWhen(listener, "cancelled").onJobCancelled(Mockito.<JobHandle<T>>any());
    answerWhen(listener, "queued").onJobQueued(Mockito.<JobHandle<T>>any());
    answerWhen(listener, "started").onJobStarted(Mockito.<JobHandle<T>>any());
    answerWhen(listener, "succeeded").onJobSucceeded(
        Mockito.<JobHandle<T>>any(), Mockito.<T>any());
    answerWhen(listener, "job started").onSparkJobStarted(
        Mockito.<JobHandle<T>>any(), Mockito.anyInt());
    Mockito.doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) throws Throwable {
        @SuppressWarnings("rawtypes")
        JobHandleImpl arg = ((JobHandleImpl)invocation.getArguments()[0]);
        LOG.info("Job failed " + arg.getClientJobId(),
            (Throwable)invocation.getArguments()[1]);
        return null;
      };
    }).when(listener).onJobFailed(Mockito.<JobHandle<T>>any(), Mockito.<Throwable>any());
    return listener;
  }

  protected <T extends Serializable> Listener<T> answerWhen(
      Listener<T> listener, final String logStr) {
    return Mockito.doAnswer(new Answer<Void>() {
      public Void answer(InvocationOnMock invocation) throws Throwable {
        @SuppressWarnings("rawtypes")
        JobHandleImpl arg = ((JobHandleImpl)invocation.getArguments()[0]);
        LOG.info("Job " + logStr + " " + arg.getClientJobId());
        return null;
      };
    }).when(listener);
  }

  private void runTest(TestFunction test) throws Exception {
    Map<String, String> conf = createConf();
    SparkClientFactory.initialize(conf, HIVECONF);
    SparkClient client = null;
    try {
      test.config(conf);
      client = SparkClientFactory.createClient(conf, HIVECONF, UUID.randomUUID().toString());
      test.call(client);
    } finally {
      if (client != null) {
        client.stop();
      }
      SparkClientFactory.stop();
      waitForSparkContextShutdown();
    }
  }

  /**
   * This was added to avoid a race condition where we try to create multiple SparkContexts in
   * the same process. Since spark.master = local everything is run in the same JVM. Since we
   * don't wait for the RemoteDriver to shutdown it's SparkContext, its possible that we finish a
   * test before the SparkContext has been shutdown. In order to avoid the multiple SparkContexts
   * in a single JVM exception, we wait for the SparkContext to shutdown after each test.
   */
  private void waitForSparkContextShutdown() throws InterruptedException {
    for (int i = 0; i < 100; i++) {
      if (SparkContext$.MODULE$.getActive().isEmpty()) {
        break;
      }
      Thread.sleep(100);
    }
    if (!SparkContext$.MODULE$.getActive().isEmpty()) {
      throw new IllegalStateException("SparkContext did not shutdown in time");
    }
  }

  private static class SimpleJob implements Job<String> {

    @Override
    public String call(JobContext jc) {
      return "hello";
    }

  }

  private static class ErrorJob implements Job<String> {

    @Override
    public String call(JobContext jc) {
      throw new IllegalStateException("Hello");
    }

  }

  private static class ErrorJobNotSerializable implements Job<String> {

    private static final class NonSerializableException extends Exception {

      private static final long serialVersionUID = 2548414562750016219L;

      private final NonSerializableObject nonSerializableObject;

      private NonSerializableException(String content) {
        super("Hello");
        this.nonSerializableObject = new NonSerializableObject(content);
      }
    }

    private static final class NonSerializableObject {

      String content;

      private NonSerializableObject(String content) {
        this.content = content;
      }
    }

    @Override
    public String call(JobContext jc) throws NonSerializableException {
      throw new NonSerializableException("Hello");
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

  private static class SyncRpc implements Job<String> {

    @Override
    public String call(JobContext jc) {
      return "Hello";
    }

  }

  private abstract static class TestFunction {
    abstract void call(SparkClient client) throws Exception;
    void config(Map<String, String> conf) { }
  }

}
