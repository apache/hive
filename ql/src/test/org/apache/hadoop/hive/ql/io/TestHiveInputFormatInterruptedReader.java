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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.io;

import static org.junit.Assert.assertSame;
import static org.junit.Assert.assertThrows;
import static org.junit.Assert.assertTrue;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.SocketTimeoutException;
import java.nio.channels.ClosedByInterruptException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.conf.HiveConf.ConfVars;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.ql.exec.LimitOperator;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.tez.ObjectCache;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor;
import org.apache.hadoop.hive.ql.io.HiveInputFormat.HiveInputSplit;
import org.apache.hadoop.hive.ql.io.NullRowsInputFormat.NullRowsRecordReader;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.tez.runtime.common.objectregistry.ObjectRegistryImpl;
import org.junit.After;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

/**
 * How {@link HiveInputFormat#getRecordReader} treats an interrupt from the wrapped format: an empty
 * split once the vertex's limit is reached (HIVE-25751), a failure before that.
 */
public class TestHiveInputFormatInterruptedReader {
  private static final String VERTEX = "Map 1";
  private static final Path PATH = new Path("/tmp/interrupted-reader/000000_0");
  private static final AtomicInteger QUERY_IDS = new AtomicInteger();

  @BeforeClass
  public static void setUpObjectRegistry() {
    // Outside a daemon, the limit-reached flag lives in the Tez object cache, which only exists
    // once a processor has set up the registry.
    if (!ObjectCache.isObjectRegistryConfigured()) {
      ObjectCache.setupObjectRegistry(new ObjectRegistryImpl());
    }
  }

  @Before
  public void outsideDaemon() {
    // Other tests in the JVM may leave this set, which would switch to the LLAP object cache.
    LlapProxy.setDaemon(false);
  }

  @After
  public void reset() {
    FailingInputFormat.failure = null;
    FailingInputFormat.interrupt = false;
    // Clears the interrupt flag one of the tests sets, so it doesn't leak into later tests on this thread.
    Thread.interrupted();
  }

  @Test
  public void testInterruptOnceLimitReachedIsAnEmptySplit() throws Exception {
    // RecordReaderWrapper used to turn the first into an empty split before the limit was checked,
    // and the old root-cause check missed the last, which is how Hadoop clients report an interrupt.
    for (IOException failure : new IOException[] {
        new InterruptedIOException("interrupted"),
        new IOException(new ClosedByInterruptException()),
        new IOException(new InterruptedException("interrupted")),
        new IOException(new InterruptedIOException("interrupted"))}) {
      assertTrue(failure.toString(), innerReader(getRecordReader(failure, true))
          instanceof NullRowsRecordReader);
    }
  }

  @Test
  public void testFailureOnAnInterruptedThreadOnceLimitReachedIsAnEmptySplit() throws Exception {
    // Interrupted while the inner reader is created, not up front: code that runs earlier in
    // getRecordReader can swallow an interrupt (Hadoop's Shell static init joins a thread and
    // drops the InterruptedException), which would clear the flag before the check.
    FailingInputFormat.interrupt = true;
    assertTrue(innerReader(getRecordReader(new IOException("closed"), true))
        instanceof NullRowsRecordReader);
  }

  @Test
  public void testInterruptBeforeLimitReachedFails() {
    // An empty split here would let a task that is being killed, or that someone else interrupted,
    // finish with this split's rows missing.
    InterruptedIOException failure = new InterruptedIOException("interrupted");
    IOException thrown = assertThrows(IOException.class, () -> getRecordReader(failure, false));
    assertSame(failure, thrown.getCause());
  }

  @Test
  public void testFailureOnceLimitReachedStillFails() {
    // A timeout is an InterruptedIOException but not an interrupt.
    for (IOException failure : new IOException[] {
        new IOException("corrupt"), new SocketTimeoutException("timed out")}) {
      IOException thrown = assertThrows(IOException.class, () -> getRecordReader(failure, true));
      assertSame(failure, thrown.getCause());
    }
  }

  private static RecordReader<?, ?> getRecordReader(IOException failure, boolean limitReached)
      throws IOException, HiveException {
    String queryId = "query-" + QUERY_IDS.incrementAndGet();
    JobConf job = new JobConf();
    HiveConf.setVar(job, ConfVars.HIVE_QUERY_ID, queryId);
    job.set(TezProcessor.HIVE_TEZ_VERTEX_NAME, VERTEX);
    // What HiveInputFormat reads the vertex name from on the input path.
    job.set("tez.mapreduce.vertex.name", VERTEX);
    if (limitReached) {
      ObjectCacheFactory.getCache(job, queryId, false, true)
          .retrieve(LimitOperator.getLimitReachedKey(job), AtomicBoolean::new).set(true);
    }

    TableDesc table = new TableDesc(FailingInputFormat.class, HiveIgnoreKeyTextOutputFormat.class,
        new Properties());
    Map<Path, PartitionDesc> pathToPartitionInfo = new LinkedHashMap<>();
    pathToPartitionInfo.put(PATH.getParent(), new PartitionDesc(table, null));
    MapWork work = new MapWork();
    work.setPathToPartitionInfo(pathToPartitionInfo);

    HiveInputFormat<?, ?> inputFormat = new HiveInputFormat<>();
    inputFormat.mrwork = work;
    inputFormat.pathToPartitionInfo = pathToPartitionInfo;

    FailingInputFormat.failure = failure;
    HiveInputSplit split = new HiveInputSplit(new FileSplit(PATH, 0, 1, (String[]) null),
        FailingInputFormat.class.getName());
    return inputFormat.getRecordReader(split, job, Reporter.NULL);
  }

  private static RecordReader<?, ?> innerReader(RecordReader<?, ?> reader) {
    return ((HiveContextAwareRecordReader<?, ?>) reader).recordReader;
  }

  /** Fails {@link #getRecordReader} with {@link #failure}, interrupting first if {@link #interrupt}. */
  public static class FailingInputFormat implements InputFormat<Object, Object> {
    static volatile IOException failure;
    static volatile boolean interrupt;

    @Override
    public InputSplit[] getSplits(JobConf job, int numSplits) {
      throw new UnsupportedOperationException();
    }

    @Override
    public RecordReader<Object, Object> getRecordReader(InputSplit split, JobConf job,
        Reporter reporter) throws IOException {
      if (interrupt) {
        Thread.currentThread().interrupt();
      }
      throw failure;
    }
  }
}
