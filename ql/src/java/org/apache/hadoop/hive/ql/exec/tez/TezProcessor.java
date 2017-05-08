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
package org.apache.hadoop.hive.ql.exec.tez;

import java.io.IOException;
import java.text.NumberFormat;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.common.TezUtils;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.AbstractLogicalIOProcessor;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.ExecutionContext;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;

/**
 * Hive processor for Tez that forms the vertices in Tez and processes the data.
 * Does what ExecMapper and ExecReducer does for hive in MR framework.
 */
public class TezProcessor extends AbstractLogicalIOProcessor {

  /**
   * This provides the ability to pass things into TezProcessor, which is normally impossible
   * because of how Tez APIs are structured. Piggyback on ExecutionContext.
   */
  public static interface Hook {
    void initializeHook(TezProcessor source);
  }

  private static final Logger LOG = LoggerFactory.getLogger(TezProcessor.class);
  protected boolean isMap = false;

  protected RecordProcessor rproc = null;
  private final AtomicBoolean aborted = new AtomicBoolean(false);

  protected JobConf jobConf;

  private static final String CLASS_NAME = TezProcessor.class.getName();
  private final PerfLogger perfLogger = SessionState.getPerfLogger();

  protected ProcessorContext processorContext;

  protected static final NumberFormat taskIdFormat = NumberFormat.getInstance();
  protected static final NumberFormat jobIdFormat = NumberFormat.getInstance();
  static {
    taskIdFormat.setGroupingUsed(false);
    taskIdFormat.setMinimumIntegerDigits(6);
    jobIdFormat.setGroupingUsed(false);
    jobIdFormat.setMinimumIntegerDigits(4);
  }

  public TezProcessor(ProcessorContext context) {
    super(context);
    ObjectCache.setupObjectRegistry(context.getObjectRegistry());
  }

  @Override
  public void close() throws IOException {
    // we have to close in the processor's run method, because tez closes inputs
    // before calling close (TEZ-955) and we might need to read inputs
    // when we flush the pipeline.
  }

  @Override
  public void handleEvents(List<Event> arg0) {
    //this is not called by tez, so nothing to be done here
  }

  @Override
  public void initialize() throws IOException {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_INITIALIZE_PROCESSOR);
    Configuration conf = TezUtils.createConfFromUserPayload(getContext().getUserPayload());
    this.jobConf = new JobConf(conf);
    this.processorContext = getContext();
    ExecutionContext execCtx = processorContext.getExecutionContext();
    if (execCtx instanceof Hook) {
      ((Hook)execCtx).initializeHook(this);
    }
    setupMRLegacyConfigs(processorContext);
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_INITIALIZE_PROCESSOR);
  }

  private void setupMRLegacyConfigs(ProcessorContext processorContext) {
    // Hive "insert overwrite local directory" uses task id as dir name
    // Setting the id in jobconf helps to have the similar dir name as MR
    StringBuilder taskAttemptIdBuilder = new StringBuilder("attempt_");
    taskAttemptIdBuilder.append(processorContext.getApplicationId().getClusterTimestamp())
        .append("_")
        .append(jobIdFormat.format(processorContext.getApplicationId().getId()))
        .append("_");
    if (isMap) {
      taskAttemptIdBuilder.append("m_");
    } else {
      taskAttemptIdBuilder.append("r_");
    }
    taskAttemptIdBuilder.append(taskIdFormat.format(processorContext.getTaskIndex()))
      .append("_")
      .append(processorContext.getTaskAttemptNumber());

    // In MR, mapreduce.task.attempt.id is same as mapred.task.id. Go figure.
    String taskAttemptIdStr = taskAttemptIdBuilder.toString();
    this.jobConf.set("mapred.task.id", taskAttemptIdStr);
    this.jobConf.set("mapreduce.task.attempt.id", taskAttemptIdStr);
    this.jobConf.setInt("mapred.task.partition", processorContext.getTaskIndex());
  }

  @Override
  public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs)
      throws Exception {

    if (aborted.get()) {
      return;
    }

    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_PROCESSOR);
    // in case of broadcast-join read the broadcast edge inputs
    // (possibly asynchronously)

    if (LOG.isDebugEnabled()) {
      LOG.debug("Running task: " + getContext().getUniqueIdentifier());
    }

    synchronized (this) {
      // This check isn't absolutely mandatory, given the aborted check outside of the
      // Processor creation.
      if (aborted.get()) {
        return;
      }
      // There should be no blocking operation in RecordProcessor creation,
      // otherwise the abort operation will not register since they are synchronized on the same
      // lock.
      if (isMap) {
        rproc = new MapRecordProcessor(jobConf, getContext());
      } else {
        rproc = new ReduceRecordProcessor(jobConf, getContext());
      }
    }

    if (!aborted.get()) {
      initializeAndRunProcessor(inputs, outputs);
    }
    // TODO HIVE-14042. In case of an abort request, throw an InterruptedException
  }

  protected void initializeAndRunProcessor(Map<String, LogicalInput> inputs,
      Map<String, LogicalOutput> outputs)
      throws Exception {
    Throwable originalThrowable = null;
    try {

      MRTaskReporter mrReporter = new MRTaskReporter(getContext());
      // Init and run are both potentially long, and blocking operations. Synchronization
      // with the 'abort' operation will not work since if they end up blocking on a monitor
      // which does not belong to the lock, the abort will end up getting blocked.
      // Both of these method invocations need to handle the abort call on their own.
      rproc.init(mrReporter, inputs, outputs);
      rproc.run();

      //done - output does not need to be committed as hive does not use outputcommitter
      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_RUN_PROCESSOR);
    } catch (Throwable t) {
      originalThrowable = t;
    } finally {
      if (originalThrowable != null && originalThrowable instanceof Error) {
        LOG.error(StringUtils.stringifyException(originalThrowable));
        throw new RuntimeException(originalThrowable);
      }

      try {
        if (rproc != null) {
          rproc.close();
        }
      } catch (Throwable t) {
        if (originalThrowable == null) {
          originalThrowable = t;
        }
      }
      if (originalThrowable != null) {
        LOG.error(StringUtils.stringifyException(originalThrowable));
        if (originalThrowable instanceof InterruptedException) {
          throw (InterruptedException) originalThrowable;
        } else {
          throw new RuntimeException(originalThrowable);
        }
      }
    }
  }

  @Override
  public void abort() {
    RecordProcessor rProcLocal;
    synchronized (this) {
      LOG.info("Received abort");
      aborted.set(true);
      rProcLocal = rproc;
    }
    if (rProcLocal != null) {
      LOG.info("Forwarding abort to RecordProcessor");
      rProcLocal.abort();
    } else {
      LOG.info("RecordProcessor not yet setup. Abort will be ignored");
    }
  }

  /**
   * KVOutputCollector. OutputCollector that writes using KVWriter.
   * Must be initialized before it is used.
   *
   */
  @SuppressWarnings("rawtypes")
  static class TezKVOutputCollector implements OutputCollector {
    private KeyValueWriter writer;
    private final LogicalOutput output;

    TezKVOutputCollector(LogicalOutput logicalOutput) {
      this.output = logicalOutput;
    }

    void initialize() throws Exception {
      this.writer = (KeyValueWriter) output.getWriter();
    }

    @Override
    public void collect(Object key, Object value) throws IOException {
      writer.write(key, value);
    }
  }

  public JobConf getConf() {
    return jobConf;
  }
}
