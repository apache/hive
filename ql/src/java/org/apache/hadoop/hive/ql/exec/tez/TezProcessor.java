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
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor.KVOutputCollector;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.tez.common.TezUtils;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.Event;
import org.apache.tez.runtime.api.LogicalIOProcessor;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.TezProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueWriter;

/**
 * Hive processor for Tez that forms the vertices in Tez and processes the data.
 * Does what ExecMapper and ExecReducer does for hive in MR framework.
 */
public class TezProcessor implements LogicalIOProcessor {
  private static final Log LOG = LogFactory.getLog(TezProcessor.class);
  private boolean isMap = false;

  RecordProcessor rproc = null;

  private JobConf jobConf;

  private static final String CLASS_NAME = TezProcessor.class.getName();
  private final PerfLogger perfLogger = PerfLogger.getPerfLogger();

  private TezProcessorContext processorContext;

  protected static final NumberFormat taskIdFormat = NumberFormat.getInstance();
  protected static final NumberFormat jobIdFormat = NumberFormat.getInstance();
  static {
    taskIdFormat.setGroupingUsed(false);
    taskIdFormat.setMinimumIntegerDigits(6);
    jobIdFormat.setGroupingUsed(false);
    jobIdFormat.setMinimumIntegerDigits(4);
  }

  public TezProcessor(boolean isMap) {
    this.isMap = isMap;
  }

  @Override
  public void close() throws IOException {
    if(rproc != null){
      rproc.close();
    }
  }

  @Override
  public void handleEvents(List<Event> arg0) {
    //this is not called by tez, so nothing to be done here
  }

  @Override
  public void initialize(TezProcessorContext processorContext)
      throws IOException {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_INITIALIZE_PROCESSOR);
    this.processorContext = processorContext;
    //get the jobconf
    byte[] userPayload = processorContext.getUserPayload();
    Configuration conf = TezUtils.createConfFromUserPayload(userPayload);
    this.jobConf = new JobConf(conf);
    setupMRLegacyConfigs(processorContext);
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_INITIALIZE_PROCESSOR);
  }

  private void setupMRLegacyConfigs(TezProcessorContext processorContext) {
    // Hive "insert overwrite local directory" uses task id as dir name
    // Setting the id in jobconf helps to have the similar dir name as MR
    StringBuilder taskAttemptIdBuilder = new StringBuilder("task");
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
  }

  @Override
  public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs)
      throws Exception {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_PROCESSOR);
    // in case of broadcast-join read the broadcast edge inputs
    // (possibly asynchronously)

    LOG.info("Running map: " + processorContext.getUniqueIdentifier());

    Map<String, OutputCollector> outMap = new HashMap<String, OutputCollector>();

    for (String outputName: outputs.keySet()) {
      LOG.info("Handling output: " + outputName);
      KeyValueWriter kvWriter = (KeyValueWriter) outputs.get(outputName).getWriter();
      OutputCollector collector = new KVOutputCollector(kvWriter);
      outMap.put(outputName, collector);
    }

    if(isMap){
      rproc = new MapRecordProcessor();
    }
    else{
      rproc = new ReduceRecordProcessor();
    }

    MRTaskReporter mrReporter = new MRTaskReporter(processorContext);
    rproc.init(jobConf, mrReporter, inputs, outMap);
    rproc.run();

    //done - output does not need to be committed as hive does not use outputcommitter
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_RUN_PROCESSOR);
  }

  /**
   * KVOutputCollector. OutputCollector that writes using KVWriter
   *
   */
  static class KVOutputCollector implements OutputCollector {
    private final KeyValueWriter output;

    KVOutputCollector(KeyValueWriter output) {
      this.output = output;
    }

    public void collect(Object key, Object value) throws IOException {
        output.write(key, value);
    }
  }

}
