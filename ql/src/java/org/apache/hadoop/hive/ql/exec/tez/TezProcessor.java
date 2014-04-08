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
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.tez.common.TezUtils;
import org.apache.tez.mapreduce.input.MRInputLegacy;
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
    // we have to close in the processor's run method, because tez closes inputs 
    // before calling close (TEZ-955) and we might need to read inputs
    // when we flush the pipeline.
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
    this.jobConf.setInt("mapred.task.partition",processorContext.getTaskIndex());
  }

  @Override
  public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs)
      throws Exception {
    
    Exception processingException = null;
    
    try{
      perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_RUN_PROCESSOR);
      // in case of broadcast-join read the broadcast edge inputs
      // (possibly asynchronously)
      
      LOG.info("Running task: " + processorContext.getUniqueIdentifier());
      
      if (isMap) {
        rproc = new MapRecordProcessor();
        MRInputLegacy mrInput = getMRInput(inputs);
        try {
          mrInput.init();
        } catch (IOException e) {
          throw new RuntimeException("Failed while initializing MRInput", e);
        }
      } else {
        rproc = new ReduceRecordProcessor();
      }

      TezCacheAccess cacheAccess = TezCacheAccess.createInstance(jobConf);
      // Start the actual Inputs. After MRInput initialization.
      for (Entry<String, LogicalInput> inputEntry : inputs.entrySet()) {
        if (!cacheAccess.isInputCached(inputEntry.getKey())) {
          LOG.info("Input: " + inputEntry.getKey() + " is not cached");
          inputEntry.getValue().start();
        } else {
          LOG.info("Input: " + inputEntry.getKey() + " is already cached. Skipping start");
        }
      }
      
      // Outputs will be started later by the individual Processors.
      
      MRTaskReporter mrReporter = new MRTaskReporter(processorContext);
      rproc.init(jobConf, processorContext, mrReporter, inputs, outputs);
      rproc.run();

      //done - output does not need to be committed as hive does not use outputcommitter
      perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_RUN_PROCESSOR);
    } catch (Exception e) {
      processingException = e;
    } finally {
      try {
        if(rproc != null){
          rproc.close();
        }
      } catch (Exception e) {
        if (processingException == null) {
          processingException = e;
        }
      }
      if (processingException != null) {
        throw processingException;
      }
    }
  }

  /**
   * KVOutputCollector. OutputCollector that writes using KVWriter.
   * Must be initialized before it is used.
   * 
   */
  static class TezKVOutputCollector implements OutputCollector {
    private KeyValueWriter writer;
    private final LogicalOutput output;

    TezKVOutputCollector(LogicalOutput logicalOutput) {
      this.output = logicalOutput;
    }

    void initialize() throws Exception {
      this.writer = (KeyValueWriter) output.getWriter();
    }

    public void collect(Object key, Object value) throws IOException {
      writer.write(key, value);
    }
  }

  static  MRInputLegacy getMRInput(Map<String, LogicalInput> inputs) {
    //there should be only one MRInput
    MRInputLegacy theMRInput = null;
    for(LogicalInput inp : inputs.values()){
      if(inp instanceof MRInputLegacy){
        if(theMRInput != null){
          throw new IllegalArgumentException("Only one MRInput is expected");
        }
        //a better logic would be to find the alias
        theMRInput = (MRInputLegacy)inp;
      }
    }
    return theMRInput;
  }
}
