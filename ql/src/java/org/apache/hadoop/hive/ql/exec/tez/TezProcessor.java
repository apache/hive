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
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.tez.common.TezUtils;
import org.apache.tez.mapreduce.input.MRInput;
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

  boolean isMap;
  RecordProcessor rproc = null;

  private JobConf jobConf;

  private TezProcessorContext processorContext;

  public TezProcessor() {
    this.isMap = true;
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
    this.processorContext = processorContext;
    //get the jobconf
    byte[] userPayload = processorContext.getUserPayload();
    Configuration conf = TezUtils.createConfFromUserPayload(userPayload);
    this.jobConf = new JobConf(conf);
  }

  @Override
  public void run(Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs)
      throws Exception {
    // in case of broadcast-join read the broadcast edge inputs
    // (possibly asynchronously)

    LOG.info("Running map: " + processorContext.getUniqueIdentifier());

    //this will change when TezProcessor has support for shuffle joins and broadcast joins
    if (inputs.size() != 1){
      throw new IOException("Cannot handle multiple inputs "
          + " inputCount=" + inputs.size());
    }

    if(outputs.size() > 1) {
          throw new IOException("Cannot handle more than one output"
          + ", outputCount=" + outputs.size());
    }
    LogicalInput in = inputs.values().iterator().next();
    LogicalOutput out = outputs.values().iterator().next();

    MRInput input = (MRInput)in;

    //update config
    Configuration updatedConf = input.getConfigUpdates();
    if (updatedConf != null) {
      for (Entry<String, String> entry : updatedConf) {
        this.jobConf.set(entry.getKey(), entry.getValue());
      }
    }

    KeyValueWriter kvWriter = (KeyValueWriter)out.getWriter();
    OutputCollector collector = new KVOutputCollector(kvWriter);

    if(isMap){
      rproc = new MapRecordProcessor();
    }
    else{
      throw new UnsupportedOperationException("Reduce is yet to be implemented");
    }

    MRTaskReporter mrReporter = new MRTaskReporter(processorContext);
    rproc.init(jobConf, mrReporter, inputs.values(), collector);
    rproc.run();

    //done - output does not need to be committed as hive does not use outputcommitter
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
