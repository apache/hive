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
package org.apache.hadoop.hive.ql.exec.tez;

import java.util.Map;

import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;

/**
 * TezContext contains additional context only available with Tez
 */
public class TezContext extends MapredContext {

  // all the inputs for the tez processor
  private Map<String, LogicalInput> inputs;

  private Map<String, LogicalOutput> outputs;

  private ProcessorContext processorContext;

  private RecordSource[] sources;

  private Map<Integer, DummyStoreOperator> dummyOpsMap;

  public TezContext(boolean isMap, JobConf jobConf) {
    super(isMap, jobConf);
  }

  public void setInputs(Map<String, LogicalInput> inputs) {
    this.inputs = inputs;
  }

  public void setOutputs(Map<String, LogicalOutput> outputs) {
    this.outputs = outputs;
  }

  public LogicalInput getInput(String name) {
    if (inputs == null) {
      return null;
    }
    return inputs.get(name);
  }

  public LogicalOutput getOutput(String name) {
    if (outputs == null) {
      return null;
    }
    return outputs.get(name);
  }

  public void setTezProcessorContext(ProcessorContext processorContext) {
    this.processorContext = processorContext;
  }

  public ProcessorContext getTezProcessorContext() {
    return processorContext;
  }

  public RecordSource[] getRecordSources() {
    return sources;
  }

  public void setRecordSources(RecordSource[] sources) {
    this.sources = sources;
  }

  public void setDummyOpsMap(Map<Integer, DummyStoreOperator> dummyOpsMap) {
    this.dummyOpsMap = dummyOpsMap;
  }

  public Map<Integer, DummyStoreOperator> getDummyOpsMap() {
    return dummyOpsMap;
  }
}
