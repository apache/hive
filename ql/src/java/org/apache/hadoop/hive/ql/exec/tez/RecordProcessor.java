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
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor.TezKVOutputCollector;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Process input from tez LogicalInput and write output
 * It has different subclasses for map and reduce processing
 */
public abstract class RecordProcessor extends InterruptibleProcessing {
  protected final JobConf jconf;
  protected Map<String, LogicalInput> inputs;
  protected Map<String, LogicalOutput> outputs;
  protected Map<String, OutputCollector> outMap;
  protected final ProcessorContext processorContext;

  private static final Logger LOG = LoggerFactory.getLogger(RecordProcessor.class);

  protected MRTaskReporter reporter;

  protected PerfLogger perfLogger = SessionState.getPerfLogger();
  protected String CLASS_NAME = RecordProcessor.class.getName();

  public RecordProcessor(JobConf jConf, ProcessorContext processorContext) {
    this.jconf = jConf;
    this.processorContext = processorContext;
  }

  /**
   * Common initialization code for RecordProcessors
   * @param mrReporter
   * @param inputs map of Input names to {@link LogicalInput}s
   * @param outputs map of Output names to {@link LogicalOutput}s
   * @throws Exception
   */
  void init(MRTaskReporter mrReporter,
      Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {
    this.reporter = mrReporter;
    this.inputs = inputs;
    this.outputs = outputs;

    checkAbortCondition();
    Utilities.tryLoggingClassPaths(jconf, LOG);
  }

  /**
   * start processing the inputs and writing output
   * @throws Exception
   */
  abstract void run() throws Exception;

  abstract void close();

  protected void createOutputMap() {
    Preconditions.checkState(outMap == null, "Outputs should only be setup once");
    outMap = new HashMap<>();
    for (Entry<String, LogicalOutput> entry : outputs.entrySet()) {
      TezKVOutputCollector collector = new TezKVOutputCollector(entry.getValue());
      outMap.put(entry.getKey(), collector);
    }
  }

  public List<BaseWork> getMergeWorkList(final JobConf jconf, String key, String queryId,
      ObjectCache cache, List<String> cacheKeys) throws HiveException {
    String prefixes = jconf.get(DagUtils.TEZ_MERGE_WORK_FILE_PREFIXES);
    if (prefixes != null) {
      List<BaseWork> mergeWorkList = new ArrayList<>();

      for (final String prefix : prefixes.split(",")) {
        if (prefix.isEmpty()) {
          continue;
        }

        key = prefix;
        cacheKeys.add(key);

        mergeWorkList.add(cache.retrieve(key, () -> Utilities.getMergeWork(jconf, prefix)));
      }

      return mergeWorkList;
    } else {
      return null;
    }
  }
}
