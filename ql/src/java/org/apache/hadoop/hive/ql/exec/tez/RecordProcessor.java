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
import com.google.common.base.Preconditions;
import com.google.common.collect.Maps;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor.TezKVOutputCollector;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;

import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;
import java.util.Map.Entry;

/**
 * Process input from tez LogicalInput and write output
 * It has different subclasses for map and reduce processing
 */
public abstract class RecordProcessor  {

  protected JobConf jconf;
  protected Map<String, LogicalInput> inputs;
  protected Map<String, LogicalOutput> outputs;
  protected Map<String, OutputCollector> outMap;
  protected ProcessorContext processorContext;

  public static final Log l4j = LogFactory.getLog(RecordProcessor.class);


  // used to log memory usage periodically
  protected boolean isLogInfoEnabled = false;
  protected boolean isLogTraceEnabled = false;
  protected MRTaskReporter reporter;

  protected PerfLogger perfLogger = PerfLogger.getPerfLogger();
  protected String CLASS_NAME = RecordProcessor.class.getName();

  /**
   * Common initialization code for RecordProcessors
   * @param jconf
   * @param processorContext the {@link ProcessorContext}
   * @param mrReporter
   * @param inputs map of Input names to {@link LogicalInput}s
   * @param outputs map of Output names to {@link LogicalOutput}s
   * @throws Exception
   */
  void init(JobConf jconf, ProcessorContext processorContext, MRTaskReporter mrReporter,
      Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {
    this.jconf = jconf;
    this.reporter = mrReporter;
    this.inputs = inputs;
    this.outputs = outputs;
    this.processorContext = processorContext;

    isLogInfoEnabled = l4j.isInfoEnabled();
    isLogTraceEnabled = l4j.isTraceEnabled();

    //log classpaths
    try {
      if (l4j.isDebugEnabled()) {
        l4j.debug("conf classpath = "
            + Arrays.asList(((URLClassLoader) jconf.getClassLoader()).getURLs()));
        l4j.debug("thread classpath = "
            + Arrays.asList(((URLClassLoader) Thread.currentThread()
            .getContextClassLoader()).getURLs()));
      }
    } catch (Exception e) {
      l4j.info("cannot get classpath: " + e.getMessage());
    }
  }

  /**
   * start processing the inputs and writing output
   * @throws Exception
   */
  abstract void run() throws Exception;


  abstract void close();

  protected void createOutputMap() {
    Preconditions.checkState(outMap == null, "Outputs should only be setup once");
    outMap = Maps.newHashMap();
    for (Entry<String, LogicalOutput> entry : outputs.entrySet()) {
      TezKVOutputCollector collector = new TezKVOutputCollector(entry.getValue());
      outMap.put(entry.getKey(), collector);
    }
  }
}
