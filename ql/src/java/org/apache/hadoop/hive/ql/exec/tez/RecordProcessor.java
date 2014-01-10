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
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.LogicalInput;

/**
 * Process input from tez LogicalInput and write output
 * It has different subclasses for map and reduce processing
 */
public abstract class RecordProcessor  {

  protected JobConf jconf;
  protected Map<String, LogicalInput> inputs;
  protected Map<String, OutputCollector> outMap;

  public static final Log l4j = LogFactory.getLog(RecordProcessor.class);


  // used to log memory usage periodically
  public static MemoryMXBean memoryMXBean;
  protected boolean isLogInfoEnabled = false;
  protected MRTaskReporter reporter;

  private long numRows = 0;
  private long nextUpdateCntr = 1;
  protected PerfLogger perfLogger = PerfLogger.getPerfLogger();
  protected String CLASS_NAME = RecordProcessor.class.getName();


  /**
   * Common initialization code for RecordProcessors
   * @param jconf
   * @param mrReporter
   * @param inputs
   * @param out
   */
  void init(JobConf jconf, MRTaskReporter mrReporter, Map<String, LogicalInput> inputs,
      Map<String, OutputCollector> outMap){
    this.jconf = jconf;
    this.reporter = mrReporter;
    this.inputs = inputs;
    this.outMap = outMap;

    // Allocate the bean at the beginning -
    memoryMXBean = ManagementFactory.getMemoryMXBean();

    l4j.info("maximum memory = " + memoryMXBean.getHeapMemoryUsage().getMax());

    isLogInfoEnabled = l4j.isInfoEnabled();

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
   * @throws IOException
   */
  abstract void run() throws IOException;


  abstract void close();

  /**
   * Log information to be logged at the end
   */
  protected void logCloseInfo() {
    long used_memory = memoryMXBean.getHeapMemoryUsage().getUsed();
    l4j.info("ExecMapper: processed " + numRows + " rows: used memory = "
        + used_memory);
  }

  /**
   * Log number of records processed and memory used after processing many records
   */
  protected void logProgress() {
    numRows++;
    if (numRows == nextUpdateCntr) {
      long used_memory = memoryMXBean.getHeapMemoryUsage().getUsed();
      l4j.info("ExecMapper: processing " + numRows
          + " rows: used memory = " + used_memory);
      nextUpdateCntr = getNextUpdateRecordCounter(numRows);
    }
  }

  private long getNextUpdateRecordCounter(long cntr) {
    // A very simple counter to keep track of number of rows processed by the
    // reducer. It dumps
    // every 1 million times, and quickly before that
    if (cntr >= 1000000) {
      return cntr + 1000000;
    }

    return 10 * cntr;
  }

}
