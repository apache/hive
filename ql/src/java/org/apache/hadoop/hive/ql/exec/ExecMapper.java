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

package org.apache.hadoop.hive.ql.exec;

import java.io.IOException;
import java.io.Serializable;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.plan.FetchWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.MapredWork;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

/**
 * ExecMapper.
 *
 */
public class ExecMapper extends MapReduceBase implements Mapper {

  private MapOperator mo;
  private Map<String, FetchOperator> fetchOperators;
  private OutputCollector oc;
  private JobConf jc;
  private boolean abort = false;
  private Reporter rp;
  public static final Log l4j = LogFactory.getLog("ExecMapper");
  private static boolean done;

  // used to log memory usage periodically
  public static MemoryMXBean memoryMXBean;
  private long numRows = 0;
  private long nextCntr = 1;
  private MapredLocalWork localWork = null;

  private final ExecMapperContext execContext = new ExecMapperContext();

  @Override
  public void configure(JobConf job) {
    // Allocate the bean at the beginning -
    memoryMXBean = ManagementFactory.getMemoryMXBean();
    l4j.info("maximum memory = " + memoryMXBean.getHeapMemoryUsage().getMax());

    try {
      l4j.info("conf classpath = "
          + Arrays.asList(((URLClassLoader) job.getClassLoader()).getURLs()));
      l4j.info("thread classpath = "
          + Arrays.asList(((URLClassLoader) Thread.currentThread()
          .getContextClassLoader()).getURLs()));
    } catch (Exception e) {
      l4j.info("cannot get classpath: " + e.getMessage());
    }
    try {
      jc = job;
      execContext.setJc(jc);
      // create map and fetch operators
      MapredWork mrwork = Utilities.getMapRedWork(job);
      mo = new MapOperator();
      mo.setConf(mrwork);
      // initialize map operator
      mo.setChildren(job);
      l4j.info(mo.dump(0));
      mo.setExecContext(execContext);
      mo.initializeLocalWork(jc);
      mo.initialize(jc, null);

      // initialize map local work
      localWork = mrwork.getMapLocalWork();
      if (localWork == null) {
        return;
      }
      fetchOperators = new HashMap<String, FetchOperator>();
      // create map local operators
      for (Map.Entry<String, FetchWork> entry : localWork.getAliasToFetchWork()
          .entrySet()) {
        fetchOperators.put(entry.getKey(), new FetchOperator(entry.getValue(),
            job));
        l4j.info("fetchoperator for " + entry.getKey() + " created");
      }
      // initialize map local operators
      for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
        Operator<? extends Serializable> forwardOp = localWork.getAliasToWork()
            .get(entry.getKey());
        forwardOp.setExecContext(execContext);
        // All the operators need to be initialized before process
        forwardOp.initialize(jc, new ObjectInspector[] {entry.getValue()
            .getOutputObjectInspector()});
        l4j.info("fetchoperator for " + entry.getKey() + " initialized");
      }
      this.execContext.setLocalWork(localWork);
      this.execContext.setFetchOperators(fetchOperators);
      // defer processing of map local operators to first row if in case there
      // is no input (??)
    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        // will this be true here?
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        throw new RuntimeException("Map operator initialization failed", e);
      }
    }

  }

  public void map(Object key, Object value, OutputCollector output,
      Reporter reporter) throws IOException {
    if (oc == null) {
      oc = output;
      rp = reporter;
      mo.setOutputCollector(oc);
      mo.setReporter(rp);
    }
    // reset the execContext for each new row
    execContext.resetRow();

    try {
      if (mo.getDone()) {
        done = true;
      } else {
        // Since there is no concept of a group, we don't invoke
        // startGroup/endGroup for a mapper
        mo.process((Writable) value);
        if (l4j.isInfoEnabled()) {
          numRows++;
          if (numRows == nextCntr) {
            long used_memory = memoryMXBean.getHeapMemoryUsage().getUsed();
            l4j.info("ExecMapper: processing " + numRows
                + " rows: used memory = " + used_memory);
            nextCntr = getNextCntr(numRows);
          }
        }
      }
    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        l4j.fatal(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
    }
  }


  private long getNextCntr(long cntr) {
    // A very simple counter to keep track of number of rows processed by the
    // reducer. It dumps
    // every 1 million times, and quickly before that
    if (cntr >= 1000000) {
      return cntr + 1000000;
    }

    return 10 * cntr;
  }

  @Override
  public void close() {
    // No row was processed
    if (oc == null) {
      l4j.trace("Close called. no row processed by map.");
    }

    // detecting failed executions by exceptions thrown by the operator tree
    // ideally hadoop should let us know whether map execution failed or not
    try {
      mo.close(abort);
      if (fetchOperators != null) {
        MapredLocalWork localWork = mo.getConf().getMapLocalWork();
        for (Map.Entry<String, FetchOperator> entry : fetchOperators.entrySet()) {
          Operator<? extends Serializable> forwardOp = localWork
              .getAliasToWork().get(entry.getKey());
          forwardOp.close(abort);
        }
      }

      if (l4j.isInfoEnabled()) {
        long used_memory = memoryMXBean.getHeapMemoryUsage().getUsed();
        l4j.info("ExecMapper: processed " + numRows + " rows: used memory = "
            + used_memory);
      }

      reportStats rps = new reportStats(rp);
      mo.preorderMap(rps);
      return;
    } catch (Exception e) {
      if (!abort) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException("Hive Runtime Error while closing operators", e);
      }
    }
  }

  public static boolean getDone() {
    return done;
  }

  public boolean isAbort() {
    return abort;
  }

  public void setAbort(boolean abort) {
    this.abort = abort;
  }

  public static void setDone(boolean done) {
    ExecMapper.done = done;
  }

  /**
   * reportStats.
   *
   */
  public static class reportStats implements Operator.OperatorFunc {
    Reporter rp;

    public reportStats(Reporter rp) {
      this.rp = rp;
    }

    public void func(Operator op) {
      Map<Enum, Long> opStats = op.getStats();
      for (Map.Entry<Enum, Long> e : opStats.entrySet()) {
        if (rp != null) {
          rp.incrCounter(e.getKey(), e.getValue());
        }
      }
    }
  }
}
