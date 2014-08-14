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

package org.apache.hadoop.hive.ql.exec.spark;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.MapOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapOperator;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;

import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.MemoryMXBean;
import java.net.URLClassLoader;
import java.util.Arrays;
import java.util.List;
import java.util.Map;


/**
 * Clone from ExecMapper. SparkMapRecordHandler is the bridge between the spark framework and
 * the Hive operator pipeline at execution time. It's main responsibilities are:
 *
 * - Load and setup the operator pipeline from XML
 * - Run the pipeline by transforming key value pairs to records and forwarding them to the operators
 * - Stop execution when the "limit" is reached
 * - Catch and handle errors during execution of the operators.
 *
 */
public class SparkMapRecordHandler {

  private static final String PLAN_KEY = "__MAP_PLAN__";
  private MapOperator mo;
  private OutputCollector oc;
  private JobConf jc;
  private boolean abort = false;
  private Reporter rp;
  public static final Log l4j = LogFactory.getLog(SparkMapRecordHandler.class);
  private boolean done;

  // used to log memory usage periodically
  public static MemoryMXBean memoryMXBean;
  private long numRows = 0;
  private long nextCntr = 1;
  private MapredLocalWork localWork = null;
  private boolean isLogInfoEnabled = false;

  private final ExecMapperContext execContext = new ExecMapperContext();

  public void init(JobConf job, OutputCollector output, Reporter reporter) {
    // Allocate the bean at the beginning -
    memoryMXBean = ManagementFactory.getMemoryMXBean();
    l4j.info("maximum memory = " + memoryMXBean.getHeapMemoryUsage().getMax());

    isLogInfoEnabled = l4j.isInfoEnabled();

    try {
      l4j.info("conf classpath = "
        + Arrays.asList(((URLClassLoader) job.getClassLoader()).getURLs()));
      l4j.info("thread classpath = "
        + Arrays.asList(((URLClassLoader) Thread.currentThread()
        .getContextClassLoader()).getURLs()));
    } catch (Exception e) {
      l4j.info("cannot get classpath: " + e.getMessage());
    }

    setDone(false);

    ObjectCache cache = ObjectCacheFactory.getCache(job);

    try {
      jc = job;
      execContext.setJc(jc);
      // create map and fetch operators
      MapWork mrwork = (MapWork) cache.retrieve(PLAN_KEY);
      if (mrwork == null) {
        mrwork = Utilities.getMapWork(job);
        cache.cache(PLAN_KEY, mrwork);
      } else {
        Utilities.setMapWork(job, mrwork);
      }
      if (mrwork.getVectorMode()) {
        mo = new VectorMapOperator();
      } else {
        mo = new MapOperator();
      }
      mo.setConf(mrwork);
      // initialize map operator
      mo.setChildren(job);
      l4j.info(mo.dump(0));
      // initialize map local work
      localWork = mrwork.getMapLocalWork();
      execContext.setLocalWork(localWork);

      MapredContext.init(true, new JobConf(jc));

      mo.setExecContext(execContext);
      mo.initializeLocalWork(jc);
      mo.initialize(jc, null);

      oc = output;
      rp = reporter;
      OperatorUtils.setChildrenCollector(mo.getChildOperators(), output);
      mo.setReporter(rp);
      MapredContext.get().setReporter(reporter);

      if (localWork == null) {
        return;
      }

      //The following code is for mapjoin
      //initialize all the dummy ops
      l4j.info("Initializing dummy operator");
      List<Operator<? extends OperatorDesc>> dummyOps = localWork.getDummyParentOp();
      for (Operator<? extends OperatorDesc> dummyOp : dummyOps){
        dummyOp.setExecContext(execContext);
        dummyOp.initialize(jc,null);
      }
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

  public void process(Object value) throws IOException {
    // reset the execContext for each new row
    execContext.resetRow();

    try {
      if (mo.getDone()) {
        done = true;
      } else {
        // Since there is no concept of a group, we don't invoke
        // startGroup/endGroup for a mapper
        mo.process((Writable)value);
        if (isLogInfoEnabled) {
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

  public void close() {
    // No row was processed
    if (oc == null) {
      l4j.trace("Close called. no row processed by map.");
    }

    // check if there are IOExceptions
    if (!abort) {
      abort = execContext.getIoCxt().getIOExceptions();
    }

    // detecting failed executions by exceptions thrown by the operator tree
    // ideally hadoop should let us know whether map execution failed or not
    try {
      mo.close(abort);

      //for close the local work
      if(localWork != null){
        List<Operator<? extends OperatorDesc>> dummyOps = localWork.getDummyParentOp();

        for (Operator<? extends OperatorDesc> dummyOp : dummyOps){
          dummyOp.close(abort);
        }
      }

      if (isLogInfoEnabled) {
        long used_memory = memoryMXBean.getHeapMemoryUsage().getUsed();
        l4j.info("ExecMapper: processed " + numRows + " rows: used memory = "
          + used_memory);
      }

      ReportStats rps = new ReportStats(rp);
      mo.preorderMap(rps);
      return;
    } catch (Exception e) {
      if (!abort) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException("Hive Runtime Error while closing operators", e);
      }
    } finally {
      MapredContext.close();
      Utilities.clearWorkMap();
    }
  }

  public  boolean getDone() {
    return done;
  }

  public boolean isAbort() {
    return abort;
  }

  public void setAbort(boolean abort) {
    this.abort = abort;
  }

  public void setDone(boolean done) {
    this.done = done;
  }

  /**
   * reportStats.
   *
   */
  public static class ReportStats implements Operator.OperatorFunc {
    private final Reporter rp;

    public ReportStats(Reporter rp) {
      this.rp = rp;
    }

    public void func(Operator op) {
      Map<Enum<?>, Long> opStats = op.getStats();
      for (Map.Entry<Enum<?>, Long> e : opStats.entrySet()) {
        if (rp != null) {
          rp.incrCounter(e.getKey(), e.getValue());
        }
      }
    }
  }
}
