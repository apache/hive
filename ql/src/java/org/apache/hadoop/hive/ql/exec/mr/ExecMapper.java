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

package org.apache.hadoop.hive.ql.exec.mr;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.ql.exec.AbstractMapOperator;
import org.apache.hadoop.hive.ql.exec.MapOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapOperator;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.MapredLocalWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ExecMapper is the generic Map class for Hive. Together with ExecReducer it is
 * the bridge between the map-reduce framework and the Hive operator pipeline at
 * execution time. It's main responsibilities are:
 *
 * - Load and setup the operator pipeline from XML
 * - Run the pipeline by transforming key value pairs to records and forwarding them to the operators
 * - Stop execution when the "limit" is reached
 * - Catch and handle errors during execution of the operators.
 *
 */
public class ExecMapper extends MapReduceBase implements Mapper {

  private AbstractMapOperator mo;
  private OutputCollector oc;
  private JobConf jc;
  private boolean abort = false;
  private Reporter rp;
  public static final Logger l4j = LoggerFactory.getLogger(ExecMapper.class);
  private static boolean done;

  private MapredLocalWork localWork = null;
  private ExecMapperContext execContext = null;

  @Override
  public void configure(JobConf job) {
    execContext = new ExecMapperContext(job);
    Utilities.tryLoggingClassPaths(job, l4j);
    setDone(false);

    try {
      jc = job;
      execContext.setJc(jc);

      // create map and fetch operators
      MapWork mrwork = Utilities.getMapWork(job);
      for (PartitionDesc part : mrwork.getAliasToPartnInfo().values()) {
        TableDesc tableDesc = part.getTableDesc();
        Utilities.copyJobSecretToTableProperties(tableDesc);
      }

      CompilationOpContext runtimeCtx = new CompilationOpContext();
      if (mrwork.getVectorMode()) {
        mo = new VectorMapOperator(runtimeCtx);
      } else {
        mo = new MapOperator(runtimeCtx);
      }
      mo.setConf(mrwork);
      // initialize map operator
      mo.initialize(job, null);
      mo.setChildren(job);
      l4j.info(mo.dump(0));
      // initialize map local work
      localWork = mrwork.getMapRedLocalWork();
      execContext.setLocalWork(localWork);

      MapredContext.init(true, new JobConf(jc));

      mo.passExecContext(execContext);
      mo.initializeLocalWork(jc);
      mo.initializeMapOperator(jc);

      if (localWork == null) {
        return;
      }

      //The following code is for mapjoin
      //initialize all the dummy ops
      l4j.info("Initializing dummy operator");
      List<Operator<? extends OperatorDesc>> dummyOps = localWork.getDummyParentOp();
      for (Operator<? extends OperatorDesc> dummyOp : dummyOps){
        dummyOp.passExecContext(execContext);
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
  @Override
  public void map(Object key, Object value, OutputCollector output,
      Reporter reporter) throws IOException {
    ensureOutputInitialize(output, reporter);
    // reset the execContext for each new row
    execContext.resetRow();

    try {
      if (mo.getDone()) {
        done = true;
      } else {
        // Since there is no concept of a group, we don't invoke
        // startGroup/endGroup for a mapper
        mo.process((Writable)value);
      }
    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        l4j.error(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
    }
  }

  public void ensureOutputInitialize(OutputCollector output, Reporter reporter) {
    if (oc == null) {
      oc = output;
      rp = reporter;
      OperatorUtils.setChildrenCollector(mo.getChildOperators(), output);
      mo.setReporter(rp);
      MapredContext.get().setReporter(reporter);
    }
  }

  @Override
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

      ReportStats rps = new ReportStats(rp, jc);
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
      Utilities.clearWorkMap(jc);
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
  public static class ReportStats implements Operator.OperatorFunc {
    private final Reporter rp;
    private final String groupName;

    public ReportStats(Reporter rp, Configuration conf) {
      this.rp = rp;
      this.groupName = HiveConf.getVar(conf, HiveConf.ConfVars.HIVE_COUNTER_GROUP);
    }

    @Override
    public void func(Operator op) {
      Map<String, Long> opStats = op.getStats();
      for (Map.Entry<String, Long> e : opStats.entrySet()) {
        if (rp != null) {
          rp.incrCounter(groupName, e.getKey(), e.getValue());
        }
      }
    }
  }
}
