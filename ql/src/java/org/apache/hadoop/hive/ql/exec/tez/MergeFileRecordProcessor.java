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

import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.io.merge.MergeFileWork;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;

import com.google.common.collect.Lists;

/**
 * Record processor for fast merging of files.
 */
public class MergeFileRecordProcessor extends RecordProcessor {

  public static final Logger LOG = LoggerFactory
      .getLogger(MergeFileRecordProcessor.class);

  protected Operator<? extends OperatorDesc> mergeOp;
  private ExecMapperContext execContext = null;
  protected static final String MAP_PLAN_KEY = "__MAP_PLAN__";
  private String cacheKey;
  private MergeFileWork mfWork;
  MRInputLegacy mrInput = null;
  private final Object[] row = new Object[2];
  org.apache.hadoop.hive.ql.exec.ObjectCache cache;

  public MergeFileRecordProcessor(final JobConf jconf, final ProcessorContext context) {
    super(jconf, context);
  }

  @Override
  void init(
      MRTaskReporter mrReporter, Map<String, LogicalInput> inputs,
      Map<String, LogicalOutput> outputs) throws Exception {
    // TODO HIVE-14042. Abort handling.
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_INIT_OPERATORS);
    super.init(mrReporter, inputs, outputs);
    execContext = new ExecMapperContext(jconf);

    //Update JobConf using MRInput, info like filename comes via this
    mrInput = getMRInput(inputs);
    Configuration updatedConf = mrInput.getConfigUpdates();
    if (updatedConf != null) {
      for (Map.Entry<String, String> entry : updatedConf) {
        jconf.set(entry.getKey(), entry.getValue());
      }
    }
    createOutputMap();
    // Start all the Outputs.
    for (Map.Entry<String, LogicalOutput> outputEntry : outputs.entrySet()) {
      outputEntry.getValue().start();
      ((TezProcessor.TezKVOutputCollector) outMap.get(outputEntry.getKey()))
          .initialize();
    }

    String queryId = HiveConf.getVar(jconf, HiveConf.ConfVars.HIVEQUERYID);
    cache = ObjectCacheFactory.getCache(jconf, queryId, true);

    try {
      execContext.setJc(jconf);

      cacheKey = MAP_PLAN_KEY;

      MapWork mapWork = (MapWork) cache.retrieve(cacheKey, new Callable<Object>() {
        @Override
        public Object call() {
          return Utilities.getMapWork(jconf);
        }
      });
      Utilities.setMapWork(jconf, mapWork);

      if (mapWork instanceof MergeFileWork) {
        mfWork = (MergeFileWork) mapWork;
      } else {
        throw new RuntimeException("MapWork should be an instance of MergeFileWork.");
      }

      String alias = mfWork.getAliasToWork().keySet().iterator().next();
      mergeOp = mfWork.getAliasToWork().get(alias);
      LOG.info(mergeOp.dump(0));

      MapredContext.init(true, new JobConf(jconf));
      ((TezContext) MapredContext.get()).setInputs(inputs);
      mergeOp.passExecContext(execContext);
      mergeOp.initializeLocalWork(jconf);
      mergeOp.initialize(jconf, null);

      OperatorUtils.setChildrenCollector(mergeOp.getChildOperators(), outMap);
      mergeOp.setReporter(reporter);
      MapredContext.get().setReporter(reporter);
    } catch (Throwable e) {
      if (e instanceof OutOfMemoryError) {
        // will this be true here?
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else if (e instanceof InterruptedException) {
        l4j.info("Hit an interrupt while initializing MergeFileRecordProcessor. Message={}",
            e.getMessage());
        throw (InterruptedException) e;
      } else {
        throw new RuntimeException("Map operator initialization failed", e);
      }
    }
    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_INIT_OPERATORS);
  }

  @Override
  void run() throws Exception {
    KeyValueReader reader = mrInput.getReader();

    //process records until done
    while (reader.next()) {
      boolean needMore = processRow(reader.getCurrentKey(),
          reader.getCurrentValue());
      if (!needMore || isAborted()) {
        break;
      }
    }
  }

  @Override
  void close() {

    if (cache != null && cacheKey != null) {
      cache.release(cacheKey);
    }

    // check if there are IOExceptions
    if (!isAborted()) {
      setAborted(execContext.getIoCxt().getIOExceptions());
    }

    // detecting failed executions by exceptions thrown by the operator tree
    try {
      if (mergeOp == null || mfWork == null) {
        return;
      }
      boolean abort = isAborted();
      mergeOp.close(abort);

      ExecMapper.ReportStats rps = new ExecMapper.ReportStats(reporter, jconf);
      mergeOp.preorderMap(rps);
    } catch (Exception e) {
      if (!isAborted()) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException("Hive Runtime Error while closing operators",
            e);
      }
    } finally {
      Utilities.clearWorkMap(jconf);
      MapredContext.close();
    }
  }

  /**
   * @param key   key to process
   * @param value value to process
   * @return true if it is not done and can take more inputs
   */
  private boolean processRow(Object key, Object value) {
    // reset the execContext for each new row
    execContext.resetRow();

    try {
      if (mergeOp.getDone()) {
        return false; //done
      } else {
        row[0] = key;
        row[1] = value;
        mergeOp.process(row, 0);
      }
    } catch (Throwable e) {
      setAborted(true);
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        l4j.error(StringUtils.stringifyException(e));
        throw new RuntimeException(e);
      }
    }
    return true; //give me more
  }

  private MRInputLegacy getMRInput(Map<String, LogicalInput> inputs) throws Exception {
    LOG.info("The inputs are: " + inputs);

    // start the mr input and wait for ready event. number of MRInput is expected to be 1
    List<Input> li = Lists.newArrayList();
    int numMRInputs = 0;
    for (LogicalInput inp : inputs.values()) {
      if (inp instanceof MRInputLegacy) {
        numMRInputs++;
        if (numMRInputs > 1) {
          throw new IllegalArgumentException("Only one MRInput is expected");
        }
        inp.start();
        li.add(inp);
      } else {
        throw new IllegalArgumentException("Expecting only one input of type MRInputLegacy." +
            " Found type: " + inp.getClass().getCanonicalName());
      }
    }

    // typically alter table .. concatenate is run on only one partition/one table,
    // so it doesn't matter if we wait for all inputs or any input to be ready.
    processorContext.waitForAnyInputReady(li);

    final MRInputLegacy theMRInput;
    if (li.size() == 1) {
      theMRInput = (MRInputLegacy) li.get(0);
      theMRInput.init();
    } else {
      throw new IllegalArgumentException("MRInputs count is expected to be 1");
    }

    return theMRInput;
  }
}
