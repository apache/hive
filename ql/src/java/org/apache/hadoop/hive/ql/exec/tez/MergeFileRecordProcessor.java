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
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.conf.Configuration;
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
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;

/**
 * Record processor for fast merging of files.
 */
public class MergeFileRecordProcessor extends RecordProcessor {

  public static final Log LOG = LogFactory
      .getLog(MergeFileRecordProcessor.class);

  protected Operator<? extends OperatorDesc> mergeOp;
  private ExecMapperContext execContext = null;
  protected static final String MAP_PLAN_KEY = "__MAP_PLAN__";
  private MergeFileWork mfWork;
  MRInputLegacy mrInput = null;
  private boolean abort = false;
  private final Object[] row = new Object[2];

  @Override
  void init(JobConf jconf, ProcessorContext processorContext,
      MRTaskReporter mrReporter, Map<String, LogicalInput> inputs,
      Map<String, LogicalOutput> outputs) throws Exception {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_INIT_OPERATORS);
    super.init(jconf, processorContext, mrReporter, inputs, outputs);
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

    org.apache.hadoop.hive.ql.exec.ObjectCache cache = ObjectCacheFactory
        .getCache(jconf);
    try {
      execContext.setJc(jconf);
      // create map and fetch operators
      MapWork mapWork = (MapWork) cache.retrieve(MAP_PLAN_KEY);
      if (mapWork == null) {
        mapWork = Utilities.getMapWork(jconf);
        cache.cache(MAP_PLAN_KEY, mapWork);
      } else {
        Utilities.setMapWork(jconf, mapWork);
      }

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
      mergeOp.setExecContext(execContext);
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
      if (!needMore) {
        break;
      }
    }
  }

  @Override
  void close() {
    // check if there are IOExceptions
    if (!abort) {
      abort = execContext.getIoCxt().getIOExceptions();
    }

    // detecting failed executions by exceptions thrown by the operator tree
    try {
      if (mergeOp == null || mfWork == null) {
        return;
      }
      mergeOp.close(abort);

      ExecMapper.ReportStats rps = new ExecMapper.ReportStats(reporter, jconf);
      mergeOp.preorderMap(rps);
    } catch (Exception e) {
      if (!abort) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException("Hive Runtime Error while closing operators",
            e);
      }
    } finally {
      Utilities.clearWorkMap();
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
        mergeOp.processOp(row, 0);
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
    return true; //give me more
  }

  private MRInputLegacy getMRInput(Map<String, LogicalInput> inputs) throws Exception {
    // there should be only one MRInput
    MRInputLegacy theMRInput = null;
    LOG.info("VDK: the inputs are: " + inputs);
    for (Entry<String, LogicalInput> inp : inputs.entrySet()) {
      if (inp.getValue() instanceof MRInputLegacy) {
        if (theMRInput != null) {
          throw new IllegalArgumentException("Only one MRInput is expected");
        }
        // a better logic would be to find the alias
        theMRInput = (MRInputLegacy) inp.getValue();
      } else {
        throw new IOException("Expecting only one input of type MRInputLegacy. Found type: "
            + inp.getClass().getCanonicalName());
      }
    }
    theMRInput.init();

    return theMRInput;
  }
}
