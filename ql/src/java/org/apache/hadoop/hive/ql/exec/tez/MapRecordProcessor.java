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
import org.apache.hadoop.hive.ql.exec.MapOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper.reportStats;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.util.StringUtils;
import org.apache.tez.mapreduce.input.MRInput;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.library.api.KeyValueReader;

/**
 * Process input from tez LogicalInput and write output - for a map plan
 * Just pump the records through the query plan.
 */
public class MapRecordProcessor  extends RecordProcessor{


  private MapOperator mapOp;
  public static final Log l4j = LogFactory.getLog(MapRecordProcessor.class);
  private final ExecMapperContext execContext = new ExecMapperContext();
  private boolean abort = false;
  protected static final String MAP_PLAN_KEY = "__MAP_PLAN__";

  @Override
  void init(JobConf jconf, MRTaskReporter mrReporter, Map<String, LogicalInput> inputs,
      OutputCollector out){
    super.init(jconf, mrReporter, inputs, out);

    //Update JobConf using MRInput, info like filename comes via this
    MRInput mrInput = getMRInput(inputs);
    Configuration updatedConf = mrInput.getConfigUpdates();
    if (updatedConf != null) {
      for (Entry<String, String> entry : updatedConf) {
        jconf.set(entry.getKey(), entry.getValue());
      }
    }

    ObjectCache cache = ObjectCacheFactory.getCache(jconf);
    try {

      execContext.setJc(jconf);
      // create map and fetch operators
      MapWork mrwork = (MapWork) cache.retrieve(MAP_PLAN_KEY);
      if (mrwork == null) {
        mrwork = Utilities.getMapWork(jconf);
        cache.cache(MAP_PLAN_KEY, mrwork);
      }
      mapOp = new MapOperator();

      // initialize map operator
      mapOp.setConf(mrwork);
      mapOp.setChildren(jconf);
      l4j.info(mapOp.dump(0));

      MapredContext.init(true, new JobConf(jconf));
      ((TezContext)MapredContext.get()).setInputs(inputs);
      mapOp.setExecContext(execContext);
      mapOp.initializeLocalWork(jconf);
      mapOp.initialize(jconf, null);

      mapOp.setOutputCollector(out);
      mapOp.setReporter(reporter);
      MapredContext.get().setReporter(reporter);

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

  private MRInput getMRInput(Map<String, LogicalInput> inputs) {
    //there should be only one MRInput
    MRInput theMRInput = null;
    for(LogicalInput inp : inputs.values()){
      if(inp instanceof MRInput){
        if(theMRInput != null){
          throw new IllegalArgumentException("Only one MRInput is expected");
        }
        //a better logic would be to find the alias
        theMRInput = (MRInput)inp;
      }
    }
    return theMRInput;
  }

  @Override
  void run() throws IOException{
    if (inputs.size() != 1) {
      throw new IllegalArgumentException("MapRecordProcessor expects single input"
          + ", inputCount=" + inputs.size());
    }

    MRInput in = getMRInput(inputs);
    KeyValueReader reader = in.getReader();

    //process records until done
    while(reader.next()){
      //ignore the key for maps -  reader.getCurrentKey();
      Object value = reader.getCurrentValue();
      boolean needMore = processRow(value);
      if(!needMore){
        break;
      }
    }
  }


  /**
   * @param value  value to process
   * @return true if it is not done and can take more inputs
   */
  private boolean processRow(Object value) {
    // reset the execContext for each new row
    execContext.resetRow();

    try {
      if (mapOp.getDone()) {
        return false; //done
      } else {
        // Since there is no concept of a group, we don't invoke
        // startGroup/endGroup for a mapper
        mapOp.process((Writable)value);
        if (isLogInfoEnabled) {
          logProgress();
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
    return true; //give me more
  }

  @Override
  void close(){
    // check if there are IOExceptions
    if (!abort) {
      abort = execContext.getIoCxt().getIOExceptions();
    }

    // detecting failed executions by exceptions thrown by the operator tree
    try {
      mapOp.close(abort);
      if (isLogInfoEnabled) {
        logCloseInfo();
      }
      reportStats rps = new reportStats(reporter);
      mapOp.preorderMap(rps);
      return;
    } catch (Exception e) {
      if (!abort) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException("Hive Runtime Error while closing operators", e);
      }
    } finally {
      MapredContext.close();
    }
  }

}
