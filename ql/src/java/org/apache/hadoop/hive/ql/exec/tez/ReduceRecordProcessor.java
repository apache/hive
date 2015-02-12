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

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper.ReportStats;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor.TezKVOutputCollector;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValuesReader;

/**
 * Process input from tez LogicalInput and write output - for a map plan
 * Just pump the records through the query plan.
 */
public class ReduceRecordProcessor  extends RecordProcessor{

  private static final String REDUCE_PLAN_KEY = "__REDUCE_PLAN__";

  public static final Log l4j = LogFactory.getLog(ReduceRecordProcessor.class);

  private ReduceWork redWork;

  private Operator<?> reducer;

  private ReduceRecordSource[] sources;

  private final byte position = 0;

  private boolean abort;

  @Override
  void init(JobConf jconf, ProcessorContext processorContext, MRTaskReporter mrReporter,
      Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_INIT_OPERATORS);
    super.init(jconf, processorContext, mrReporter, inputs, outputs);

    ObjectCache cache = ObjectCacheFactory.getCache(jconf);

    redWork = (ReduceWork) cache.retrieve(REDUCE_PLAN_KEY);
    if (redWork == null) {
      redWork = Utilities.getReduceWork(jconf);
      cache.cache(REDUCE_PLAN_KEY, redWork);
    } else {
      Utilities.setReduceWork(jconf, redWork);
    }

    reducer = redWork.getReducer();
    reducer.getParentOperators().clear();
    reducer.setParentOperators(null); // clear out any parents as reducer is the root

    int numTags = redWork.getTagToValueDesc().size();

    ObjectInspector[] ois = new ObjectInspector[numTags];
    sources = new ReduceRecordSource[numTags];

    for (int tag = 0; tag < redWork.getTagToValueDesc().size(); tag++) {
      TableDesc keyTableDesc = redWork.getKeyDesc();
      TableDesc valueTableDesc = redWork.getTagToValueDesc().get(tag);
      KeyValuesReader reader =
          (KeyValuesReader) inputs.get(redWork.getTagToInput().get(tag)).getReader();

      sources[tag] = new ReduceRecordSource();
      sources[tag].init(jconf, reducer, redWork.getVectorMode(), keyTableDesc, valueTableDesc,
          reader, tag == position, (byte) tag,
          redWork.getAllScratchColumnVectorTypeMaps());
      ois[tag] = sources[tag].getObjectInspector();
    }

    MapredContext.init(false, new JobConf(jconf));
    ((TezContext) MapredContext.get()).setInputs(inputs);
    ((TezContext) MapredContext.get()).setTezProcessorContext(processorContext);
    ((TezContext) MapredContext.get()).setRecordSources(sources);

    // initialize reduce operator tree
    try {
      l4j.info(reducer.dump(0));
      reducer.initialize(jconf, ois);

      // Initialization isn't finished until all parents of all operators
      // are initialized. For broadcast joins that means initializing the
      // dummy parent operators as well.
      List<HashTableDummyOperator> dummyOps = redWork.getDummyOps();
      if (dummyOps != null) {
        for (Operator<? extends OperatorDesc> dummyOp : dummyOps){
          dummyOp.initialize(jconf, null);
        }
      }

      // set output collector for any reduce sink operators in the pipeline.
      List<Operator<? extends OperatorDesc>> children = new LinkedList<Operator<? extends OperatorDesc>>();
      children.add(reducer);
      if (dummyOps != null) {
        children.addAll(dummyOps);
      }
      createOutputMap();
      OperatorUtils.setChildrenCollector(children, outMap);

      reducer.setReporter(reporter);
      MapredContext.get().setReporter(reporter);

    } catch (Throwable e) {
      abort = true;
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else {
        throw new RuntimeException("Reduce operator initialization failed", e);
      }
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.TEZ_INIT_OPERATORS);
  }

  @Override
  void run() throws Exception {
    List<LogicalInput> shuffleInputs = getShuffleInputs(inputs);
    if (shuffleInputs != null) {
      l4j.info("Waiting for ShuffleInputs to become ready");
      processorContext.waitForAllInputsReady(new ArrayList<Input>(shuffleInputs));
    }

    for (Entry<String, LogicalOutput> outputEntry : outputs.entrySet()) {
      l4j.info("Starting Output: " + outputEntry.getKey());
      outputEntry.getValue().start();
      ((TezKVOutputCollector) outMap.get(outputEntry.getKey())).initialize();
    }

    // run the operator pipeline
    while (sources[position].pushRecord()) {}
  }

  /**
   * Get the inputs that should be streamed through reduce plan.
   * @param inputs
   * @return
   */
  private List<LogicalInput> getShuffleInputs(Map<String, LogicalInput> inputs) {
    //the reduce plan inputs have tags, add all inputs that have tags
    Map<Integer, String> tagToinput = redWork.getTagToInput();
    ArrayList<LogicalInput> shuffleInputs = new ArrayList<LogicalInput>();
    for(String inpStr : tagToinput.values()){
      if (inputs.get(inpStr) == null) {
        throw new AssertionError("Cound not find input: " + inpStr);
      }
      shuffleInputs.add(inputs.get(inpStr));
    }
    return shuffleInputs;
  }

  @Override
  void close(){
    try {
      for (ReduceRecordSource rs: sources) {
        abort = abort && rs.close();
      }

      reducer.close(abort);

      // Need to close the dummyOps as well. The operator pipeline
      // is not considered "closed/done" unless all operators are
      // done. For broadcast joins that includes the dummy parents.
      List<HashTableDummyOperator> dummyOps = redWork.getDummyOps();
      if (dummyOps != null) {
        for (Operator<? extends OperatorDesc> dummyOp : dummyOps){
          dummyOp.close(abort);
        }
      }
      ReportStats rps = new ReportStats(reporter, jconf);
      reducer.preorderMap(rps);

    } catch (Exception e) {
      if (!abort) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException("Hive Runtime Error while closing operators: "
            + e.getMessage(), e);
      }
    } finally {
      Utilities.clearWorkMap();
      MapredContext.close();
    }
  }

}
