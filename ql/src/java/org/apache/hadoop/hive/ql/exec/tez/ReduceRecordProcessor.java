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
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.concurrent.Callable;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
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
import org.apache.hadoop.hive.ql.plan.BaseWork;
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

  private ObjectCache cache;

  private String cacheKey;

  public static final Log l4j = LogFactory.getLog(ReduceRecordProcessor.class);

  private ReduceWork reduceWork;

  List<BaseWork> mergeWorkList = null;
  List<String> cacheKeys;

  private final Map<Integer, DummyStoreOperator> connectOps =
      new TreeMap<Integer, DummyStoreOperator>();
  private final Map<Integer, ReduceWork> tagToReducerMap = new HashMap<Integer, ReduceWork>();

  private Operator<?> reducer;

  private ReduceRecordSource[] sources;

  private byte bigTablePosition = 0;

  private boolean abort;

  public ReduceRecordProcessor(final JobConf jconf, final ProcessorContext context) throws Exception {
    super(jconf, context);

    ObjectCache cache = ObjectCacheFactory.getCache(jconf);

    String queryId = HiveConf.getVar(jconf, HiveConf.ConfVars.HIVEQUERYID);
    cacheKey = queryId + REDUCE_PLAN_KEY;
    cacheKeys = new ArrayList<String>();
    cacheKeys.add(cacheKey);
    reduceWork = (ReduceWork) cache.retrieve(cacheKey, new Callable<Object>() {
        @Override
        public Object call() {
          return Utilities.getReduceWork(jconf);
      }
    });

    Utilities.setReduceWork(jconf, reduceWork);
    mergeWorkList = getMergeWorkList(jconf, cacheKey, queryId, cache, cacheKeys);
  }

  @Override
  void init(
      MRTaskReporter mrReporter, Map<String, LogicalInput> inputs,
      Map<String, LogicalOutput> outputs) throws Exception {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.TEZ_INIT_OPERATORS);
    super.init(mrReporter, inputs, outputs);

    MapredContext.init(false, new JobConf(jconf));
    List<LogicalInput> shuffleInputs = getShuffleInputs(inputs);
    if (shuffleInputs != null) {
      l4j.info("Waiting for ShuffleInputs to become ready");
      processorContext.waitForAllInputsReady(new ArrayList<Input>(shuffleInputs));
    }

    connectOps.clear();
    ReduceWork redWork = reduceWork;
    tagToReducerMap.put(redWork.getTag(), redWork);
    if (mergeWorkList != null) {
      for (BaseWork mergeWork : mergeWorkList) {
        ReduceWork mergeReduceWork = (ReduceWork) mergeWork;
        reducer = mergeReduceWork.getReducer();
        DummyStoreOperator dummyStoreOp = getJoinParentOp(reducer);
        connectOps.put(mergeReduceWork.getTag(), dummyStoreOp);
        tagToReducerMap.put(mergeReduceWork.getTag(), mergeReduceWork);
      }

      bigTablePosition = (byte) reduceWork.getTag();
      ((TezContext) MapredContext.get()).setDummyOpsMap(connectOps);
    }

    ObjectInspector[] mainWorkOIs = null;
    ((TezContext) MapredContext.get()).setInputs(inputs);
    ((TezContext) MapredContext.get()).setTezProcessorContext(processorContext);
    int numTags = reduceWork.getTagToValueDesc().size();
    reducer = reduceWork.getReducer();
    if (numTags > 1) {
      sources = new ReduceRecordSource[numTags];
      mainWorkOIs = new ObjectInspector[numTags];
      initializeMultipleSources(reduceWork, numTags, mainWorkOIs, sources);
      ((TezContext) MapredContext.get()).setRecordSources(sources);
      reducer.initialize(jconf, mainWorkOIs);
    } else {
      numTags = tagToReducerMap.keySet().size();
      sources = new ReduceRecordSource[numTags];
      mainWorkOIs = new ObjectInspector[numTags];
      for (int i : tagToReducerMap.keySet()) {
        redWork = tagToReducerMap.get(i);
        reducer = redWork.getReducer();
        initializeSourceForTag(redWork, i, mainWorkOIs, sources,
            redWork.getTagToValueDesc().get(0), redWork.getTagToInput().get(0));
        reducer.initializeLocalWork(jconf);
      }
      reducer = reduceWork.getReducer();
      ((TezContext) MapredContext.get()).setRecordSources(sources);
      reducer.initialize(jconf, new ObjectInspector[] { mainWorkOIs[bigTablePosition] });
      for (int i : tagToReducerMap.keySet()) {
        if (i == bigTablePosition) {
          continue;
        }
        redWork = tagToReducerMap.get(i);
        reducer = redWork.getReducer();
        reducer.initialize(jconf, new ObjectInspector[] { mainWorkOIs[i] });
      }
    }

    reducer = reduceWork.getReducer();
    // initialize reduce operator tree
    try {
      l4j.info(reducer.dump(0));

      // Initialization isn't finished until all parents of all operators
      // are initialized. For broadcast joins that means initializing the
      // dummy parent operators as well.
      List<HashTableDummyOperator> dummyOps = redWork.getDummyOps();
      if (dummyOps != null) {
        for (HashTableDummyOperator dummyOp : dummyOps) {
          dummyOp.initialize(jconf, null);
        }
      }

      // set output collector for any reduce sink operators in the pipeline.
      List<Operator<?>> children = new LinkedList<Operator<?>>();
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

  private void initializeMultipleSources(ReduceWork redWork, int numTags, ObjectInspector[] ois,
      ReduceRecordSource[] sources) throws Exception {
    for (int tag = 0; tag < redWork.getTagToValueDesc().size(); tag++) {
      if (redWork.getTagToValueDesc().get(tag) == null) {
        continue;
      }
      initializeSourceForTag(redWork, tag, ois, sources, redWork.getTagToValueDesc().get(tag),
          redWork.getTagToInput().get(tag));
    }
  }

  private void initializeSourceForTag(ReduceWork redWork, int tag, ObjectInspector[] ois,
      ReduceRecordSource[] sources, TableDesc valueTableDesc, String inputName)
      throws Exception {
    reducer = redWork.getReducer();
    reducer.getParentOperators().clear();
    reducer.setParentOperators(null); // clear out any parents as reducer is the root

    TableDesc keyTableDesc = redWork.getKeyDesc();
    KeyValuesReader reader = (KeyValuesReader) inputs.get(inputName).getReader();

    sources[tag] = new ReduceRecordSource();
    sources[tag].init(jconf, redWork.getReducer(), redWork.getVectorMode(), keyTableDesc,
        valueTableDesc, reader, tag == bigTablePosition, (byte) tag,
        redWork.getVectorScratchColumnTypeMap());
    ois[tag] = sources[tag].getObjectInspector();
  }

  @Override
  void run() throws Exception {

    for (Entry<String, LogicalOutput> outputEntry : outputs.entrySet()) {
      l4j.info("Starting Output: " + outputEntry.getKey());
      outputEntry.getValue().start();
      ((TezKVOutputCollector) outMap.get(outputEntry.getKey())).initialize();
    }

    // run the operator pipeline
    while (sources[bigTablePosition].pushRecord()) {
    }
  }

  /**
   * Get the inputs that should be streamed through reduce plan.
   *
   * @param inputs
   * @return
   * @throws Exception
   */
  private List<LogicalInput> getShuffleInputs(Map<String, LogicalInput> inputs) throws Exception {
    // the reduce plan inputs have tags, add all inputs that have tags
    Map<Integer, String> tagToinput = reduceWork.getTagToInput();
    ArrayList<LogicalInput> shuffleInputs = new ArrayList<LogicalInput>();
    for (String inpStr : tagToinput.values()) {
      if (inputs.get(inpStr) == null) {
        throw new AssertionError("Cound not find input: " + inpStr);
      }
      inputs.get(inpStr).start();
      shuffleInputs.add(inputs.get(inpStr));
    }
    return shuffleInputs;
  }

  @Override
  void close(){
    if (cache != null && cacheKeys != null) {
      for (String key : cacheKeys) {
        cache.release(key);
      }
    }

    try {
      for (ReduceRecordSource rs: sources) {
        abort = abort && rs.close();
      }

      reducer.close(abort);
      if (mergeWorkList != null) {
        for (BaseWork redWork : mergeWorkList) {
          ((ReduceWork) redWork).getReducer().close(abort);
        }
      }

      // Need to close the dummyOps as well. The operator pipeline
      // is not considered "closed/done" unless all operators are
      // done. For broadcast joins that includes the dummy parents.
      List<HashTableDummyOperator> dummyOps = reduceWork.getDummyOps();
      if (dummyOps != null) {
        for (Operator<?> dummyOp : dummyOps) {
          dummyOp.close(abort);
        }
      }
      ReportStats rps = new ReportStats(reporter, jconf);
      reducer.preorderMap(rps);

    } catch (Exception e) {
      if (!abort) {
        // signal new failure to map-reduce
        l4j.error("Hit error while closing operators - failing tree");
        throw new RuntimeException(
            "Hive Runtime Error while closing operators: " + e.getMessage(), e);
      }
    } finally {
      Utilities.clearWorkMap();
      MapredContext.close();
    }
  }

  private DummyStoreOperator getJoinParentOp(Operator<?> mergeReduceOp) {
    for (Operator<?> childOp : mergeReduceOp.getChildOperators()) {
      if ((childOp.getChildOperators() == null) || (childOp.getChildOperators().isEmpty())) {
        if (childOp instanceof DummyStoreOperator) {
          return (DummyStoreOperator) childOp;
        } else {
          throw new IllegalStateException("Was expecting dummy store operator but found: "
              + childOp);
        }
      } else {
        return getJoinParentOp(childOp);
      }
    }
    throw new IllegalStateException("Expecting a DummyStoreOperator found op: " + mergeReduceOp);
  }
}
