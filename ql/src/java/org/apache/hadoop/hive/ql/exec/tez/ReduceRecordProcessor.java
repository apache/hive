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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;

import org.apache.hadoop.hive.llap.LlapUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
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
import org.apache.hadoop.hive.ql.exec.tez.DynamicValueRegistryTez.RegistryConfTez;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor.TezKVOutputCollector;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.DynamicValue;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.api.Reader;

import com.google.common.collect.Lists;

/**
 * Process input from tez LogicalInput and write output - for a map plan
 * Just pump the records through the query plan.
 */
public class ReduceRecordProcessor extends RecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(ReduceRecordProcessor.class);

  private static final String REDUCE_PLAN_KEY = "__REDUCE_PLAN__";

  private final ObjectCache cache, dynamicValueCache;

  private ReduceWork reduceWork;

  private final List<BaseWork> mergeWorkList;
  private final List<String> cacheKeys;
  private final List<String> dynamicValueCacheKeys = new ArrayList<>();

  private final Map<Integer, DummyStoreOperator> connectOps = new TreeMap<>();
  private final Map<Integer, ReduceWork> tagToReducerMap = new HashMap<>();

  private Operator<?> reducer;

  private ReduceRecordSource[] sources;

  private byte bigTablePosition = 0;

  public ReduceRecordProcessor(final JobConf jconf, final ProcessorContext context) throws Exception {
    super(jconf, context);

    String queryId = HiveConf.getVar(jconf, HiveConf.ConfVars.HIVE_QUERY_ID);
    cache = ObjectCacheFactory.getCache(jconf, queryId, true);
    dynamicValueCache = ObjectCacheFactory.getCache(jconf, queryId, false, true);

    String cacheKey = processorContext.getTaskVertexName() + REDUCE_PLAN_KEY;
    cacheKeys = Lists.newArrayList(cacheKey);
    reduceWork = cache.retrieve(cacheKey, () -> Utilities.getReduceWork(jconf));

    Utilities.setReduceWork(jconf, reduceWork);
    mergeWorkList = getMergeWorkList(jconf, cacheKey, queryId, cache, cacheKeys);
  }

  @Override
  void init(MRTaskReporter mrReporter, Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs)
      throws Exception {
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.TEZ_INIT_OPERATORS);
    super.init(mrReporter, inputs, outputs);

    MapredContext.init(false, new JobConf(jconf));
    List<LogicalInput> shuffleInputs = getShuffleInputs(inputs);
    // TODO HIVE-14042. Move to using a loop and a timed wait once TEZ-3302 is fixed.
    checkAbortCondition();
    if (shuffleInputs != null) {
      LOG.info("Waiting for ShuffleInputs to become ready");
      processorContext.waitForAllInputsReady(new ArrayList<Input>(shuffleInputs));
    }

    connectOps.clear();
    ReduceWork redWork = reduceWork;
    LOG.info("Main work is " + reduceWork.getName());
    List<HashTableDummyOperator> workOps = reduceWork.getDummyOps();
    Set<HashTableDummyOperator> dummyOps = workOps == null ? new HashSet<>() : new HashSet<>(workOps);
    tagToReducerMap.put(redWork.getTag(), redWork);
    if (mergeWorkList != null) {
      for (BaseWork mergeWork : mergeWorkList) {
        LOG.debug("Additional work {}", mergeWork.getName());
        workOps = mergeWork.getDummyOps();
        if (workOps != null) {
          dummyOps.addAll(workOps);
        }
        ReduceWork mergeReduceWork = (ReduceWork) mergeWork;
        reducer = mergeReduceWork.getReducer();
        // Check immediately after reducer is assigned, in cae the abort came in during
        checkAbortCondition();
        DummyStoreOperator dummyStoreOp = getJoinParentOp(reducer);
        connectOps.put(mergeReduceWork.getTag(), dummyStoreOp);
        tagToReducerMap.put(mergeReduceWork.getTag(), mergeReduceWork);
      }

      ((TezContext) MapredContext.get()).setDummyOpsMap(connectOps);
    }
    checkAbortCondition();

    bigTablePosition = (byte) reduceWork.getTag();

    ObjectInspector[] mainWorkOIs = null;
    ((TezContext) MapredContext.get()).setInputs(inputs);
    ((TezContext) MapredContext.get()).setTezProcessorContext(processorContext);
    int numTags = reduceWork.getTagToValueDesc().size();
    reducer = reduceWork.getReducer();
    // Check immediately after reducer is assigned, in cae the abort came in during
    checkAbortCondition();
    // set memory available for operators
    long memoryAvailableToTask = processorContext.getTotalMemoryAvailableToTask();
    if (reducer.getConf() != null) {
      reducer.getConf().setMaxMemoryAvailable(memoryAvailableToTask);
      LOG.info("Memory available for operators set to {}", LlapUtil.humanReadableByteCount(memoryAvailableToTask));
    }
    OperatorUtils.setMemoryAvailable(reducer.getChildOperators(), memoryAvailableToTask);

    // Setup values registry
    String valueRegistryKey = DynamicValue.DYNAMIC_VALUE_REGISTRY_CACHE_KEY;
    DynamicValueRegistryTez registryTez =
        dynamicValueCache.retrieve(valueRegistryKey, () -> new DynamicValueRegistryTez());
    dynamicValueCacheKeys.add(valueRegistryKey);
    RegistryConfTez registryConf = new RegistryConfTez(jconf, reduceWork, processorContext, inputs);
    registryTez.init(registryConf);
    checkAbortCondition();

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
        // Check immediately after reducer is assigned, in cae the abort came in during
        checkAbortCondition();
        initializeSourceForTag(redWork, i, mainWorkOIs, sources, redWork.getTagToValueDesc().get(0),
            redWork.getTagToInput().get(0));
        reducer.initializeLocalWork(jconf);
      }
      reducer = reduceWork.getReducer();
      // Check immediately after reducer is assigned, in cae the abort came in during
      checkAbortCondition();
      ((TezContext) MapredContext.get()).setRecordSources(sources);
      reducer.initialize(jconf, new ObjectInspector[] {mainWorkOIs[bigTablePosition]});
      for (int i : tagToReducerMap.keySet()) {
        if (i == bigTablePosition) {
          continue;
        }
        redWork = tagToReducerMap.get(i);
        reducer = redWork.getReducer();
        // Check immediately after reducer is assigned, in cae the abort came in during
        checkAbortCondition();
        reducer.initialize(jconf, new ObjectInspector[] {mainWorkOIs[i]});
      }
    }
    checkAbortCondition();

    reducer = reduceWork.getReducer();

    // initialize reduce operator tree
    try {
      LOG.info(reducer.dump(0));

      // Initialization isn't finished until all parents of all operators
      // are initialized. For broadcast joins that means initializing the
      // dummy parent operators as well.
      for (HashTableDummyOperator dummyOp : dummyOps) {
        // TODO HIVE-14042. Propagating abort to dummyOps.
        dummyOp.initialize(jconf, null);
        checkAbortCondition();
      }

      // set output collector for any reduce sink operators in the pipeline.
      List<Operator<?>> children = new ArrayList<>();
      children.add(reducer);
      children.addAll(dummyOps);
      createOutputMap();
      OperatorUtils.setChildrenCollector(children, outMap);

      checkAbortCondition();
      reducer.setReporter(reporter);
      MapredContext.get().setReporter(reporter);

    } catch (Throwable e) {
      super.setAborted(true);
      if (e instanceof OutOfMemoryError) {
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else if (e instanceof InterruptedException) {
        LOG.info("Hit an interrupt while initializing ReduceRecordProcessor. Message={}", e.getMessage());
        throw (InterruptedException) e;
      } else {
        throw new RuntimeException(redWork.getName() + " operator initialization failed", e);
      }
    }

    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.TEZ_INIT_OPERATORS);
  }

  private void initializeMultipleSources(ReduceWork redWork, int numTags, ObjectInspector[] ois,
      ReduceRecordSource[] sources) throws Exception {
    for (int tag = 0; tag < redWork.getTagToValueDesc().size(); tag++) {
      if (redWork.getTagToValueDesc().get(tag) == null) {
        continue;
      }
      checkAbortCondition();
      initializeSourceForTag(redWork, tag, ois, sources, redWork.getTagToValueDesc().get(tag),
          redWork.getTagToInput().get(tag));
    }
  }

  private void initializeSourceForTag(ReduceWork redWork, int tag, ObjectInspector[] ois, ReduceRecordSource[] sources,
      TableDesc valueTableDesc, String inputName) throws Exception {
    reducer = redWork.getReducer();
    reducer.getParentOperators().clear();
    reducer.setParentOperators(null); // clear out any parents as reducer is the root

    TableDesc keyTableDesc = redWork.getKeyDesc();
    Reader reader = inputs.get(inputName).getReader();

    sources[tag] = new ReduceRecordSource();
    // Only the big table input source should be vectorized (if applicable)
    // Note this behavior may have to change if we ever implement a vectorized merge join
    boolean vectorizedRecordSource = (tag == bigTablePosition) && redWork.getVectorMode();
    sources[tag].init(jconf, redWork.getReducer(), vectorizedRecordSource, keyTableDesc, valueTableDesc, reader,
        tag == bigTablePosition, (byte) tag, redWork.getVectorizedRowBatchCtx(), redWork.getVectorizedVertexNum(),
        redWork.getVectorizedTestingReducerBatchSize());
    ois[tag] = sources[tag].getObjectInspector();
  }

  @Override
  void run() throws Exception {

    for (Entry<String, LogicalOutput> outputEntry : outputs.entrySet()) {
      LOG.info("Starting Output: " + outputEntry.getKey());
      if (!isAborted()) {
        outputEntry.getValue().start();
        ((TezKVOutputCollector) outMap.get(outputEntry.getKey())).initialize();
      }
    }

    // run the operator pipeline
    startAbortChecks();
    while (sources[bigTablePosition].pushRecord()) {
      addRowAndMaybeCheckAbort();
    }
  }

  @Override
  public void abort() {
    // this will stop run() from pushing records, along with potentially
    // blocking initialization.
    super.abort();

    if (reducer != null) {
      LOG.info("Forwarding abort to reducer: {} " + reducer.getName());
      reducer.abort();
    } else {
      LOG.info("reducer not setup yet. abort not being forwarded");
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
  void close() {
    if (cache != null) {
      for (String key : cacheKeys) {
        cache.release(key);
      }
    }

    if (dynamicValueCache != null) {
      for (String k : dynamicValueCacheKeys) {
        dynamicValueCache.release(k);
      }
    }

    try {
      if (isAborted()) {
        for (ReduceRecordSource rs : sources) {
          if (!rs.close()) {
            setAborted(false); // Preserving the old logic. Hmm...
            break;
          }
        }
      }

      boolean abort = isAborted();
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
      if (!isAborted()) {
        // signal new failure to map-reduce
        LOG.error("Hit error while closing operators - failing tree");
        throw new RuntimeException("Hive Runtime Error while closing operators: " + e.getMessage(), e);
      }
    } finally {
      Utilities.clearWorkMap(jconf);
      MapredContext.close();
    }
  }

  private DummyStoreOperator getJoinParentOp(Operator<?> mergeReduceOp) {
    for (Operator<?> childOp : mergeReduceOp.getChildOperators()) {
      if ((childOp.getChildOperators() == null) || (childOp.getChildOperators().isEmpty())) {
        if (childOp instanceof DummyStoreOperator) {
          return (DummyStoreOperator) childOp;
        } else {
          throw new IllegalStateException("Was expecting dummy store operator but found: " + childOp);
        }
      } else {
        return getJoinParentOp(childOp);
      }
    }
    throw new IllegalStateException("Expecting a DummyStoreOperator found op: " + mergeReduceOp);
  }
}
