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

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.hadoop.hive.llap.LlapUtil;
import org.apache.hadoop.hive.ql.plan.PartitionDesc;
import org.apache.hadoop.hive.ql.plan.TableDesc;
import org.apache.hadoop.hive.ql.txn.compactor.CompactorUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.AbstractMapOperator;
import org.apache.hadoop.hive.llap.io.api.LlapProxy;
import org.apache.hadoop.hive.llap.tez.Converters;
import org.apache.hadoop.hive.ql.CompilationOpContext;
import org.apache.hadoop.hive.llap.LlapOutputFormat;
import org.apache.hadoop.hive.ql.exec.DummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.HashTableDummyOperator;
import org.apache.hadoop.hive.ql.exec.MapOperator;
import org.apache.hadoop.hive.ql.exec.MapredContext;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.OperatorUtils;
import org.apache.hadoop.hive.ql.exec.TezDummyStoreOperator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper.ReportStats;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapperContext;
import org.apache.hadoop.hive.ql.exec.tez.DynamicValueRegistryTez.RegistryConfTez;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor.TezKVOutputCollector;
import org.apache.hadoop.hive.ql.exec.tez.tools.KeyValueInputMerger;
import org.apache.hadoop.hive.ql.exec.vector.VectorMapOperator;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.DynamicValue;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.OperatorDesc;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.mapred.JobConf;
import org.apache.tez.mapreduce.input.MRInputLegacy;
import org.apache.tez.mapreduce.input.MultiMRInput;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.Input;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.apache.tez.runtime.library.api.KeyValueReader;

/**
 * Process input from tez LogicalInput and write output - for a map plan
 * Just pump the records through the query plan.
 */
public class MapRecordProcessor extends RecordProcessor {
  private static final Logger LOG = LoggerFactory.getLogger(MapRecordProcessor.class);
  private static final String MAP_PLAN_KEY = "__MAP_PLAN__";

  private AbstractMapOperator mapOp;
  private final List<AbstractMapOperator> mergeMapOpList = new ArrayList<>();
  private MapRecordSource[] sources;
  private final Map<String, MultiMRInput> multiMRInputMap = new HashMap<>();
  private int position;
  private MRInputLegacy legacyMRInput;
  private MultiMRInput mainWorkMultiMRInput;
  private final ExecMapperContext execContext;
  private MapWork mapWork;
  private List<MapWork> mergeWorkList;
  private final List<String> cacheKeys = new ArrayList<>();
  private final List<String> dynamicValueCacheKeys = new ArrayList<>();
  private final ObjectCache cache, dynamicValueCache;
  // is this part of the query-based compaction process
  private final boolean isInCompaction;

  public MapRecordProcessor(final JobConf jconf, final ProcessorContext context) throws Exception {
    super(jconf, context);
    String queryId = HiveConf.getVar(jconf, HiveConf.ConfVars.HIVE_QUERY_ID);
    if (LlapProxy.isDaemon()) {
      setLlapOfFragmentId(context);
    }
    cache = ObjectCacheFactory.getCache(jconf, queryId, true);
    dynamicValueCache = ObjectCacheFactory.getCache(jconf, queryId, false, true);
    execContext = new ExecMapperContext(jconf);
    execContext.setJc(jconf);
    isInCompaction = CompactorUtil.COMPACTOR.equalsIgnoreCase(
        HiveConf.getVar(jconf, HiveConf.ConfVars.SPLIT_GROUPING_MODE));
  }

  private void setLlapOfFragmentId(final ProcessorContext context) {
    // TODO: could we do this only if the OF is actually used?
    String attemptId = Converters.createTaskAttemptId(context).toString();
    LOG.debug("Setting the LLAP fragment ID for OF to {}", attemptId);
    jconf.set(LlapOutputFormat.LLAP_OF_ID_KEY, attemptId);
  }

  @Override
  void init(MRTaskReporter mrReporter,
      Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {
    perfLogger.perfLogBegin(CLASS_NAME, PerfLogger.TEZ_INIT_OPERATORS);
    super.init(mrReporter, inputs, outputs);
    checkAbortCondition();


    String key = processorContext.getTaskVertexName() + MAP_PLAN_KEY;
    cacheKeys.add(key);


    // create map and fetch operators
    if (!isInCompaction) {
      mapWork = cache.retrieve(key, () -> Utilities.getMapWork(jconf));
    } else {
      // During query-based compaction, we don't want to retrieve old MapWork from the cache, we want a new mapper
      // and new UDF validate_acid_sort_order instance for each bucket, otherwise validate_acid_sort_order will fail.
      mapWork = Utilities.getMapWork(jconf);
    }
    // TODO HIVE-14042. Cleanup may be required if exiting early.
    Utilities.setMapWork(jconf, mapWork);

    for (PartitionDesc part : mapWork.getAliasToPartnInfo().values()) {
      TableDesc tableDesc = part.getTableDesc();
      Utilities.copyJobSecretToTableProperties(tableDesc);
    }

    String prefixes = jconf.get(DagUtils.TEZ_MERGE_WORK_FILE_PREFIXES);
    if (prefixes != null) {
      mergeWorkList = new ArrayList<>();

      for (final String prefix : prefixes.split(",")) {
        if (prefix == null || prefix.isEmpty()) {
          continue;
        }

        key = processorContext.getTaskVertexName() + prefix;
        cacheKeys.add(key);

        checkAbortCondition();
        mergeWorkList.add(
            (MapWork) cache.retrieve(key, () -> Utilities.getMergeWork(jconf, prefix)));
      }
    }

    MapredContext.init(true, new JobConf(jconf));
    ((TezContext) MapredContext.get()).setInputs(inputs);
    ((TezContext) MapredContext.get()).setTezProcessorContext(processorContext);

    // Update JobConf using MRInput, info like filename comes via this
    checkAbortCondition();
    legacyMRInput = getMRInput(inputs);
    if (legacyMRInput != null) {
      Configuration updatedConf = legacyMRInput.getConfigUpdates();
      if (updatedConf != null) {
        for (Entry<String, String> entry : updatedConf) {
          jconf.set(entry.getKey(), entry.getValue());
        }
      }
    }
    checkAbortCondition();

    createOutputMap();
    // Start all the Outputs.
    for (Entry<String, LogicalOutput> outputEntry : outputs.entrySet()) {
      LOG.debug("Starting Output: " + outputEntry.getKey());
      outputEntry.getValue().start();
      ((TezKVOutputCollector) outMap.get(outputEntry.getKey())).initialize();
    }
    checkAbortCondition();

    try {

      CompilationOpContext runtimeCtx = new CompilationOpContext();
      if (mapWork.getVectorMode()) {
        mapOp = new VectorMapOperator(runtimeCtx);
      } else {
        mapOp = new MapOperator(runtimeCtx);
      }

      // Not synchronizing creation of mapOp with an invocation. Check immediately
      // after creation in case abort has been set.
      // Relying on the regular flow to clean up the actual operator. i.e. If an exception is
      // thrown, an attempt will be made to cleanup the op.
      // If we are here - exit out via an exception. If we're in the middle of the opeartor.initialize
      // call further down, we rely upon op.abort().
      checkAbortCondition();

      mapOp.clearConnectedOperators();
      mapOp.setExecContext(execContext);

      boolean fromCache = false;
      if (mergeWorkList != null) {
        AbstractMapOperator mergeMapOp = null;
        for (BaseWork mergeWork : mergeWorkList) {
          // TODO HIVE-14042. What is mergeWork, and why is it not part of the regular operator chain.
          // The mergeMapOp.initialize call further down can block, and will not receive information
          // about an abort request.
          MapWork mergeMapWork = (MapWork) mergeWork;
          if (mergeMapWork.getVectorMode()) {
            mergeMapOp = new VectorMapOperator(runtimeCtx);
          } else {
            mergeMapOp = new MapOperator(runtimeCtx);
          }

          mergeMapOpList.add(mergeMapOp);
          // initialize the merge operators first.
          if (mergeMapOp != null) {
            mergeMapOp.setConf(mergeMapWork);
            LOG.info("Input name is {}", mergeMapWork.getName());
            jconf.set(Utilities.INPUT_NAME, mergeMapWork.getName());
            mergeMapOp.initialize(jconf, null);
            // if there are no files/partitions to read, we need to skip trying to read
            MultiMRInput multiMRInput = multiMRInputMap.get(mergeMapWork.getName());
            boolean skipRead = false;
            if (multiMRInput == null) {
              LOG.info("Multi MR Input for work {} is null. Skipping read.", mergeMapWork.getName());
              skipRead = true;
            } else {
              Collection<KeyValueReader> keyValueReaders = multiMRInput.getKeyValueReaders();
              if ((keyValueReaders == null) || (keyValueReaders.isEmpty())) {
                LOG.info("Key value readers are null or empty and hence skipping read. "
                    + "KeyValueReaders = {}", keyValueReaders);
                skipRead = true;
              }
            }
            if (skipRead) {
              List<Operator<?>> children = new ArrayList<>();
              children.addAll(mergeMapOp.getConf().getAliasToWork().values());
              // do the same thing as setChildren when there is nothing to read.
              // the setChildren method initializes the object inspector needed by the operators
              // based on path and partition information which we don't have in this case.
              mergeMapOp.initEmptyInputChildren(children, jconf);
            } else {
              // the setChildren method initializes the object inspector needed by the operators
              // based on path and partition information.
              mergeMapOp.setChildren(jconf);
            }

            Operator<? extends OperatorDesc> finalOp = getFinalOp(mergeMapOp);
            if (finalOp instanceof TezDummyStoreOperator) {
              // we ensure that we don't try to read any data in case of skip read.
              ((TezDummyStoreOperator) finalOp).setFetchDone(skipRead);
              mapOp.setConnectedOperators(mergeMapWork.getTag(), (DummyStoreOperator) finalOp);
            } else {
              // found the plan is already connected which means this is derived from the cache.
              fromCache = true;
            }

            mergeMapOp.passExecContext(new ExecMapperContext(jconf));
            mergeMapOp.initializeLocalWork(jconf);
          }
        }
      }

      if (!fromCache) {
        // if not from cache, we still need to hook up the plans.
        ((TezContext) (MapredContext.get())).setDummyOpsMap(mapOp.getConnectedOperators());
      }

      // initialize map operator
      mapOp.setConf(mapWork);
      LOG.info("Main input name is " + mapWork.getName());
      jconf.set(Utilities.INPUT_NAME, mapWork.getName());
      mapOp.initialize(jconf, null);
      checkAbortCondition();
      mapOp.setChildren(jconf);
      mapOp.passExecContext(execContext);
      LOG.info(mapOp.dump(0));

      // set memory available for operators
      long memoryAvailableToTask = processorContext.getTotalMemoryAvailableToTask();
      if (mapOp.getConf() != null) {
        mapOp.getConf().setMaxMemoryAvailable(memoryAvailableToTask);
        LOG.info("Memory available for operators set to {}", LlapUtil.humanReadableByteCount(memoryAvailableToTask));
      }
      OperatorUtils.setMemoryAvailable(mapOp.getChildOperators(), memoryAvailableToTask);

      mapOp.initializeLocalWork(jconf);

      // Setup values registry
      checkAbortCondition();
      String valueRegistryKey = DynamicValue.DYNAMIC_VALUE_REGISTRY_CACHE_KEY;
      // On LLAP dynamic value registry might already be cached.
      final DynamicValueRegistryTez registryTez = dynamicValueCache.retrieve(valueRegistryKey,
          () -> new DynamicValueRegistryTez());
      dynamicValueCacheKeys.add(valueRegistryKey);
      RegistryConfTez registryConf = new RegistryConfTez(jconf, mapWork, processorContext, inputs);
      registryTez.init(registryConf);

      checkAbortCondition();
      initializeMapRecordSources();
      mapOp.initializeMapOperator(jconf);
      if ((mergeMapOpList != null) && !mergeMapOpList.isEmpty()) {
        for (AbstractMapOperator mergeMapOp : mergeMapOpList) {
          jconf.set(Utilities.INPUT_NAME, mergeMapOp.getConf().getName());
          // TODO HIVE-14042. abort handling: Handling of mergeMapOp
          mergeMapOp.initializeMapOperator(jconf);
        }
      }

      // Initialization isn't finished until all parents of all operators
      // are initialized. For broadcast joins that means initializing the
      // dummy parent operators as well.
      List<HashTableDummyOperator> dummyOps = mapWork.getDummyOps();
      jconf.set(Utilities.INPUT_NAME, mapWork.getName());
      if (dummyOps != null) {
        for (Operator<? extends OperatorDesc> dummyOp : dummyOps){
          dummyOp.setExecContext(execContext);
          // TODO HIVE-14042. Handling of dummyOps, and propagating abort information to them
          dummyOp.initialize(jconf, null);
        }
      }

      OperatorUtils.setChildrenCollector(mapOp.getChildOperators(), outMap);
      mapOp.setReporter(reporter);
      MapredContext.get().setReporter(reporter);

    } catch (Throwable e) {
      setAborted(true);
      if (e instanceof OutOfMemoryError) {
        // will this be true here?
        // Don't create a new object if we are already out of memory
        throw (OutOfMemoryError) e;
      } else if (e instanceof InterruptedException) {
        LOG.info("Hit an interrupt while initializing MapRecordProcessor. Message={}",
            e.getMessage());
        throw (InterruptedException) e;
      } else {
        throw new RuntimeException("Map operator initialization failed", e);
      }
    }
    perfLogger.perfLogEnd(CLASS_NAME, PerfLogger.TEZ_INIT_OPERATORS);
  }

  private void initializeMapRecordSources() throws Exception {

    int size = mergeMapOpList.size() + 1; // the +1 is for the main map operator itself
    sources = new MapRecordSource[size];
    position = mapOp.getConf().getTag();
    sources[position] = new MapRecordSource();
    KeyValueReader reader = null;
    if (mainWorkMultiMRInput != null) {
      reader = getKeyValueReader(mainWorkMultiMRInput.getKeyValueReaders(), mapOp);
    } else {
      reader = legacyMRInput.getReader();
    }
    sources[position].init(jconf, mapOp, reader);
    for (AbstractMapOperator mapOp : mergeMapOpList) {
      int tag = mapOp.getConf().getTag();
      sources[tag] = new MapRecordSource();
      String inputName = mapOp.getConf().getName();
      MultiMRInput multiMRInput = multiMRInputMap.get(inputName);
      Collection<KeyValueReader> kvReaders = multiMRInput.getKeyValueReaders();
      LOG.debug("There are {} key-value readers for input {}", kvReaders.size(), inputName);
      if (kvReaders.size() > 0) {
        reader = getKeyValueReader(kvReaders, mapOp);
        sources[tag].init(jconf, mapOp, reader);
      }
    }
    ((TezContext) MapredContext.get()).setRecordSources(sources);
  }

  private KeyValueReader getKeyValueReader(Collection<KeyValueReader> keyValueReaders,
      AbstractMapOperator mapOp) throws Exception {
    List<KeyValueReader> kvReaderList = new ArrayList<KeyValueReader>(keyValueReaders);
    // this sets up the map operator contexts correctly
    mapOp.initializeContexts();
    Deserializer deserializer = mapOp.getCurrentDeserializer();
    // deserializer is null in case of VectorMapOperator
    KeyValueReader reader =
      new KeyValueInputMerger(kvReaderList, deserializer,
          new ObjectInspector[] { deserializer == null ? null : deserializer.getObjectInspector() }, mapOp
          .getConf()
          .getSortCols());
    return reader;
  }

  private Operator<? extends OperatorDesc> getFinalOp(Operator<? extends OperatorDesc> mergeMapOp) {
    for (Operator<? extends OperatorDesc> childOp : mergeMapOp.getChildOperators()) {
      if ((childOp.getChildOperators() == null) || (childOp.getChildOperators().isEmpty())) {
        return childOp;
      } else {
        return getFinalOp(childOp);
      }
    }
    return null;
  }

  @Override
  void run() throws Exception {
    startAbortChecks();
    while (sources[position].pushRecord()) {
      addRowAndMaybeCheckAbort();
    }
  }

  @Override
  public void abort() {
    // this will stop run() from pushing records, along with potentially
    // blocking initialization.
    super.abort();

    // this will abort initializeOp()
    if (mapOp != null) {
      LOG.info("Forwarding abort to mapOp: {} ", mapOp.getName());
      mapOp.abort();
    } else {
      LOG.info("mapOp not setup yet. abort not being forwarded");
    }
  }

  @Override
  void close(){
    // check if there are IOExceptions
    if (!isAborted()) {
      setAborted(execContext.getIoCxt().getIOExceptions());
    }

    if (cache != null) {
      for (String k: cacheKeys) {
        cache.release(k);
      }
    }

    if (dynamicValueCache != null) {
      for (String k: dynamicValueCacheKeys) {
        dynamicValueCache.release(k);
      }
    }

    // detecting failed executions by exceptions thrown by the operator tree
    try {
      if (mapOp == null || mapWork == null) {
        return;
      }
      boolean abort = isAborted();
      mapOp.close(abort);
      if (mergeMapOpList.isEmpty() == false) {
        for (AbstractMapOperator mergeMapOp : mergeMapOpList) {
          mergeMapOp.close(abort);
        }
      }

      // Need to close the dummyOps as well. The operator pipeline
      // is not considered "closed/done" unless all operators are
      // done. For broadcast joins that includes the dummy parents.
      List<HashTableDummyOperator> dummyOps = mapWork.getDummyOps();
      if (dummyOps != null) {
        for (Operator<? extends OperatorDesc> dummyOp : dummyOps){
          dummyOp.close(abort);
        }
      }

      ReportStats rps = new ReportStats(reporter, jconf);
      mapOp.preorderMap(rps);
      return;
    } catch (Exception e) {
      if (!isAborted()) {
        // signal new failure to map-reduce
        LOG.error("Hit error while closing operators - failing tree");
        throw new RuntimeException("Hive Runtime Error while closing operators", e);
      }
    } finally {
      Utilities.clearWorkMap(jconf);
      MapredContext.close();
    }
  }

  private MRInputLegacy getMRInput(Map<String, LogicalInput> inputs) throws Exception {
    // there should be only one MRInput
    MRInputLegacy theMRInput = null;

    // start all mr/multi-mr inputs
    Set<Input> li = new HashSet<>();
    for (LogicalInput inp: inputs.values()) {
      if (inp instanceof MRInputLegacy || inp instanceof MultiMRInput) {
        inp.start();
        li.add(inp);
      }
    }
    // TODO: HIVE-14042. Potential blocking call. MRInput handles this correctly even if an interrupt is swallowed.
    // MultiMRInput may not. Fix once TEZ-3302 is resolved.
    processorContext.waitForAllInputsReady(li);

    LOG.info("The input names are: {}", String.join(",", inputs.keySet()));
    for (Entry<String, LogicalInput> inp : inputs.entrySet()) {
      if (inp.getValue() instanceof MRInputLegacy) {
        if (theMRInput != null) {
          throw new IllegalArgumentException("Only one MRInput is expected");
        }
        // a better logic would be to find the alias
        theMRInput = (MRInputLegacy) inp.getValue();
      } else if (inp.getValue() instanceof MultiMRInput) {
        multiMRInputMap.put(inp.getKey(), (MultiMRInput) inp.getValue());
      }
    }
    if (theMRInput != null) {
      theMRInput.init();
    } else {
      String alias = mapWork.getAliasToWork().keySet().iterator().next();
      if (inputs.get(alias) instanceof MultiMRInput) {
        mainWorkMultiMRInput = (MultiMRInput) inputs.get(alias);
      } else {
        throw new IOException("Unexpected input type found: "
            + inputs.get(alias).getClass().getCanonicalName());
      }
    }
    return theMRInput;
  }
}
