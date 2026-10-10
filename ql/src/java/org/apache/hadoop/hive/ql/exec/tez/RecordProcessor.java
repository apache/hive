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
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.hadoop.hive.ql.exec.tez;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.exec.ObjectCache;
import org.apache.hadoop.hive.ql.exec.ObjectCacheFactory;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.tez.TezProcessor.TezKVOutputCollector;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.tez.mapreduce.processor.MRTaskReporter;
import org.apache.tez.runtime.api.LogicalInput;
import org.apache.tez.runtime.api.LogicalOutput;
import org.apache.tez.runtime.api.ProcessorContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Preconditions;

/**
 * Process input from tez LogicalInput and write output
 * It has different subclasses for map and reduce processing
 */
public abstract class RecordProcessor extends InterruptibleProcessing {
  protected final JobConf jconf;
  protected Map<String, LogicalInput> inputs;
  protected Map<String, LogicalOutput> outputs;
  protected Map<String, OutputCollector> outMap;
  protected final ProcessorContext processorContext;

  private static final Logger LOG = LoggerFactory.getLogger(RecordProcessor.class);

  protected MRTaskReporter reporter;

  protected PerfLogger perfLogger = SessionState.getPerfLogger();
  protected String CLASS_NAME = RecordProcessor.class.getName();

  protected final String queryId;

  /**
   * Per-processor plan cache — no daemon-wide sharing. Sharing would race on
   * per-fragment operator state that {@code initializeOp()} resets (HIVE-14433:
   * {@code FileSinkOperator.fsp}, {@code VectorGroupByOperator.aggregator},
   * {@code Operator.childOperatorsArray}, {@code VectorTopNKeyOperator} filter
   * state, ...) and yields NPE / {@code FileAlreadyExistsException}.
   */
  protected final TrackedCache planCache;

  /**
   * Dynamic value cache. On LLAP this is the per-query daemon-wide
   * {@link LlapObjectCache}, so dynamic values computed once (e.g. broadcast
   * hash tables, DPP registries) are reused across fragments of the same query.
   */
  protected final TrackedCache dynamicValueCache;

  public RecordProcessor(JobConf jConf, ProcessorContext processorContext) {
    this.jconf = jConf;
    this.processorContext = processorContext;
    this.queryId = HiveConf.getVar(jConf, HiveConf.ConfVars.HIVE_QUERY_ID);
    this.planCache = new TrackedCache(
        ObjectCacheFactory.getCache(jConf, queryId, true, false));
    this.dynamicValueCache = new TrackedCache(
        ObjectCacheFactory.getCache(jConf, queryId, false, true));
  }

  /**
   * Common initialization code for RecordProcessors
   * @param mrReporter
   * @param inputs map of Input names to {@link LogicalInput}s
   * @param outputs map of Output names to {@link LogicalOutput}s
   * @throws Exception
   */
  void init(MRTaskReporter mrReporter,
      Map<String, LogicalInput> inputs, Map<String, LogicalOutput> outputs) throws Exception {
    this.reporter = mrReporter;
    this.inputs = inputs;
    this.outputs = outputs;

    checkAbortCondition();
    Utilities.tryLoggingClassPaths(jconf, LOG);
  }

  /**
   * start processing the inputs and writing output
   * @throws Exception
   */
  abstract void run() throws Exception;

  abstract void close();

  protected void createOutputMap() {
    Preconditions.checkState(outMap == null, "Outputs should only be setup once");
    outMap = new HashMap<>();
    for (Entry<String, LogicalOutput> entry : outputs.entrySet()) {
      TezKVOutputCollector collector = new TezKVOutputCollector(entry.getValue());
      outMap.put(entry.getKey(), collector);
    }
  }

  /**
   * Release every key retrieved through the plan and dynamic-value caches.
   * A no-op for {@link LlapObjectCache} (which relies on soft references), but
   * preserved for correctness against other {@link ObjectCache} implementations.
   */
  protected void releaseCache() {
    planCache.releaseAll();
    dynamicValueCache.releaseAll();
  }

  public List<BaseWork> getMergeWorkList(final JobConf jconf) throws HiveException {
    String prefixes = jconf.get(DagUtils.TEZ_MERGE_WORK_FILE_PREFIXES);
    if (prefixes == null) {
      return null;
    }
    List<BaseWork> mergeWorkList = new ArrayList<>();
    for (final String prefix : prefixes.split(",")) {
      if (prefix.isEmpty()) {
        continue;
      }
      mergeWorkList.add(planCache.retrieve(prefix, () -> Utilities.getMergeWork(jconf, prefix)));
    }
    return mergeWorkList;
  }

  /**
   * An {@link ObjectCache} paired with the set of keys retrieved through it, so
   * {@link #releaseAll()} releases exactly those keys at close time. All retrievals
   * that need to be released should go through {@link #retrieve} — calling
   * {@link ObjectCache#retrieve} directly on the underlying cache bypasses the
   * tracking and leaks the key.
   */
  protected static final class TrackedCache {
    private final ObjectCache cache;
    private final List<String> keys = new ArrayList<>();

    TrackedCache(ObjectCache cache) {
      this.cache = cache;
    }

    /** Retrieve (or compute) the value for {@code key}, tracking it for release. */
    <T> T retrieve(String key, Callable<T> fn) throws HiveException {
      keys.add(key);
      return cache.retrieve(key, fn);
    }

    /** Release every key retrieved through this wrapper. Null-safe on the underlying cache. */
    void releaseAll() {
      if (cache == null) {
        return;
      }
      for (String k : keys) {
        cache.release(k);
      }
    }
  }
}
