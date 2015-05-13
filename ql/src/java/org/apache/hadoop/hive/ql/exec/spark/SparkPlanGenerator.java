/**
 *  Licensed to the Apache Software Foundation (ASF) under one
 *  or more contributor license agreements.  See the NOTICE file
 *  distributed with this work for additional information
 *  regarding copyright ownership.  The ASF licenses this file
 *  to you under the Apache License, Version 2.0 (the
 *  "License"); you may not use this file except in compliance
 *  with the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

package org.apache.hadoop.hive.ql.exec.spark;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import com.google.common.base.Preconditions;

import com.google.common.collect.Maps;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.ql.io.merge.MergeFileMapper;
import org.apache.hadoop.hive.ql.io.merge.MergeFileOutputFormat;
import org.apache.hadoop.hive.ql.io.merge.MergeFileWork;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.exec.mr.ExecReducer;
import org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
import org.apache.hadoop.hive.ql.metadata.HiveException;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkEdgeProperty;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

@SuppressWarnings("rawtypes")
public class SparkPlanGenerator {
  private static final String CLASS_NAME = SparkPlanGenerator.class.getName();
  private final PerfLogger perfLogger = PerfLogger.getPerfLogger();
  private static final Log LOG = LogFactory.getLog(SparkPlanGenerator.class);

  private JavaSparkContext sc;
  private final JobConf jobConf;
  private Context context;
  private Path scratchDir;
  private SparkReporter sparkReporter;
  private final Map<BaseWork, SparkTran> workToTranMap;
  // a map from each BaseWork to its cloned JobConf
  private final Map<BaseWork, JobConf> workToJobConf;

  private Map<BaseWork, Pair<String, Integer>> equivalentWorks;
  private List<BaseWork> cachingWorks;

  public SparkPlanGenerator(
    JavaSparkContext sc,
    Context context,
    JobConf jobConf,
    Path scratchDir,
    SparkReporter sparkReporter) {

    this.sc = sc;
    this.context = context;
    this.jobConf = jobConf;
    this.scratchDir = scratchDir;
    this.workToTranMap = new HashMap<BaseWork, SparkTran>();
    this.sparkReporter = sparkReporter;
    this.workToJobConf = new HashMap<BaseWork, JobConf>();
  }

  public SparkPlan generate(SparkWork sparkWork) throws Exception {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_BUILD_PLAN);
    equivalentWorks = sparkWork.getEquivalentWorks();
    cachingWorks = sparkWork.getCachingWorks();
    SparkPlan sparkPlan = new SparkPlan();
    workToTranMap.clear();

    try {
      for (BaseWork work : sparkWork.getAllWork()) {
        perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_CREATE_TRAN + work.getName());
        SparkTran tran = generate(work);
        SparkTran parentTran = generateParentTran(sparkPlan, sparkWork, work);
        sparkPlan.addTran(tran);
        sparkPlan.connect(parentTran, tran);
        workToTranMap.put(work, tran);
        perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_CREATE_TRAN + work.getName());
      }
    } finally {
      // clear all ThreadLocal cached MapWork/ReduceWork after plan generation
      // as this may executed in a pool thread.
      Utilities.clearWorkMap();
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_BUILD_PLAN);
    return sparkPlan;
  }

  // Generate (possibly get from a cached result) parent SparkTran
  private SparkTran generateParentTran(SparkPlan sparkPlan, SparkWork sparkWork,
                                       BaseWork work) throws Exception {
    SparkTran result;
    if (work instanceof MapWork) {
      result = generateMapInput(sparkPlan, (MapWork)work);
      sparkPlan.addTran(result);
    } else if (work instanceof ReduceWork) {
      List<BaseWork> parentWorks = sparkWork.getParents(work);
      result = generate(sparkPlan,
        sparkWork.getEdgeProperty(parentWorks.get(0), work));
      sparkPlan.addTran(result);
      for (BaseWork parentWork : parentWorks) {
        sparkPlan.connect(workToTranMap.get(parentWork), result);
      }
    } else {
      throw new IllegalStateException("AssertionError: expected either MapWork or ReduceWork, "
        + "but found " + work.getClass().getName());
    }

    return result;
  }

  private Class<?> getInputFormat(JobConf jobConf, MapWork mWork) throws HiveException {
    // MergeFileWork is sub-class of MapWork, we don't need to distinguish here
    if (mWork.getInputformat() != null) {
      HiveConf.setVar(jobConf, HiveConf.ConfVars.HIVEINPUTFORMAT,
          mWork.getInputformat());
    }
    String inpFormat = HiveConf.getVar(jobConf,
        HiveConf.ConfVars.HIVEINPUTFORMAT);

    if (mWork.isUseBucketizedHiveInputFormat()) {
      inpFormat = BucketizedHiveInputFormat.class.getName();
    }

    Class inputFormatClass;
    try {
      inputFormatClass = JavaUtils.loadClass(inpFormat);
    } catch (ClassNotFoundException e) {
      String message = "Failed to load specified input format class:"
          + inpFormat;
      LOG.error(message, e);
      throw new HiveException(message, e);
    }

    return inputFormatClass;
  }

  @SuppressWarnings("unchecked")
  private MapInput generateMapInput(SparkPlan sparkPlan, MapWork mapWork)
      throws Exception {
    JobConf jobConf = cloneJobConf(mapWork);
    Class ifClass = getInputFormat(jobConf, mapWork);

    if (equivalentWorks != null && equivalentWorks.containsKey(mapWork)) {
      Pair<String, Integer> pair = equivalentWorks.get(mapWork);
      String uuid = pair.getLeft();
      SparkTran sparkTran = SparkTranContainer.getSparkTran(uuid);
      if (sparkTran != null) {
        return (MapInput)sparkTran;
      } else {
        JavaPairRDD<WritableComparable, Writable> hadoopRDD = sc.hadoopRDD(jobConf, ifClass,
          WritableComparable.class, Writable.class);
        MapInput result = new MapInput(sparkPlan, hadoopRDD, true);
        SparkTranContainer.putSparkTran(uuid, result, pair.getRight());
        result = (MapInput)SparkTranContainer.getSparkTran(uuid);
        return result;
      }
    } else {
      JavaPairRDD<WritableComparable, Writable> hadoopRDD = sc.hadoopRDD(jobConf, ifClass,
        WritableComparable.class, Writable.class);
      MapInput result = new MapInput(sparkPlan, hadoopRDD);
      return result;
    }
  }

  private ShuffleTran generate(SparkPlan sparkPlan, SparkEdgeProperty edge) {
    Preconditions.checkArgument(!edge.isShuffleNone(),
        "AssertionError: SHUFFLE_NONE should only be used for UnionWork.");
    SparkShuffler shuffler;
    if (edge.isMRShuffle()) {
      shuffler = new SortByShuffler(false);
    } else if (edge.isShuffleSort()) {
      shuffler = new SortByShuffler(true);
    } else {
      shuffler = new GroupByShuffler();
    }
    return new ShuffleTran(sparkPlan, shuffler, edge.getNumPartitions());
  }

  private SparkTran generate(BaseWork work) throws Exception {
    initStatsPublisher(work);
    JobConf newJobConf = cloneJobConf(work);
    checkSpecs(work, newJobConf);
    byte[] confBytes = KryoSerializer.serializeJobConf(newJobConf);
    if (work instanceof MapWork) {
      MapTran mapTran;
      if (cachingWorks != null && cachingWorks.contains(work)) {
        mapTran = new MapTran(true);
      } else {
        mapTran = new MapTran();
      }
      HiveMapFunction mapFunc = new HiveMapFunction(confBytes, sparkReporter);
      mapTran.setMapFunction(mapFunc);
      return mapTran;
    } else if (work instanceof ReduceWork) {
      ReduceTran reduceTran;
      if (cachingWorks != null && cachingWorks.contains(work)) {
        reduceTran  = new ReduceTran(true);
      } else {
        reduceTran = new ReduceTran();
      }
      HiveReduceFunction reduceFunc = new HiveReduceFunction(confBytes, sparkReporter);
      reduceTran.setReduceFunction(reduceFunc);
      return reduceTran;
    } else {
      throw new IllegalStateException("AssertionError: expected either MapWork or ReduceWork, "
        + "but found " + work.getClass().getName());
    }
  }

  private void checkSpecs(BaseWork work, JobConf jc) throws Exception {
    Set<Operator<?>> opList = work.getAllOperators();
    for (Operator<?> op : opList) {
      if (op instanceof FileSinkOperator) {
        ((FileSinkOperator) op).checkOutputSpecs(null, jc);
      }
    }
  }

  @SuppressWarnings({ "unchecked" })
  private JobConf cloneJobConf(BaseWork work) throws Exception {
    if (workToJobConf.containsKey(work)) {
      return workToJobConf.get(work);
    }
    JobConf cloned = new JobConf(jobConf);
    // Make sure we'll use a different plan path from the original one
    HiveConf.setVar(cloned, HiveConf.ConfVars.PLAN, "");
    try {
      cloned.setPartitionerClass((Class<? extends Partitioner>)
          JavaUtils.loadClass(HiveConf.getVar(cloned, HiveConf.ConfVars.HIVEPARTITIONER)));
    } catch (ClassNotFoundException e) {
      String msg = "Could not find partitioner class: " + e.getMessage()
        + " which is specified by: " + HiveConf.ConfVars.HIVEPARTITIONER.varname;
      throw new IllegalArgumentException(msg, e);
    }
    if (work instanceof MapWork) {
      cloned.setBoolean("mapred.task.is.map", true);
      List<Path> inputPaths = Utilities.getInputPaths(cloned, (MapWork) work,
          scratchDir, context, false);
      Utilities.setInputPaths(cloned, inputPaths);
      Utilities.setMapWork(cloned, (MapWork) work, scratchDir, false);
      Utilities.createTmpDirs(cloned, (MapWork) work);
      if (work instanceof MergeFileWork) {
        MergeFileWork mergeFileWork = (MergeFileWork) work;
        cloned.set(Utilities.MAPRED_MAPPER_CLASS, MergeFileMapper.class.getName());
        cloned.set("mapred.input.format.class", mergeFileWork.getInputformat());
        cloned.setClass("mapred.output.format.class", MergeFileOutputFormat.class,
            FileOutputFormat.class);
      } else {
        cloned.set(Utilities.MAPRED_MAPPER_CLASS, ExecMapper.class.getName());
      }
      // remember the JobConf cloned for each MapWork, so we won't clone for it again
      workToJobConf.put(work, cloned);
    } else if (work instanceof ReduceWork) {
      cloned.setBoolean("mapred.task.is.map", false);
      Utilities.setReduceWork(cloned, (ReduceWork) work, scratchDir, false);
      Utilities.createTmpDirs(cloned, (ReduceWork) work);
      cloned.set(Utilities.MAPRED_REDUCER_CLASS, ExecReducer.class.getName());
    }
    return cloned;
  }

  private void initStatsPublisher(BaseWork work) throws HiveException {
    // initialize stats publisher if necessary
    if (work.isGatheringStats()) {
      StatsPublisher statsPublisher;
      StatsFactory factory = StatsFactory.newFactory(jobConf);
      if (factory != null) {
        statsPublisher = factory.getStatsPublisher();
        if (!statsPublisher.init(jobConf)) { // creating stats table if not exists
          if (HiveConf.getBoolVar(jobConf, HiveConf.ConfVars.HIVE_STATS_RELIABLE)) {
            throw new HiveException(
                ErrorMsg.STATSPUBLISHER_INITIALIZATION_ERROR.getErrorCodedMsg());
          }
        }
      }
    }
  }

  /**
   * RDD may be cached cross SparkTasks, while SparkPlanGenerator is created per SparkTask,
   * so cached SparkTran is stored in static variables. We add SparkTran container with count
   * here to make sure it is cleared after all referred SparkJob has generated.
   */
  static class SparkTranContainer {

    private static ConcurrentMap<String, SparkTran> sparkTranMap = Maps.newConcurrentMap();
    private static ConcurrentMap<String, AtomicInteger> sparkTranCount = Maps.newConcurrentMap();

    public static SparkTran getSparkTran(String id) {

      SparkTran sparkTran = sparkTranMap.get(id);
      if (sparkTran != null) {
        int count = sparkTranCount.get(id).decrementAndGet();
        if (count == 0) {
          sparkTranMap.remove(id);
          sparkTranCount.remove(id);
        }
      }
      return sparkTran;
    }

    public static void putSparkTran(String id, SparkTran sparkTran, int count) {
      sparkTranMap.putIfAbsent(id, sparkTran);
      sparkTranCount.putIfAbsent(id, new AtomicInteger(count));
    }
  }
}
