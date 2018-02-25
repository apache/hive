/*
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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

import org.apache.spark.util.CallSite;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.common.JavaUtils;
import org.apache.hadoop.hive.ql.io.merge.MergeFileMapper;
import org.apache.hadoop.hive.ql.io.merge.MergeFileOutputFormat;
import org.apache.hadoop.hive.ql.io.merge.MergeFileWork;
import org.apache.hadoop.hive.ql.log.PerfLogger;
import org.apache.hadoop.hive.ql.session.SessionState;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator;
import org.apache.hadoop.hive.ql.exec.TableScanOperator;
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
import org.apache.hadoop.hive.ql.plan.TableScanDesc;
import org.apache.hadoop.hive.ql.stats.StatsCollectionContext;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

import com.google.common.base.Preconditions;

@SuppressWarnings("rawtypes")
public class SparkPlanGenerator {
  private static final String CLASS_NAME = SparkPlanGenerator.class.getName();
  private final PerfLogger perfLogger = SessionState.getPerfLogger();
  private static final Logger LOG = LoggerFactory.getLogger(SparkPlanGenerator.class);

  private final JavaSparkContext sc;
  private final JobConf jobConf;
  private final Context context;
  private final Path scratchDir;
  private final SparkReporter sparkReporter;
  private Map<BaseWork, BaseWork> cloneToWork;
  private final Map<BaseWork, SparkTran> workToTranMap;
  private final Map<BaseWork, SparkTran> workToParentWorkTranMap;
  // a map from each BaseWork to its cloned JobConf
  private final Map<BaseWork, JobConf> workToJobConf;

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
    this.workToParentWorkTranMap = new HashMap<BaseWork, SparkTran>();
    this.sparkReporter = sparkReporter;
    this.workToJobConf = new HashMap<BaseWork, JobConf>();
  }

  public SparkPlan generate(SparkWork sparkWork) throws Exception {
    perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_BUILD_PLAN);
    SparkPlan sparkPlan = new SparkPlan(this.sc.sc());
    cloneToWork = sparkWork.getCloneToWork();
    workToTranMap.clear();
    workToParentWorkTranMap.clear();

    try {
      for (BaseWork work : sparkWork.getAllWork()) {
        perfLogger.PerfLogBegin(CLASS_NAME, PerfLogger.SPARK_CREATE_TRAN + work.getName());
        SparkTran tran = generate(work, sparkWork);
        SparkTran parentTran = generateParentTran(sparkPlan, sparkWork, work);
        sparkPlan.addTran(tran);
        sparkPlan.connect(parentTran, tran);
        workToTranMap.put(work, tran);
        perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_CREATE_TRAN + work.getName());
      }
    } finally {
      // clear all ThreadLocal cached MapWork/ReduceWork after plan generation
      // as this may executed in a pool thread.
      Utilities.clearWorkMap(jobConf);
    }

    perfLogger.PerfLogEnd(CLASS_NAME, PerfLogger.SPARK_BUILD_PLAN);
    return sparkPlan;
  }

  // Generate (possibly get from a cached result) parent SparkTran
  private SparkTran generateParentTran(SparkPlan sparkPlan, SparkWork sparkWork,
                                       BaseWork work) throws Exception {
    if (cloneToWork.containsKey(work)) {
      BaseWork originalWork = cloneToWork.get(work);
      if (workToParentWorkTranMap.containsKey(originalWork)) {
        return workToParentWorkTranMap.get(originalWork);
      }
    }

    SparkTran result;
    if (work instanceof MapWork) {
      result = generateMapInput(sparkPlan, (MapWork)work);
      sparkPlan.addTran(result);
    } else if (work instanceof ReduceWork) {
      boolean toCache = cloneToWork.containsKey(work);
      List<BaseWork> parentWorks = sparkWork.getParents(work);
      SparkEdgeProperty sparkEdgeProperty = sparkWork.getEdgeProperty(parentWorks.get(0), work);
      result = generate(sparkPlan, sparkEdgeProperty, toCache, work.getName());
      sparkPlan.addTran(result);
      for (BaseWork parentWork : parentWorks) {
        sparkPlan.connect(workToTranMap.get(parentWork), result);
      }
    } else {
      throw new IllegalStateException("AssertionError: expected either MapWork or ReduceWork, "
        + "but found " + work.getClass().getName());
    }

    if (cloneToWork.containsKey(work)) {
      workToParentWorkTranMap.put(cloneToWork.get(work), result);
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

    sc.sc().setCallSite(CallSite.apply(mapWork.getName(), ""));

    JavaPairRDD<WritableComparable, Writable> hadoopRDD;
    if (mapWork.getNumMapTasks() != null) {
      jobConf.setNumMapTasks(mapWork.getNumMapTasks());
      hadoopRDD = sc.hadoopRDD(jobConf, ifClass,
          WritableComparable.class, Writable.class, mapWork.getNumMapTasks());
    } else {
      hadoopRDD = sc.hadoopRDD(jobConf, ifClass, WritableComparable.class, Writable.class);
    }

    boolean toCache = false/*cloneToWork.containsKey(mapWork)*/;

    String tables = mapWork.getAllRootOperators().stream()
            .filter(op -> op instanceof TableScanOperator)
            .map(ts -> ((TableScanDesc) ts.getConf()).getAlias())
            .collect(Collectors.joining(", "));

    String rddName = mapWork.getName() + " (" + tables + ", " + hadoopRDD.getNumPartitions() +
            (toCache ? ", cached)" : ")");

    // Caching is disabled for MapInput due to HIVE-8920
    MapInput result = new MapInput(sparkPlan, hadoopRDD, toCache, rddName);
    return result;
  }

  private ShuffleTran generate(SparkPlan sparkPlan, SparkEdgeProperty edge, boolean toCache,
                               String name) {

    Preconditions.checkArgument(!edge.isShuffleNone(),
        "AssertionError: SHUFFLE_NONE should only be used for UnionWork.");
    SparkShuffler shuffler;
    if (edge.isMRShuffle()) {
      shuffler = new SortByShuffler(false, sparkPlan);
    } else if (edge.isShuffleSort()) {
      shuffler = new SortByShuffler(true, sparkPlan);
    } else {
      shuffler = new GroupByShuffler();
    }
    return new ShuffleTran(sparkPlan, shuffler, edge.getNumPartitions(), toCache, name, edge);
  }

  private SparkTran generate(BaseWork work, SparkWork sparkWork) throws Exception {
    initStatsPublisher(work);
    JobConf newJobConf = cloneJobConf(work);
    checkSpecs(work, newJobConf);
    byte[] confBytes = KryoSerializer.serializeJobConf(newJobConf);
    boolean caching = isCachingWork(work, sparkWork);
    if (work instanceof MapWork) {
      // Create tmp dir for MergeFileWork
      if (work instanceof MergeFileWork) {
        Path outputPath = ((MergeFileWork) work).getOutputDir();
        Path tempOutPath = Utilities.toTempPath(outputPath);
        FileSystem fs = outputPath.getFileSystem(jobConf);
        try {
          if (!fs.exists(tempOutPath)) {
            fs.mkdirs(tempOutPath);
          }
        } catch (IOException e) {
          throw new RuntimeException(
              "Can't make path " + outputPath + " : " + e.getMessage());
        }
      }
      MapTran mapTran = new MapTran(caching, work.getName());
      HiveMapFunction mapFunc = new HiveMapFunction(confBytes, sparkReporter);
      mapTran.setMapFunction(mapFunc);
      return mapTran;
    } else if (work instanceof ReduceWork) {
      ReduceTran reduceTran = new ReduceTran(caching, work.getName());
      HiveReduceFunction reduceFunc = new HiveReduceFunction(confBytes, sparkReporter);
      reduceTran.setReduceFunction(reduceFunc);
      return reduceTran;
    } else {
      throw new IllegalStateException("AssertionError: expected either MapWork or ReduceWork, "
        + "but found " + work.getClass().getName());
    }
  }

  private boolean isCachingWork(BaseWork work, SparkWork sparkWork) {
    boolean caching = true;
    List<BaseWork> children = sparkWork.getChildren(work);
    if (children.size() < 2) {
      caching = false;
    } else {
      // do not cache this if its child RDD is intend to be cached.
      for (BaseWork child : children) {
        if (cloneToWork.containsKey(child)) {
          caching = false;
        }
      }
    }
    return caching;
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
      cloned.setPartitionerClass(JavaUtils.loadClass(HiveConf.getVar(cloned, HiveConf.ConfVars.HIVEPARTITIONER)));
    } catch (ClassNotFoundException e) {
      String msg = "Could not find partitioner class: " + e.getMessage()
        + " which is specified by: " + HiveConf.ConfVars.HIVEPARTITIONER.varname;
      throw new IllegalArgumentException(msg, e);
    }
    if (work instanceof MapWork) {
      MapWork mapWork = (MapWork) work;
      cloned.setBoolean("mapred.task.is.map", true);
      List<Path> inputPaths = Utilities.getInputPaths(cloned, mapWork,
          scratchDir, context, false);
      Utilities.setInputPaths(cloned, inputPaths);
      Utilities.setMapWork(cloned, mapWork, scratchDir, false);
      Utilities.createTmpDirs(cloned, mapWork);
      if (work instanceof MergeFileWork) {
        MergeFileWork mergeFileWork = (MergeFileWork) work;
        cloned.set(Utilities.MAPRED_MAPPER_CLASS, MergeFileMapper.class.getName());
        cloned.set("mapred.input.format.class", mergeFileWork.getInputformat());
        cloned.setClass("mapred.output.format.class", MergeFileOutputFormat.class,
            FileOutputFormat.class);
      } else {
        cloned.set(Utilities.MAPRED_MAPPER_CLASS, ExecMapper.class.getName());
      }
      if (mapWork.getMaxSplitSize() != null) {
        HiveConf.setLongVar(cloned, HiveConf.ConfVars.MAPREDMAXSPLITSIZE,
            mapWork.getMaxSplitSize());
      }
      if (mapWork.getMinSplitSize() != null) {
        HiveConf.setLongVar(cloned, HiveConf.ConfVars.MAPREDMINSPLITSIZE,
            mapWork.getMinSplitSize());
      }
      if (mapWork.getMinSplitSizePerNode() != null) {
        HiveConf.setLongVar(cloned, HiveConf.ConfVars.MAPREDMINSPLITSIZEPERNODE,
            mapWork.getMinSplitSizePerNode());
      }
      if (mapWork.getMinSplitSizePerRack() != null) {
        HiveConf.setLongVar(cloned, HiveConf.ConfVars.MAPREDMINSPLITSIZEPERRACK,
            mapWork.getMinSplitSizePerRack());
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
        StatsCollectionContext sc = new StatsCollectionContext(jobConf);
        sc.setStatsTmpDirs(Utilities.getStatsTmpDirs(work, jobConf));
        if (!statsPublisher.init(sc)) { // creating stats table if not exists
          if (HiveConf.getBoolVar(jobConf, HiveConf.ConfVars.HIVE_STATS_RELIABLE)) {
            throw new HiveException(
                ErrorMsg.STATSPUBLISHER_INITIALIZATION_ERROR.getErrorCodedMsg());
          }
        }
      }
    }
  }

}
