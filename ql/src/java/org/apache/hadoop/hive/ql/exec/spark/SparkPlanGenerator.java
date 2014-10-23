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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Operator;
import org.apache.hadoop.hive.ql.exec.ReduceSinkOperator;
import org.apache.hadoop.hive.ql.io.merge.MergeFileMapper;
import org.apache.hadoop.hive.ql.io.merge.MergeFileOutputFormat;
import org.apache.hadoop.hive.ql.io.merge.MergeFileWork;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
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
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkPlanGenerator {
  private static final Log LOG = LogFactory.getLog(SparkPlanGenerator.class);

  private JavaSparkContext sc;
  private final JobConf jobConf;
  private Context context;
  private Path scratchDir;
  private SparkReporter sparkReporter;
  private final Map<BaseWork, BaseWork> cloneToWork;
  private final Map<BaseWork, SparkTran> workToTranMap;
  private final Map<BaseWork, SparkTran> workToParentWorkTranMap;

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
    this.cloneToWork = new HashMap<BaseWork, BaseWork>();
    this.workToTranMap = new HashMap<BaseWork, SparkTran>();
    this.workToParentWorkTranMap = new HashMap<BaseWork, SparkTran>();
    this.sparkReporter = sparkReporter;
  }

  public SparkPlan generate(SparkWork sparkWork) throws Exception {
    SparkPlan sparkPlan = new SparkPlan();
    cloneToWork.clear();
    workToTranMap.clear();
    workToParentWorkTranMap.clear();

    splitSparkWork(sparkWork);

    for (BaseWork work : sparkWork.getAllWork()) {
      SparkTran tran;
      if (work instanceof MapWork) {
        SparkTran mapInput = generateParentTran(sparkPlan, sparkWork, work);
        tran = generate((MapWork)work);
        sparkPlan.addTran(tran);
        sparkPlan.connect(mapInput, tran);
      } else if (work instanceof ReduceWork) {
        SparkTran shuffleTran = generateParentTran(sparkPlan, sparkWork, work);
        tran = generate((ReduceWork)work);
        sparkPlan.addTran(tran);
        sparkPlan.connect(shuffleTran, tran);
      } else {
        List<BaseWork> parentWorks = sparkWork.getParents(work);
        tran = new IdentityTran();
        sparkPlan.addTran(tran);
        for (BaseWork parentWork : parentWorks) {
          SparkTran parentTran = workToTranMap.get(parentWork);
          sparkPlan.connect(parentTran, tran);
        }
      }

      workToTranMap.put(work, tran);
    }

    return sparkPlan;
  }

  // Generate (possibly get from a cached result) parent SparkTran
  private SparkTran generateParentTran(SparkPlan sparkPlan, SparkWork sparkWork, BaseWork work) throws Exception {
    if (cloneToWork.containsKey(work)) {
      BaseWork originalWork = cloneToWork.get(work);
      if (workToParentWorkTranMap.containsKey(originalWork)) {
        return workToParentWorkTranMap.get(originalWork);
      }
    }

    SparkTran result;
    if (work instanceof MapWork) {
      result = generateMapInput((MapWork)work);
      sparkPlan.addTran(result);
    } else if (work instanceof ReduceWork) {
      List<BaseWork> parentWorks = sparkWork.getParents(work);
      result = generate(sparkWork.getEdgeProperty(parentWorks.get(0), work), cloneToWork.containsKey(work));
      sparkPlan.addTran(result);
      for (BaseWork parentWork : parentWorks) {
        sparkPlan.connect(workToTranMap.get(parentWork), result);
      }
    } else {
      throw new IllegalStateException("AssertionError: generateParentTran() only expect MapWork or ReduceWork," +
          " but found " + work.getClass().getName());
    }

    if (cloneToWork.containsKey(work)) {
      workToParentWorkTranMap.put(cloneToWork.get(work), result);
    }

    return result;
  }


  private void splitSparkWork(SparkWork sparkWork) {
    // do a BFS on the sparkWork graph, and look for any work that has more than one child.
    // If we found such a work, we split it into multiple ones, one for each of its child.
    Queue<BaseWork> queue = new LinkedList<BaseWork>();
    Set<BaseWork> visited = new HashSet<BaseWork>();
    queue.addAll(sparkWork.getRoots());
    while (!queue.isEmpty()) {
      BaseWork work = queue.poll();
      if (!visited.add(work)) {
        continue;
      }

      List<BaseWork> childWorks = sparkWork.getChildren(work);
      // First, add all children of this work into queue, to be processed later.
      for (BaseWork w : childWorks) {
        queue.add(w);
      }

      // Second, check if this work has multiple reduceSinks. If so, do split.
      splitBaseWork(sparkWork, work, childWorks);
    }
  }

  private Set<Operator<?>> getAllReduceSinks(BaseWork work) {
    Set<Operator<?>> resultSet = work.getAllLeafOperators();
    Iterator<Operator<?>> it = resultSet.iterator();
    while (it.hasNext()) {
      if (!(it.next() instanceof ReduceSinkOperator)) {
        it.remove();
      }
    }
    return resultSet;
  }

  // Split work into multiple branches, one for each childWork in childWorks.
  // It also set up the connection between each parent work and child work.
  private void splitBaseWork(SparkWork sparkWork, BaseWork parentWork, List<BaseWork> childWorks) {
    if (getAllReduceSinks(parentWork).size() <= 1) {
      return;
    }

    // Grand-parent works - we need to set these to be the parents of the cloned works.
    List<BaseWork> grandParentWorks = sparkWork.getParents(parentWork);
    boolean isFirst = true;

    for (BaseWork childWork : childWorks) {
      BaseWork clonedParentWork = Utilities.cloneBaseWork(parentWork);
      String childReducerName = childWork.getName();
      SparkEdgeProperty clonedEdgeProperty = sparkWork.getEdgeProperty(parentWork, childWork);

      // We need to remove those branches that
      // 1, ended with a ReduceSinkOperator, and
      // 2, the ReduceSinkOperator's name is not the same as childReducerName.
      // Also, if the cloned work is not the first, we remove ALL leaf operators except
      // the corresponding ReduceSinkOperator.
      for (Operator<?> op : clonedParentWork.getAllLeafOperators()) {
        if (op instanceof ReduceSinkOperator) {
          if (!((ReduceSinkOperator)op).getConf().getOutputName().equals(childReducerName)) {
            removeOpRecursive(op);
          }
        } else if (!isFirst) {
          removeOpRecursive(op);
        }
      }

      isFirst = false;

      // Then, we need to set up the graph connection. Especially:
      // 1, we need to connect this cloned parent work with all the grand-parent works.
      // 2, we need to connect this cloned parent work with the corresponding child work.
      sparkWork.add(clonedParentWork);
      for (BaseWork gpw : grandParentWorks) {
        sparkWork.connect(gpw, clonedParentWork, sparkWork.getEdgeProperty(gpw, parentWork));
      }
      sparkWork.connect(clonedParentWork, childWork, clonedEdgeProperty);
      cloneToWork.put(clonedParentWork, parentWork);
    }

    sparkWork.remove(parentWork);
  }

  // Remove op from all its parents' child list.
  // Recursively remove any of its parent who only have this op as child.
  private void removeOpRecursive(Operator<?> operator) {
    List<Operator<?>> parentOperators = new ArrayList<Operator<?>>();
    for (Operator<?> op : operator.getParentOperators()) {
      parentOperators.add(op);
    }
    for (Operator<?> parentOperator : parentOperators) {
      Preconditions.checkArgument(parentOperator.getChildOperators().contains(operator),
          "AssertionError: parent of " + operator.getName() + " doesn't have it as child.");
      parentOperator.removeChild(operator);
      if (parentOperator.getNumChild() == 0) {
        removeOpRecursive(parentOperator);
      }
    }
  }

  private Class getInputFormat(JobConf jobConf, MapWork mWork) throws HiveException {
    // MergeFileWork is sub-class of MapWork, we don't need to distinguish here
    if (mWork.getInputformat() != null) {
      HiveConf.setVar(jobConf, HiveConf.ConfVars.HIVEINPUTFORMAT,
          mWork.getInputformat());
    }
    String inpFormat = HiveConf.getVar(jobConf,
        HiveConf.ConfVars.HIVEINPUTFORMAT);
    if ((inpFormat == null) || (StringUtils.isBlank(inpFormat))) {
      inpFormat = ShimLoader.getHadoopShims().getInputFormatClassName();
    }

    if (mWork.isUseBucketizedHiveInputFormat()) {
      inpFormat = BucketizedHiveInputFormat.class.getName();
    }

    Class inputFormatClass;
    try {
      inputFormatClass = Class.forName(inpFormat);
    } catch (ClassNotFoundException e) {
      String message = "Failed to load specified input format class:"
          + inpFormat;
      LOG.error(message, e);
      throw new HiveException(message, e);
    }

    return inputFormatClass;
  }

  private MapInput generateMapInput(MapWork mapWork)
      throws Exception {
    JobConf jobConf = cloneJobConf(mapWork);
    Class ifClass = getInputFormat(jobConf, mapWork);

    JavaPairRDD<WritableComparable, Writable> hadoopRDD = sc.hadoopRDD(jobConf, ifClass,
        WritableComparable.class, Writable.class);
    MapInput result = new MapInput(hadoopRDD,
        false /*TODO: fix this after resolving HIVE-8457: cloneToWork.containsKey(mapWork)*/);
    return result;
  }

  private ShuffleTran generate(SparkEdgeProperty edge, boolean needCache) {
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
    return new ShuffleTran(shuffler, edge.getNumPartitions(), needCache);
  }

  private MapTran generate(MapWork mw) throws Exception {
    initStatsPublisher(mw);
    MapTran result = new MapTran();
    JobConf newJobConf = cloneJobConf(mw);
    byte[] confBytes = KryoSerializer.serializeJobConf(newJobConf);
    HiveMapFunction mapFunc = new HiveMapFunction(confBytes, sparkReporter);
    result.setMapFunction(mapFunc);
    return result;
  }

  private ReduceTran generate(ReduceWork rw) throws Exception {
    ReduceTran result = new ReduceTran();
    JobConf newJobConf = cloneJobConf(rw);
    byte[] confBytes = KryoSerializer.serializeJobConf(newJobConf);
    HiveReduceFunction redFunc = new HiveReduceFunction(confBytes, sparkReporter);
    result.setReduceFunction(redFunc);
    return result;
  }

  private JobConf cloneJobConf(BaseWork work) throws Exception {
    JobConf cloned = new JobConf(jobConf);
    // Make sure we'll use a different plan path from the original one
    HiveConf.setVar(cloned, HiveConf.ConfVars.PLAN, "");
    try {
      cloned.setPartitionerClass((Class<? extends Partitioner>) (Class.forName(HiveConf.getVar(cloned,
        HiveConf.ConfVars.HIVEPARTITIONER))));
    } catch (ClassNotFoundException e) {
      String msg = "Could not find partitioner class: " + e.getMessage() + " which is specified by: " +
        HiveConf.ConfVars.HIVEPARTITIONER.varname;
      throw new IllegalArgumentException(msg, e);
    }
    if (work instanceof MapWork) {
      List<Path> inputPaths = Utilities.getInputPaths(cloned, (MapWork) work, scratchDir, context, false);
      Utilities.setInputPaths(cloned, inputPaths);
      Utilities.setMapWork(cloned, (MapWork) work, scratchDir, false);
      Utilities.createTmpDirs(cloned, (MapWork) work);
      if (work instanceof MergeFileWork) {
        MergeFileWork mergeFileWork = (MergeFileWork) work;
        cloned.set(Utilities.MAPRED_MAPPER_CLASS, MergeFileMapper.class.getName());
        cloned.set("mapred.input.format.class", mergeFileWork.getInputformat());
        cloned.setClass("mapred.output.format.class", MergeFileOutputFormat.class, FileOutputFormat.class);
      } else {
        cloned.set(Utilities.MAPRED_MAPPER_CLASS, ExecMapper.class.getName());
      }
    } else if (work instanceof ReduceWork) {
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

}
