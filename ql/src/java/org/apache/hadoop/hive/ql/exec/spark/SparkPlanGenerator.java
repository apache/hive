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

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.io.merge.MergeFileMapper;
import org.apache.hadoop.hive.ql.io.merge.MergeFileOutputFormat;
import org.apache.hadoop.hive.ql.io.merge.MergeFileWork;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.hive.ql.io.HiveKey;
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
import org.apache.hadoop.hive.ql.plan.UnionWork;
import org.apache.hadoop.hive.ql.stats.StatsFactory;
import org.apache.hadoop.hive.ql.stats.StatsPublisher;
import org.apache.hadoop.hive.shims.ShimLoader;
import org.apache.hadoop.io.BytesWritable;
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

  public SparkPlanGenerator(JavaSparkContext sc, Context context,
      JobConf jobConf, Path scratchDir) {
    this.sc = sc;
    this.context = context;
    this.jobConf = jobConf;
    this.scratchDir = scratchDir;
  }

  public SparkPlan generate(SparkWork sparkWork) throws Exception {
    SparkPlan result = new SparkPlan();
    Map<BaseWork, SparkTran> createdTransMap = new HashMap<BaseWork, SparkTran>();

    for (BaseWork work : sparkWork.getAllWork()) {
      SparkTran tran;
      if (work instanceof MapWork) {
        JavaPairRDD<HiveKey, BytesWritable> inputRDD = generateRDD((MapWork)work);
        tran = generate(work, null);
        result.addInput(tran, inputRDD);
      } else {
        List<BaseWork> parentWorks = sparkWork.getParents(work);
        tran = generate(work, sparkWork.getEdgeProperty(parentWorks.get(0), work));
        for (BaseWork parentWork : parentWorks) {
          SparkTran parentTran = createdTransMap.get(parentWork);
          result.connect(parentTran, tran);
        }
      }
      createdTransMap.put(work, tran);
    }

    return result;
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

  public SparkTran generate(BaseWork work, SparkEdgeProperty edge) throws Exception {
    if (work instanceof MapWork) {
      MapWork mw = (MapWork) work;
      return generate(mw);
    } else if (work instanceof ReduceWork) {
      ReduceWork rw = (ReduceWork) work;
      ReduceTran tran = generate(rw);
      SparkShuffler shuffler = generate(edge);
      tran.setShuffler(shuffler);
      tran.setNumPartitions(edge.getNumPartitions());
      return tran;
    } else if (work instanceof UnionWork) {
      return new IdentityTran();
    } else {
      throw new HiveException("Unexpected work: " + work.getName());
    }
  }

  private JavaPairRDD<HiveKey, BytesWritable> generateRDD(MapWork mapWork)
      throws Exception {
    JobConf jobConf = cloneJobConf(mapWork);
    Class ifClass = getInputFormat(jobConf, mapWork);

    return sc.hadoopRDD(jobConf, ifClass, WritableComparable.class,
        Writable.class);
  }

  private SparkShuffler generate(SparkEdgeProperty edge) {
    Preconditions.checkArgument(!edge.isShuffleNone(),
        "AssertionError: SHUFFLE_NONE should only be used for UnionWork.");
    if (edge.isMRShuffle()) {
      return new SortByShuffler(false);
    } else if (edge.isShuffleSort()) {
      return new SortByShuffler(true);
    }
    return new GroupByShuffler();
  }

  private MapTran generate(MapWork mw) throws Exception {
    initStatsPublisher(mw);
    MapTran result = new MapTran();
    JobConf newJobConf = cloneJobConf(mw);
    byte[] confBytes = KryoSerializer.serializeJobConf(newJobConf);
    HiveMapFunction mapFunc = new HiveMapFunction(confBytes);
    result.setMapFunction(mapFunc);
    return result;
  }

  private ReduceTran generate(ReduceWork rw) throws Exception {
    ReduceTran result = new ReduceTran();
    JobConf newJobConf = cloneJobConf(rw);
    byte[] confBytes = KryoSerializer.serializeJobConf(newJobConf);
    HiveReduceFunction redFunc = new HiveReduceFunction(confBytes);
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
