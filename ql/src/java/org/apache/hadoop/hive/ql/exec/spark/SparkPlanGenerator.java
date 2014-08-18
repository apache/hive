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

import java.io.IOException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.google.common.base.Preconditions;
import org.apache.commons.lang.StringUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.ErrorMsg;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.io.BucketizedHiveInputFormat;
import org.apache.hadoop.hive.ql.io.CombineHiveInputFormat;
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
  private Map<BaseWork, SparkTran> unionWorkTrans = new HashMap<BaseWork, SparkTran>();

  public SparkPlanGenerator(JavaSparkContext sc, Context context,
      JobConf jobConf, Path scratchDir) {
    this.sc = sc;
    this.context = context;
    this.jobConf = jobConf;
    this.scratchDir = scratchDir;
  }

  public SparkPlan generate(SparkWork sparkWork) throws Exception {
    SparkPlan plan = new SparkPlan();
    GraphTran trans = new GraphTran();
    Set<BaseWork> roots = sparkWork.getRoots();
    for (BaseWork w : roots) {
      if (!(w instanceof MapWork)) {
        throw new Exception(
            "The roots in the SparkWork must be MapWork instances!");
      }
      MapWork mapWork = (MapWork) w;
      SparkTran tran = generate(w);
      JavaPairRDD<BytesWritable, BytesWritable> input = generateRDD(mapWork);
      trans.addTranWithInput(tran, input);

      while (sparkWork.getChildren(w).size() > 0) {
        BaseWork child = sparkWork.getChildren(w).get(0);
        if (child instanceof ReduceWork) {
          SparkEdgeProperty edge = sparkWork.getEdgeProperty(w, child);
          SparkShuffler st = generate(edge);
          ReduceTran rt = generate((ReduceWork) child);
          rt.setShuffler(st);
          rt.setNumPartitions(edge.getNumPartitions());
          trans.addTran(rt);
          trans.connect(tran, rt);
          w = child;
          tran = rt;
        } else if (child instanceof UnionWork) {
          if (unionWorkTrans.get(child) != null) {
            trans.connect(tran, unionWorkTrans.get(child));
            break;
          } else {
            SparkTran ut = generate((UnionWork) child);
            unionWorkTrans.put(child, ut);
            trans.addTran(ut);
            trans.connect(tran, ut);
            w = child;
            tran = ut;
          }
        }
      }
    }
    unionWorkTrans.clear();
    plan.setTran(trans);
    return plan;
  }

  private JavaPairRDD<BytesWritable, BytesWritable> generateRDD(MapWork mapWork)
      throws Exception {
    JobConf newJobConf = new JobConf(jobConf);
    List<Path> inputPaths = Utilities.getInputPaths(newJobConf, mapWork,
        scratchDir, context, false);
    Utilities.setInputPaths(newJobConf, inputPaths);
    Utilities.setMapWork(newJobConf, mapWork, scratchDir, true);
    Class ifClass = getInputFormat(mapWork);

    // The mapper class is expected by the HiveInputFormat.
    newJobConf.set("mapred.mapper.class", ExecMapper.class.getName());
    return sc.hadoopRDD(newJobConf, ifClass, WritableComparable.class,
        Writable.class);
  }

  private Class getInputFormat(MapWork mWork) throws HiveException {
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

  private SparkTran generate(BaseWork bw) throws Exception {
    // initialize stats publisher if necessary
    if (bw.isGatheringStats()) {
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
    if (bw instanceof MapWork) {
      return generate((MapWork) bw);
    } else if (bw instanceof ReduceWork) {
      return generate((ReduceWork) bw);
    } else {
      throw new IllegalArgumentException(
          "Only MapWork and ReduceWork are expected");
    }
  }

  private MapTran generate(MapWork mw) throws Exception {
    JobConf newJobConf = new JobConf(jobConf);
    MapTran result = new MapTran();

    List<Path> inputPaths = Utilities.getInputPaths(newJobConf, mw,
      scratchDir, context, false);
    Utilities.setInputPaths(newJobConf, inputPaths);

    Utilities.setMapWork(newJobConf, mw, scratchDir, true);
    Utilities.createTmpDirs(newJobConf, mw);
    newJobConf.set("mapred.mapper.class", ExecMapper.class.getName());
    byte[] confBytes = KryoSerializer.serializeJobConf(newJobConf);
    HiveMapFunction mapFunc = new HiveMapFunction(confBytes);
    result.setMapFunction(mapFunc);
    return result;
  }

  private SparkShuffler generate(SparkEdgeProperty edge) {
    Preconditions.checkArgument(!edge.isShuffleNone(),
        "AssertionError: SHUFFLE_NONE should only be used for UnionWork.");
    if (edge.isShuffleSort()) {
      return new SortByShuffler();
    }
    return new GroupByShuffler();
  }

  private ReduceTran generate(ReduceWork rw) throws IOException {
    ReduceTran result = new ReduceTran();
    // Clone jobConf for each ReduceWork so we can have multiple of them
    JobConf newJobConf = new JobConf(jobConf);
    // Make sure we'll use a different plan path from the original one
    HiveConf.setVar(newJobConf, HiveConf.ConfVars.PLAN, "");
    Utilities.setReduceWork(newJobConf, rw, scratchDir, false);
    Utilities.createTmpDirs(newJobConf, rw);
    byte[] confBytes = KryoSerializer.serializeJobConf(newJobConf);
    HiveReduceFunction redFunc = new HiveReduceFunction(confBytes);
    result.setReduceFunction(redFunc);
    return result;
  }

  private UnionTran generate(UnionWork uw) {
    UnionTran result = new UnionTran();
    return result;
  }

}
