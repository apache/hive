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
import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.Context;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.io.HiveInputFormat;
import org.apache.hadoop.hive.ql.plan.BaseWork;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.hive.ql.plan.ReduceWork;
import org.apache.hadoop.hive.ql.plan.SparkEdgeProperty;
import org.apache.hadoop.hive.ql.plan.SparkWork;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.io.WritableComparable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;

public class SparkPlanGenerator {
  private JavaSparkContext sc;
  private JobConf jobConf;
  private Context context;
  private Path scratchDir;

  public SparkPlanGenerator(JavaSparkContext sc, Context context, JobConf jobConf, Path scratchDir) {
    this.sc = sc;
    this.context = context;
    this.jobConf = jobConf;
    this.scratchDir = scratchDir;
  }

  public SparkPlan generate(SparkWork sparkWork) throws Exception {
    SparkPlan plan = new SparkPlan();
    List<SparkTran> trans = new ArrayList<SparkTran>();
    Set<BaseWork> roots = sparkWork.getRoots();
    assert(roots != null && roots.size() == 1);
    BaseWork w = roots.iterator().next();
    MapWork mapWork = (MapWork) w;
    trans.add(generate(w));
    while (sparkWork.getChildren(w).size() > 0) {
      ReduceWork child = (ReduceWork) sparkWork.getChildren(w).get(0);
      SparkEdgeProperty edge = sparkWork.getEdgeProperty(w, child);
      SparkShuffler st = generate(edge);
      ReduceTran rt = generate(child);
      rt.setShuffler(st);
      trans.add(rt);
      w = child;
    }
    ChainedTran chainedTran = new ChainedTran(trans);
    plan.setTran(chainedTran);
    JavaPairRDD<BytesWritable, BytesWritable> input = generateRDD(mapWork);
    plan.setInput(input);
    return plan;
  }

  private JavaPairRDD<BytesWritable, BytesWritable> generateRDD(MapWork mapWork) throws Exception {
    List<Path> inputPaths = Utilities.getInputPaths(jobConf, mapWork, scratchDir, context, false);
    Utilities.setInputPaths(jobConf, inputPaths);
    Utilities.setMapWork(jobConf, mapWork, scratchDir, true);
    Class ifClass = HiveInputFormat.class;

    // The mapper class is expected by the HiveInputFormat.
    jobConf.set("mapred.mapper.class", ExecMapper.class.getName());
    return sc.hadoopRDD(jobConf, ifClass, WritableComparable.class, Writable.class);
  }

  private SparkTran generate(BaseWork bw) throws IOException {
    if (bw instanceof MapWork) {
      return generate((MapWork)bw);
    } else if (bw instanceof ReduceWork) {
      return generate((ReduceWork)bw);
    } else {
      throw new IllegalArgumentException("Only MapWork and ReduceWork are expected");
    }
  }

  private MapTran generate(MapWork mw) throws IOException {
    MapTran result = new MapTran();
    Utilities.setMapWork(jobConf, mw, scratchDir, true);
    Utilities.createTmpDirs(jobConf, mw);
    jobConf.set("mapred.mapper.class", ExecMapper.class.getName());
    byte[] confBytes = KryoSerializer.serializeJobConf(jobConf);
    HiveMapFunction mapFunc = new HiveMapFunction(confBytes);
    result.setMapFunction(mapFunc);
    return result;
  }

  private SparkShuffler generate(SparkEdgeProperty edge) {
    // TODO: create different shuffler based on edge prop.
    return new GroupByShuffler();
  }

  private ReduceTran generate(ReduceWork rw) throws IOException {
    ReduceTran result = new ReduceTran();
    Utilities.setReduceWork(jobConf, rw, scratchDir, true);
    Utilities.createTmpDirs(jobConf, rw);
    byte[] confBytes = KryoSerializer.serializeJobConf(jobConf);
    HiveReduceFunction mapFunc = new HiveReduceFunction(confBytes);
    result.setReduceFunction(mapFunc);
    return result;
  }

}
