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

package org.apache.hadoop.hive.ql.exec.spark;

import java.util.Iterator;

import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.io.merge.MergeFileMapper;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.spark.TaskContext;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class HiveMapFunction implements PairFlatMapFunction<Iterator<Tuple2<BytesWritable, BytesWritable>>,
    HiveKey, BytesWritable> {
  private static final long serialVersionUID = 1L;

  private transient JobConf jobConf;

  private byte[] buffer;

  public HiveMapFunction(byte[] buffer) {
    this.buffer = buffer;
  }

  @Override
  public Iterable<Tuple2<HiveKey, BytesWritable>>
  call(Iterator<Tuple2<BytesWritable, BytesWritable>> it) throws Exception {
    if (jobConf == null) {
      jobConf = KryoSerializer.deserializeJobConf(this.buffer);
      SparkUtilities.setTaskInfoInJobConf(jobConf, TaskContext.get());
    }

    SparkRecordHandler mapRecordHandler;

    // need different record handler for MergeFileWork
    if (MergeFileMapper.class.getName().equals(jobConf.get(Utilities.MAPRED_MAPPER_CLASS))) {
      mapRecordHandler = new SparkMergeFileRecordHandler();
    } else {
      mapRecordHandler = new SparkMapRecordHandler();
    }

    HiveMapFunctionResultList result = new HiveMapFunctionResultList(jobConf, it, mapRecordHandler);
    //TODO we need to implement a Spark specified Reporter to collect stats, refer to HIVE-7709.
    mapRecordHandler.init(jobConf, result, Reporter.NULL);

    return result;
  }

}
