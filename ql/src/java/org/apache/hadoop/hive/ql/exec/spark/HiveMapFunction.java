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

package org.apache.hadoop.hive.ql.exec.spark;

import java.util.Iterator;

import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.hive.ql.io.merge.MergeFileMapper;
import org.apache.hadoop.io.BytesWritable;

import scala.Tuple2;

public class HiveMapFunction extends HivePairFlatMapFunction<
  Iterator<Tuple2<BytesWritable, BytesWritable>>, HiveKey, BytesWritable> {

  private static final long serialVersionUID = 1L;

  public HiveMapFunction(byte[] jobConfBuffer, SparkReporter sparkReporter) {
    super(jobConfBuffer, sparkReporter);
  }

  @SuppressWarnings("unchecked")
  @Override
  public Iterator<Tuple2<HiveKey, BytesWritable>>
  call(Iterator<Tuple2<BytesWritable, BytesWritable>> it) throws Exception {
    initJobConf();

    SparkRecordHandler mapRecordHandler;

    // need different record handler for MergeFileWork
    if (MergeFileMapper.class.getName().equals(jobConf.get(Utilities.MAPRED_MAPPER_CLASS))) {
      mapRecordHandler = new SparkMergeFileRecordHandler();
    } else {
      mapRecordHandler = new SparkMapRecordHandler();
    }

    HiveMapFunctionResultList result = new HiveMapFunctionResultList(it, mapRecordHandler);
    mapRecordHandler.init(jobConf, result, sparkReporter);

    return result;
  }

  @Override
  protected boolean isMap() {
    return true;
  }

}
