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

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.ql.exec.Utilities;
import org.apache.hadoop.hive.ql.exec.mr.ExecMapper;
import org.apache.hadoop.hive.ql.plan.MapWork;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class HiveMapFunction implements PairFlatMapFunction<Iterator<Tuple2<BytesWritable, BytesWritable>>,
BytesWritable, BytesWritable> {
  private static final long serialVersionUID = 1L;
  
  private transient ExecMapper mapper;
  private transient SparkCollector collector;
  private transient JobConf jobConf;

  private byte[] buffer;

  public HiveMapFunction(byte[] buffer) {
    this.buffer = buffer;
  }

  @Override
  public Iterable<Tuple2<BytesWritable, BytesWritable>> 
  call(Iterator<Tuple2<BytesWritable, BytesWritable>> it) throws Exception {
    if (jobConf == null) {
      jobConf = KryoSerializer.deserializeJobConf(this.buffer);
      mapper = new ExecMapper();
      mapper.configure(jobConf);
      collector = new SparkCollector();
    }

    collector.clear();
    while(it.hasNext() && !ExecMapper.getDone()) {
      Tuple2<BytesWritable, BytesWritable> input = it.next();
      mapper.map(input._1(), input._2(), collector, Reporter.NULL);
    }
    
    mapper.close();
    return collector.getResult();
  }
  
}
