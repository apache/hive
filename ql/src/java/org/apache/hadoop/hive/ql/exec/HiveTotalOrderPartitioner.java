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
package org.apache.hadoop.hive.ql.exec;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;

public class HiveTotalOrderPartitioner implements Partitioner<HiveKey, Object>, Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(HiveTotalOrderPartitioner.class);

  private Partitioner<BytesWritable, Object> partitioner;

  @Override
  public void configure(JobConf job) {
    if (partitioner == null) {
      configurePartitioner(new JobConf(job));
    }
  }

  @Override
  public void setConf(Configuration conf) {
    // walk-around of TEZ-1403
    if (partitioner == null) {
      configurePartitioner(new JobConf(conf));
    }
  }

  public int getPartition(HiveKey key, Object value, int numPartitions) {
    return partitioner.getPartition(key, value, numPartitions);
  }

  @Override
  public Configuration getConf() {
    return null;
  }

  private void configurePartitioner(JobConf conf) {
    LOG.info(TotalOrderPartitioner.getPartitionFile(conf));
    conf.setMapOutputKeyClass(BytesWritable.class);
    partitioner = new TotalOrderPartitioner<BytesWritable, Object>();
    partitioner.configure(conf);
  }
}
