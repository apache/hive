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
package org.apache.hadoop.hive.ql.exec.tez;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.CommonConfigurationKeys;
import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.serializer.JavaSerialization;
import org.apache.hadoop.io.serializer.WritableSerialization;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;
import org.apache.hadoop.mapreduce.MRJobConfig;

public class TezTotalOrderPartitioner implements Partitioner<HiveKey, Object>, Configurable {

  private static final Logger LOG = LoggerFactory.getLogger(TezTotalOrderPartitioner.class);

  private Partitioner<HiveKey, Object> partitioner;

  private static final String TEZ_RUNTIME_FRAMEWORK_PREFIX = "tez.runtime.framework.";
  public static final String TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS =
      TEZ_RUNTIME_FRAMEWORK_PREFIX + "num.expected.partitions";

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
    // make the HiveKey assumption
    conf.setMapOutputKeyClass(HiveKey.class);
    LOG.info(conf.get(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY));
    // remove the Tez fast serialization factory (TEZ-1288)
    // this one skips the len prefix, so that the sorter can assume byte-order ==
    // sort-order
    conf.setStrings(CommonConfigurationKeys.IO_SERIALIZATIONS_KEY,
        JavaSerialization.class.getName(), WritableSerialization.class.getName());
    int partitions = conf.getInt(TEZ_RUNTIME_NUM_EXPECTED_PARTITIONS, -1);
    // get the tez partitioning and feed it into the MR config
    conf.setInt(MRJobConfig.NUM_REDUCES, partitions);
    partitioner = new TotalOrderPartitioner<HiveKey, Object>();
    partitioner.configure(conf);
  }
}