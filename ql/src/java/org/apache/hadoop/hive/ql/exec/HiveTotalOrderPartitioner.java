/**
 * Copyright 2010 The Apache Software Foundation
 *
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

import org.apache.hadoop.hive.ql.io.HiveKey;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Partitioner;
import org.apache.hadoop.mapred.lib.TotalOrderPartitioner;

public class HiveTotalOrderPartitioner implements Partitioner<HiveKey, Object> {

  private Partitioner<BytesWritable, Object> partitioner
      = new TotalOrderPartitioner<BytesWritable, Object>();

  public void configure(JobConf job) {
    JobConf newconf = new JobConf(job);
    newconf.setMapOutputKeyClass(BytesWritable.class);
    partitioner.configure(newconf);
  }

  public int getPartition(HiveKey key, Object value, int numPartitions) {
    return partitioner.getPartition(key, value, numPartitions);
  }
}
