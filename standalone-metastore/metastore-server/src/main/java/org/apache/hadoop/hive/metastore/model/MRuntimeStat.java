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

package org.apache.hadoop.hive.metastore.model;

import org.apache.hadoop.hive.metastore.api.RuntimeStat;

/**
 * Represents a runtime stat query entry.
 *
 * As a query may contain a large number of operatorstat entries; they are stored together in a single row in the metastore.
 * The number of operator stat entries this entity has; is shown in the weight column.
 */
public class MRuntimeStat {

  private int createTime;
  private int weight;
  private byte[] payload;

  public static MRuntimeStat fromThrift(RuntimeStat stat) {
    MRuntimeStat ret = new MRuntimeStat();
    ret.weight = stat.getWeight();
    ret.payload = stat.getPayload();
    ret.createTime = (int) (System.currentTimeMillis() / 1000);
    return ret;
  }

  public static RuntimeStat toThrift(MRuntimeStat stat) {
    RuntimeStat ret = new RuntimeStat();
    ret.setWeight(stat.weight);
    ret.setCreateTime(stat.createTime);
    ret.setPayload(stat.payload);
    return ret;
  }

  public int getWeight() {
    return weight;
  }

  public int getCreatedTime() {
    return createTime;
  }

}
