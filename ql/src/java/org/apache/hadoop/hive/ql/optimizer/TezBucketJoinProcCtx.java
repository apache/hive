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

package org.apache.hadoop.hive.ql.optimizer;

import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.conf.HiveConf;

public class TezBucketJoinProcCtx extends BucketJoinProcCtx {
  // determines if we need to use custom edge or one-to-one edge
  boolean isSubQuery = false;
  int numBuckets = -1;

  public TezBucketJoinProcCtx(HiveConf conf) {
    super(conf);
  }

  public void setIsSubQuery (boolean isSubQuery) {
    this.isSubQuery = isSubQuery;
  }

  public boolean isSubQuery () {
    return isSubQuery;
  }

  public void setNumBuckets(int numBuckets) {
    this.numBuckets = numBuckets;
  }

  public Integer getNumBuckets() {
    return numBuckets;
  }
}
