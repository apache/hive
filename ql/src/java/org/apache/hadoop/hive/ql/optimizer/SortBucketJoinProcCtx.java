/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
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
import org.apache.hadoop.hive.ql.plan.ExprNodeDesc;


public class SortBucketJoinProcCtx extends BucketJoinProcCtx {
  private String[] srcs;
  private int bigTablePosition;
  private Map<Byte, List<ExprNodeDesc>> keyExprMap;

  public SortBucketJoinProcCtx(HiveConf conf) {
    super(conf);
  }

  public String[] getSrcs() {
    return srcs;
  }

  public void setSrcs(String[] srcs) {
    this.srcs = srcs;
  }

  public int getBigTablePosition() {
    return bigTablePosition;
  }

  public void setBigTablePosition(int bigTablePosition) {
    this.bigTablePosition = bigTablePosition;
  }

  public Map<Byte, List<ExprNodeDesc>> getKeyExprMap() {
    return keyExprMap;
  }

  public void setKeyExprMap(Map<Byte, List<ExprNodeDesc>> keyExprMap) {
    this.keyExprMap = keyExprMap;
  }
}
