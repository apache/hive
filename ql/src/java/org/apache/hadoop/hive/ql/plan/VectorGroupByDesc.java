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

package org.apache.hadoop.hive.ql.plan;

/**
 * VectorGroupByDesc.
 *
 * Extra parameters beyond GroupByDesc just for the VectorGroupByOperator.
 *
 * We don't extend GroupByDesc because the base OperatorDesc doesn't support
 * clone and adding it is a lot work for little gain.
 */
public class VectorGroupByDesc extends AbstractVectorDesc  {

  private static long serialVersionUID = 1L;

  private boolean isReduce;
  private boolean isVectorGroupBatches;
  private boolean isVectorOutput;

  public VectorGroupByDesc() {
    this.isReduce = false;
    this.isVectorGroupBatches = false;
    this.isVectorOutput = false;
  }

  public boolean isReduce() {
    return isReduce;
  }

  public void setIsReduce(boolean isReduce) {
    this.isReduce = isReduce;
  }

  public boolean isVectorGroupBatches() {
    return isVectorGroupBatches;
  }

  public void setVectorGroupBatches(boolean isVectorGroupBatches) {
    this.isVectorGroupBatches = isVectorGroupBatches;
  }

  public boolean isVectorOutput() {
    return isVectorOutput;
  }

  public void setVectorOutput(boolean isVectorOutput) {
    this.isVectorOutput = isVectorOutput;
  }
}
