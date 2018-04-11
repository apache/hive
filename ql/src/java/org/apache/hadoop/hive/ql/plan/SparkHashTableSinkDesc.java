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

package org.apache.hadoop.hive.ql.plan;

import org.apache.hadoop.hive.ql.plan.Explain.Level;
import org.apache.hadoop.hive.ql.plan.Explain.Vectorization;

/**
 * Map Join operator Descriptor implementation.
 *
 */
@Explain(displayName = "Spark HashTable Sink Operator")
public class SparkHashTableSinkDesc extends HashTableSinkDesc {
  private static final long serialVersionUID = 1L;

  // The position of this table
  private byte tag;

  public SparkHashTableSinkDesc() {
  }

  public SparkHashTableSinkDesc(MapJoinDesc clone) {
    super(clone);
  }

  public byte getTag() {
    return tag;
  }

  public void setTag(byte tag) {
    this.tag = tag;
  }

  public class SparkHashTableSinkOperatorExplainVectorization extends OperatorExplainVectorization {

    private final HashTableSinkDesc filterDesc;
    private final VectorSparkHashTableSinkDesc vectorHashTableSinkDesc;

    public SparkHashTableSinkOperatorExplainVectorization(HashTableSinkDesc filterDesc,
        VectorSparkHashTableSinkDesc vectorSparkHashTableSinkDesc) {
      // Native vectorization supported.
      super(vectorSparkHashTableSinkDesc, true);
      this.filterDesc = filterDesc;
      this.vectorHashTableSinkDesc = vectorSparkHashTableSinkDesc;
    }
  }

  @Explain(vectorization = Vectorization.OPERATOR, displayName = "Spark Hash Table Sink Vectorization", explainLevels = { Level.DEFAULT, Level.EXTENDED })
  public SparkHashTableSinkOperatorExplainVectorization getHashTableSinkVectorization() {
    VectorSparkHashTableSinkDesc vectorHashTableSinkDesc = (VectorSparkHashTableSinkDesc) getVectorDesc();
    if (vectorHashTableSinkDesc == null) {
      return null;
    }
    return new SparkHashTableSinkOperatorExplainVectorization(this, vectorHashTableSinkDesc);
  }

}
