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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;

/**
 * VectorGroupByAggregrationInfo.
 *
 * A convenience data structure that has information needed to vectorize reduce sink.
 *
 * It is created by the Vectorizer when it is determining whether it can specialize so the
 * information doesn't have to be recreated again and against by the VectorPTFOperator's
 * constructors and later during execution.
 */
public class VectorPTFInfo {

  private int[] outputColumnMap;

  private int[] orderColumnMap;
  private Type[] orderColumnVectorTypes;
  private VectorExpression[] orderExpressions;

  private int[] partitionColumnMap;
  private Type[] partitionColumnVectorTypes;
  private VectorExpression[] partitionExpressions;

  private VectorExpression[] evaluatorInputExpressions;
  private Type[] evaluatorInputColumnVectorTypes;

  private int[] keyInputColumnMap;
  private int[] nonKeyInputColumnMap;

  public VectorPTFInfo() {

    outputColumnMap = null;

    orderColumnMap = null;
    orderColumnVectorTypes = null;
    orderExpressions = null;

    partitionColumnMap = null;
    partitionColumnVectorTypes = null;
    partitionExpressions = null;

    evaluatorInputExpressions = null;
    evaluatorInputColumnVectorTypes = null;

    keyInputColumnMap = null;
    nonKeyInputColumnMap = null;
  }

  public int[] getOutputColumnMap() {
    return outputColumnMap;
  }

  public void setOutputColumnMap(int[] outputColumnMap) {
    this.outputColumnMap = outputColumnMap;
  }

  public int[] getOrderColumnMap() {
    return orderColumnMap;
  }

  public void setOrderColumnMap(int[] orderColumnMap) {
    this.orderColumnMap = orderColumnMap;
  }

  public Type[] getOrderColumnVectorTypes() {
    return orderColumnVectorTypes;
  }

  public void setOrderColumnVectorTypes(Type[] orderColumnVectorTypes) {
    this.orderColumnVectorTypes = orderColumnVectorTypes;
  }

  public VectorExpression[] getOrderExpressions() {
    return orderExpressions;
  }

  public void setOrderExpressions(VectorExpression[] orderExpressions) {
    this.orderExpressions = orderExpressions;
  }

  public int[] getPartitionColumnMap() {
    return partitionColumnMap;
  }

  public void setPartitionColumnMap(int[] partitionColumnMap) {
    this.partitionColumnMap = partitionColumnMap;
  }

  public Type[] getPartitionColumnVectorTypes() {
    return partitionColumnVectorTypes;
  }

  public void setPartitionColumnVectorTypes(Type[] partitionColumnVectorTypes) {
    this.partitionColumnVectorTypes = partitionColumnVectorTypes;
  }

  public VectorExpression[] getPartitionExpressions() {
    return partitionExpressions;
  }

  public void setPartitionExpressions(VectorExpression[] partitionExpressions) {
    this.partitionExpressions = partitionExpressions;
  }

  public VectorExpression[] getEvaluatorInputExpressions() {
    return evaluatorInputExpressions;
  }

  public void setEvaluatorInputExpressions(VectorExpression[] evaluatorInputExpressions) {
    this.evaluatorInputExpressions = evaluatorInputExpressions;
  }

  public Type[] getEvaluatorInputColumnVectorTypes() {
    return evaluatorInputColumnVectorTypes;
  }

  public void setEvaluatorInputColumnVectorTypes(Type[] evaluatorInputColumnVectorTypes) {
    this.evaluatorInputColumnVectorTypes = evaluatorInputColumnVectorTypes;
  }

  public int[] getKeyInputColumnMap() {
    return keyInputColumnMap;
  }

  public void setKeyInputColumnMap(int[] keyInputColumnMap) {
    this.keyInputColumnMap = keyInputColumnMap;
  }

  public int[] getNonKeyInputColumnMap() {
    return nonKeyInputColumnMap;
  }

  public void setNonKeyInputColumnMap(int[] nonKeyInputColumnMap) {
    this.nonKeyInputColumnMap = nonKeyInputColumnMap;
  }
}
