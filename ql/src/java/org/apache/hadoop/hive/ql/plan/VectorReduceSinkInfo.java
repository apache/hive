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
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * VectorGroupByAggregationInfo.
 *
 * A convenience data structure that has information needed to vectorize reduce sink.
 *
 * It is created by the Vectorizer when it is determining whether it can specialize so the
 * information doesn't have to be recreated again and against by the VectorReduceSinkOperator's
 * constructors and later during execution.
 */
public class VectorReduceSinkInfo {

  private static final long serialVersionUID = 1L;

  private boolean useUniformHash;

  private int[] reduceSinkKeyColumnMap;
  private TypeInfo[] reduceSinkKeyTypeInfos;
  private Type[] reduceSinkKeyColumnVectorTypes;
  private VectorExpression[] reduceSinkKeyExpressions;

  private int[] reduceSinkValueColumnMap;
  private TypeInfo[] reduceSinkValueTypeInfos;
  private Type[] reduceSinkValueColumnVectorTypes;
  private VectorExpression[] reduceSinkValueExpressions;

  private int[] reduceSinkBucketColumnMap;
  private TypeInfo[] reduceSinkBucketTypeInfos;
  private Type[] reduceSinkBucketColumnVectorTypes;
  private VectorExpression[] reduceSinkBucketExpressions;

  private int[] reduceSinkPartitionColumnMap;
  private TypeInfo[] reduceSinkPartitionTypeInfos;
  private Type[] reduceSinkPartitionColumnVectorTypes;
  private VectorExpression[] reduceSinkPartitionExpressions;

  public VectorReduceSinkInfo() {
    useUniformHash = false;

    reduceSinkKeyColumnMap = null;
    reduceSinkKeyTypeInfos = null;
    reduceSinkKeyColumnVectorTypes = null;
    reduceSinkKeyExpressions = null;

    reduceSinkValueColumnMap = null;
    reduceSinkValueTypeInfos = null;
    reduceSinkValueColumnVectorTypes = null;
    reduceSinkValueExpressions = null;

    reduceSinkBucketColumnMap = null;
    reduceSinkBucketTypeInfos = null;
    reduceSinkBucketColumnVectorTypes = null;
    reduceSinkBucketExpressions = null;

    reduceSinkPartitionColumnMap = null;
    reduceSinkPartitionTypeInfos = null;
    reduceSinkPartitionColumnVectorTypes = null;
    reduceSinkPartitionExpressions = null;
  }

  public boolean getUseUniformHash() {
    return useUniformHash;
  }

  public void setUseUniformHash(boolean useUniformHash) {
    this.useUniformHash = useUniformHash;
  }

  public int[] getReduceSinkKeyColumnMap() {
    return reduceSinkKeyColumnMap;
  }

  public void setReduceSinkKeyColumnMap(int[] reduceSinkKeyColumnMap) {
    this.reduceSinkKeyColumnMap = reduceSinkKeyColumnMap;
  }

  public TypeInfo[] getReduceSinkKeyTypeInfos() {
    return reduceSinkKeyTypeInfos;
  }

  public void setReduceSinkKeyTypeInfos(TypeInfo[] reduceSinkKeyTypeInfos) {
    this.reduceSinkKeyTypeInfos = reduceSinkKeyTypeInfos;
  }

  public Type[] getReduceSinkKeyColumnVectorTypes() {
    return reduceSinkKeyColumnVectorTypes;
  }

  public void setReduceSinkKeyColumnVectorTypes(Type[] reduceSinkKeyColumnVectorTypes) {
    this.reduceSinkKeyColumnVectorTypes = reduceSinkKeyColumnVectorTypes;
  }

  public VectorExpression[] getReduceSinkKeyExpressions() {
    return reduceSinkKeyExpressions;
  }

  public void setReduceSinkKeyExpressions(VectorExpression[] reduceSinkKeyExpressions) {
    this.reduceSinkKeyExpressions = reduceSinkKeyExpressions;
  }

  public int[] getReduceSinkValueColumnMap() {
    return reduceSinkValueColumnMap;
  }

  public void setReduceSinkValueColumnMap(int[] reduceSinkValueColumnMap) {
    this.reduceSinkValueColumnMap = reduceSinkValueColumnMap;
  }

  public TypeInfo[] getReduceSinkValueTypeInfos() {
    return reduceSinkValueTypeInfos;
  }

  public void setReduceSinkValueTypeInfos(TypeInfo[] reduceSinkValueTypeInfos) {
    this.reduceSinkValueTypeInfos = reduceSinkValueTypeInfos;
  }

  public Type[] getReduceSinkValueColumnVectorTypes() {
    return reduceSinkValueColumnVectorTypes;
  }

  public void setReduceSinkValueColumnVectorTypes(Type[] reduceSinkValueColumnVectorTypes) {
    this.reduceSinkValueColumnVectorTypes = reduceSinkValueColumnVectorTypes;
  }

  public VectorExpression[] getReduceSinkValueExpressions() {
    return reduceSinkValueExpressions;
  }

  public void setReduceSinkValueExpressions(VectorExpression[] reduceSinkValueExpressions) {
    this.reduceSinkValueExpressions = reduceSinkValueExpressions;
  }

  public int[] getReduceSinkBucketColumnMap() {
    return reduceSinkBucketColumnMap;
  }

  public void setReduceSinkBucketColumnMap(int[] reduceSinkBucketColumnMap) {
    this.reduceSinkBucketColumnMap = reduceSinkBucketColumnMap;
  }

  public TypeInfo[] getReduceSinkBucketTypeInfos() {
    return reduceSinkBucketTypeInfos;
  }

  public void setReduceSinkBucketTypeInfos(TypeInfo[] reduceSinkBucketTypeInfos) {
    this.reduceSinkBucketTypeInfos = reduceSinkBucketTypeInfos;
  }

  public Type[] getReduceSinkBucketColumnVectorTypes() {
    return reduceSinkBucketColumnVectorTypes;
  }

  public void setReduceSinkBucketColumnVectorTypes(Type[] reduceSinkBucketColumnVectorTypes) {
    this.reduceSinkBucketColumnVectorTypes = reduceSinkBucketColumnVectorTypes;
  }

  public VectorExpression[] getReduceSinkBucketExpressions() {
    return reduceSinkBucketExpressions;
  }

  public void setReduceSinkBucketExpressions(VectorExpression[] reduceSinkBucketExpressions) {
    this.reduceSinkBucketExpressions = reduceSinkBucketExpressions;
  }

  public int[] getReduceSinkPartitionColumnMap() {
    return reduceSinkPartitionColumnMap;
  }

  public void setReduceSinkPartitionColumnMap(int[] reduceSinkPartitionColumnMap) {
    this.reduceSinkPartitionColumnMap = reduceSinkPartitionColumnMap;
  }

  public TypeInfo[] getReduceSinkPartitionTypeInfos() {
    return reduceSinkPartitionTypeInfos;
  }

  public void setReduceSinkPartitionTypeInfos(TypeInfo[] reduceSinkPartitionTypeInfos) {
    this.reduceSinkPartitionTypeInfos = reduceSinkPartitionTypeInfos;
  }

  public Type[] getReduceSinkPartitionColumnVectorTypes() {
    return reduceSinkPartitionColumnVectorTypes;
  }

  public void setReduceSinkPartitionColumnVectorTypes(Type[] reduceSinkPartitionColumnVectorTypes) {
    this.reduceSinkPartitionColumnVectorTypes = reduceSinkPartitionColumnVectorTypes;
  }

  public VectorExpression[] getReduceSinkPartitionExpressions() {
    return reduceSinkPartitionExpressions;
  }

  public void setReduceSinkPartitionExpressions(VectorExpression[] reduceSinkPartitionExpressions) {
    this.reduceSinkPartitionExpressions = reduceSinkPartitionExpressions;
  }
}
