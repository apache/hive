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

import org.apache.hadoop.hive.ql.exec.vector.ColumnVector.Type;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * VectorGroupByAggregrationInfo.
 *
 * A convenience data structure that has information needed to vectorize reduce sink.
 *
 * It is created by the Vectorizer when it is determining whether it can specialize so the
 * information doesn't have to be recreated again and agains by the VectorReduceSinkOperator's
 * constructors and later during execution.
 */
public class VectorReduceSinkInfo {

  private static long serialVersionUID = 1L;

  private int[] reduceSinkKeyColumnMap;
  private TypeInfo[] reduceSinkKeyTypeInfos;
  private Type[] reduceSinkKeyColumnVectorTypes;
  private VectorExpression[] reduceSinkKeyExpressions;

  private int[] reduceSinkValueColumnMap;
  private TypeInfo[] reduceSinkValueTypeInfos;
  private Type[] reduceSinkValueColumnVectorTypes;
  private VectorExpression[] reduceSinkValueExpressions;

  public VectorReduceSinkInfo() {
    reduceSinkKeyColumnMap = null;
    reduceSinkKeyTypeInfos = null;
    reduceSinkKeyColumnVectorTypes = null;
    reduceSinkKeyExpressions = null;

    reduceSinkValueColumnMap = null;
    reduceSinkValueTypeInfos = null;
    reduceSinkValueColumnVectorTypes = null;
    reduceSinkValueExpressions = null;
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
}
