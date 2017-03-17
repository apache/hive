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

import java.util.List;

import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

import com.google.common.base.Preconditions;

/**
 * VectorGroupByDesc.
 *
 * Extra parameters beyond MapJoinDesc just for the vector map join operators.
 *
 * We don't extend MapJoinDesc because the base OperatorDesc doesn't support
 * clone and adding it is a lot work for little gain.
 */
public class VectorMapJoinDesc extends AbstractVectorDesc  {

  private static final long serialVersionUID = 1L;

  public static enum HashTableImplementationType {
    NONE,
    OPTIMIZED,
    FAST
  }

  public static enum HashTableKind {
    NONE,
    HASH_SET,
    HASH_MULTISET,
    HASH_MAP
  }

  public static enum HashTableKeyType {
    NONE,
    BOOLEAN,
    BYTE,
    SHORT,
    INT,
    LONG,
    STRING,
    MULTI_KEY;

    public PrimitiveTypeInfo getPrimitiveTypeInfo() {
      switch (this) {
      case BOOLEAN:
        return TypeInfoFactory.booleanTypeInfo;
      case BYTE:
        return TypeInfoFactory.byteTypeInfo;
      case INT:
        return TypeInfoFactory.intTypeInfo;
      case LONG:
        return TypeInfoFactory.longTypeInfo;
      case NONE:
        return TypeInfoFactory.voidTypeInfo;
      case SHORT:
        return TypeInfoFactory.shortTypeInfo;
      case STRING:
        return TypeInfoFactory.stringTypeInfo;
      case MULTI_KEY:
      default:
        return null;
      }
    }
  }

  public static enum OperatorVariation {
    NONE,
    INNER_BIG_ONLY,
    INNER,
    LEFT_SEMI,
    OUTER
  }

  private HashTableImplementationType hashTableImplementationType;
  private HashTableKind hashTableKind;
  private HashTableKeyType hashTableKeyType;
  private OperatorVariation operatorVariation;
  private boolean minMaxEnabled;

  private VectorMapJoinInfo vectorMapJoinInfo;

  public VectorMapJoinDesc() {
    hashTableImplementationType = HashTableImplementationType.NONE;
    hashTableKind = HashTableKind.NONE;
    hashTableKeyType = HashTableKeyType.NONE;
    operatorVariation = OperatorVariation.NONE;
    minMaxEnabled = false;
    vectorMapJoinInfo = null;
  }

  @Override
  public VectorMapJoinDesc clone() {
    VectorMapJoinDesc clone = new VectorMapJoinDesc();
    clone.hashTableImplementationType = this.hashTableImplementationType;
    clone.hashTableKind = this.hashTableKind;
    clone.hashTableKeyType = this.hashTableKeyType;
    clone.operatorVariation = this.operatorVariation;
    clone.minMaxEnabled = this.minMaxEnabled;
    if (vectorMapJoinInfo != null) {
      throw new RuntimeException("Cloning VectorMapJoinInfo not supported");
    }
    return clone;
  }

  public HashTableImplementationType hashTableImplementationType() {
    return hashTableImplementationType;
  }

  public void setHashTableImplementationType(HashTableImplementationType hashTableImplementationType) {
    this.hashTableImplementationType = hashTableImplementationType;
  }

  public HashTableKind hashTableKind() {
    return hashTableKind;
  }

  public void setHashTableKind(HashTableKind hashTableKind) {
    this.hashTableKind = hashTableKind;
  }

  public HashTableKeyType hashTableKeyType() {
    return hashTableKeyType;
  }

  public void setHashTableKeyType(HashTableKeyType hashTableKeyType) {
    this.hashTableKeyType = hashTableKeyType;
  }

  public OperatorVariation operatorVariation() {
    return operatorVariation;
  }

  public void setOperatorVariation(OperatorVariation operatorVariation) {
    this.operatorVariation = operatorVariation;
  }

  public boolean minMaxEnabled() {
    return minMaxEnabled;
  }

  public void setMinMaxEnabled(boolean minMaxEnabled) {
    this.minMaxEnabled = minMaxEnabled;
  }

  public void setVectorMapJoinInfo(VectorMapJoinInfo vectorMapJoinInfo) {
    Preconditions.checkState(vectorMapJoinInfo != null);
    this.vectorMapJoinInfo = vectorMapJoinInfo;
  }

  public VectorMapJoinInfo getVectorMapJoinInfo() {
    return vectorMapJoinInfo;
  }

  private boolean useOptimizedTable;
  private boolean isVectorizationMapJoinNativeEnabled;
  private String engine;
  private boolean oneMapJoinCondition;
  private boolean hasNullSafes;
  private boolean isFastHashTableEnabled;
  private boolean isHybridHashJoin;
  private boolean supportsKeyTypes;
  private List<String> notSupportedKeyTypes;
  private boolean smallTableExprVectorizes;

  public void setUseOptimizedTable(boolean useOptimizedTable) {
    this.useOptimizedTable = useOptimizedTable;
  }
  public boolean getUseOptimizedTable() {
    return useOptimizedTable;
  }
  public void setIsVectorizationMapJoinNativeEnabled(boolean isVectorizationMapJoinNativeEnabled) {
    this.isVectorizationMapJoinNativeEnabled = isVectorizationMapJoinNativeEnabled;
  }
  public boolean getIsVectorizationMapJoinNativeEnabled() {
    return isVectorizationMapJoinNativeEnabled;
  }
  public void setEngine(String engine) {
    this.engine = engine;
  }
  public String getEngine() {
    return engine;
  }
  public void setOneMapJoinCondition(boolean oneMapJoinCondition) {
    this.oneMapJoinCondition = oneMapJoinCondition;
  }
  public boolean getOneMapJoinCondition() {
    return oneMapJoinCondition;
  }
  public void setHasNullSafes(boolean hasNullSafes) {
    this.hasNullSafes = hasNullSafes;
  }
  public boolean getHasNullSafes() {
    return hasNullSafes;
  }
  public void setSupportsKeyTypes(boolean supportsKeyTypes) {
    this.supportsKeyTypes = supportsKeyTypes;
  }
  public boolean getSupportsKeyTypes() {
    return supportsKeyTypes;
  }
  public void setNotSupportedKeyTypes(List<String> notSupportedKeyTypes) {
    this.notSupportedKeyTypes = notSupportedKeyTypes;
  }
  public List<String> getNotSupportedKeyTypes() {
    return notSupportedKeyTypes;
  }
  public void setSmallTableExprVectorizes(boolean smallTableExprVectorizes) {
    this.smallTableExprVectorizes = smallTableExprVectorizes;
  }
  public boolean getSmallTableExprVectorizes() {
    return smallTableExprVectorizes;
  }

  public void setIsFastHashTableEnabled(boolean isFastHashTableEnabled) {
    this.isFastHashTableEnabled = isFastHashTableEnabled;
  }
  public boolean getIsFastHashTableEnabled() {
    return isFastHashTableEnabled;
  }
  public void setIsHybridHashJoin(boolean isHybridHashJoin) {
    this.isHybridHashJoin = isHybridHashJoin;
  }
  public boolean getIsHybridHashJoin() {
    return isHybridHashJoin;
  }

}
