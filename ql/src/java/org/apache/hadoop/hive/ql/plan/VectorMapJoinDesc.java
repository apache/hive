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

import java.util.List;

import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
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

  public static enum VectorMapJoinVariation {
    NONE,
    INNER_BIG_ONLY,
    INNER,
    LEFT_SEMI,
    OUTER
  }

  private HashTableImplementationType hashTableImplementationType;
  private HashTableKind hashTableKind;
  private HashTableKeyType hashTableKeyType;
  private VectorMapJoinVariation vectorMapJoinVariation;
  private boolean minMaxEnabled;

  private VectorExpression[] allBigTableKeyExpressions;
  private VectorExpression[] allBigTableValueExpressions;

  private VectorMapJoinInfo vectorMapJoinInfo;

  public VectorMapJoinDesc() {
    hashTableImplementationType = HashTableImplementationType.NONE;
    hashTableKind = HashTableKind.NONE;
    hashTableKeyType = HashTableKeyType.NONE;
    vectorMapJoinVariation = VectorMapJoinVariation.NONE;
    minMaxEnabled = false;

    allBigTableKeyExpressions = null;
    allBigTableValueExpressions = null;

    vectorMapJoinInfo = null;
  }

  @Override
  public VectorMapJoinDesc clone() {
    VectorMapJoinDesc clone = new VectorMapJoinDesc();
    clone.hashTableImplementationType = this.hashTableImplementationType;
    clone.hashTableKind = this.hashTableKind;
    clone.hashTableKeyType = this.hashTableKeyType;
    clone.vectorMapJoinVariation = this.vectorMapJoinVariation;
    clone.minMaxEnabled = this.minMaxEnabled;
    if (vectorMapJoinInfo != null) {
      throw new RuntimeException("Cloning VectorMapJoinInfo not supported");
    }
    return clone;
  }

  public HashTableImplementationType getHashTableImplementationType() {
    return hashTableImplementationType;
  }

  public void setHashTableImplementationType(HashTableImplementationType hashTableImplementationType) {
    this.hashTableImplementationType = hashTableImplementationType;
  }

  public HashTableKind getHashTableKind() {
    return hashTableKind;
  }

  public void setHashTableKind(HashTableKind hashTableKind) {
    this.hashTableKind = hashTableKind;
  }

  public HashTableKeyType getHashTableKeyType() {
    return hashTableKeyType;
  }

  public void setHashTableKeyType(HashTableKeyType hashTableKeyType) {
    this.hashTableKeyType = hashTableKeyType;
  }

  public VectorMapJoinVariation getVectorMapJoinVariation() {
    return vectorMapJoinVariation;
  }

  public void setVectorMapJoinVariation(VectorMapJoinVariation vectorMapJoinVariation) {
    this.vectorMapJoinVariation = vectorMapJoinVariation;
  }

  public boolean getMinMaxEnabled() {
    return minMaxEnabled;
  }

  public void setMinMaxEnabled(boolean minMaxEnabled) {
    this.minMaxEnabled = minMaxEnabled;
  }

  public VectorExpression[] getAllBigTableKeyExpressions() {
    return allBigTableKeyExpressions;
  }

  public void setAllBigTableKeyExpressions(VectorExpression[] allBigTableKeyExpressions) {
    this.allBigTableKeyExpressions = allBigTableKeyExpressions;
  }

  public VectorExpression[] getAllBigTableValueExpressions() {
    return allBigTableValueExpressions;
  }

  public void setAllBigTableValueExpressions(VectorExpression[] allBigTableValueExpressions) {
    this.allBigTableValueExpressions = allBigTableValueExpressions;
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
  private boolean supportsValueTypes;
  private List<String> notSupportedValueTypes;
  private boolean smallTableExprVectorizes;
  private boolean outerJoinHasNoKeys;

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
  public void setSupportsValueTypes(boolean supportsValueTypes) {
    this.supportsValueTypes = supportsValueTypes;
  }
  public boolean getSupportsValueTypes() {
    return supportsValueTypes;
  }
  public void setNotSupportedValueTypes(List<String> notSupportedValueTypes) {
    this.notSupportedValueTypes = notSupportedValueTypes;
  }
  public List<String> getNotSupportedValueTypes() {
    return notSupportedValueTypes;
  }
  public void setSmallTableExprVectorizes(boolean smallTableExprVectorizes) {
    this.smallTableExprVectorizes = smallTableExprVectorizes;
  }
  public boolean getSmallTableExprVectorizes() {
    return smallTableExprVectorizes;
  }
  public void setOuterJoinHasNoKeys(boolean outerJoinHasNoKeys) {
    this.outerJoinHasNoKeys = outerJoinHasNoKeys;
  }
  public boolean getOuterJoinHasNoKeys() {
    return outerJoinHasNoKeys;
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
