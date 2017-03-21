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
 * VectorReduceSinkDesc.
 *
 * Extra parameters beyond ReduceSinkDesc just for the VectorReduceSinkOperator.
 *
 * We don't extend ReduceSinkDesc because the base OperatorDesc doesn't support
 * clone and adding it is a lot work for little gain.
 */
public class VectorReduceSinkDesc extends AbstractVectorDesc  {

  private static final long serialVersionUID = 1L;

  public static enum ReduceSinkKeyType {
    NONE,
    LONG,
    STRING,
    MULTI_KEY
  }

  private ReduceSinkKeyType reduceSinkKeyType;

  private VectorReduceSinkInfo vectorReduceSinkInfo;

  public VectorReduceSinkDesc() {
    reduceSinkKeyType = ReduceSinkKeyType.NONE;
    vectorReduceSinkInfo = null;
  }

  public ReduceSinkKeyType reduceSinkKeyType() {
    return reduceSinkKeyType;
  }

  public void setReduceSinkKeyType(ReduceSinkKeyType reduceSinkKeyType) {
    this.reduceSinkKeyType = reduceSinkKeyType;
  }

  public void setVectorReduceSinkInfo(VectorReduceSinkInfo vectorReduceSinkInfo) {
    this.vectorReduceSinkInfo = vectorReduceSinkInfo;
  }

  public VectorReduceSinkInfo getVectorReduceSinkInfo() {
    return vectorReduceSinkInfo;
  }

  private boolean isVectorizationReduceSinkNativeEnabled;
  private String engine;
  private boolean hasTopN;
  private boolean hasDistinctColumns;
  private boolean isKeyBinarySortable;
  private boolean isValueLazyBinary;
  private boolean isUnexpectedCondition;

  /*
   * The following conditions are for native Vector ReduceSink.
   */
  public void setIsVectorizationReduceSinkNativeEnabled(boolean isVectorizationReduceSinkNativeEnabled) {
    this.isVectorizationReduceSinkNativeEnabled = isVectorizationReduceSinkNativeEnabled;
  }
  public boolean getIsVectorizationReduceSinkNativeEnabled() {
    return isVectorizationReduceSinkNativeEnabled;
  }
  public void setEngine(String engine) {
    this.engine = engine;
  }
  public String getEngine() {
    return engine;
  }
  public void setHasTopN(boolean hasTopN) {
    this.hasTopN = hasTopN;
  }
  public boolean getHasTopN() {
    return hasTopN;
  }
  public void setHasDistinctColumns(boolean hasDistinctColumns) {
    this.hasDistinctColumns = hasDistinctColumns;
  }
  public boolean getHasDistinctColumns() {
    return hasDistinctColumns;
  }
  public void setIsKeyBinarySortable(boolean isKeyBinarySortable) {
    this.isKeyBinarySortable = isKeyBinarySortable;
  }
  public boolean getIsKeyBinarySortable() {
    return isKeyBinarySortable;
  }
  public void setIsValueLazyBinary(boolean isValueLazyBinary) {
    this.isValueLazyBinary = isValueLazyBinary;
  }
  public boolean getIsValueLazyBinary() {
    return isValueLazyBinary;
  }
  public void setIsUnexpectedCondition(boolean isUnexpectedCondition) {
    this.isUnexpectedCondition = isUnexpectedCondition;
  }
  public boolean getIsUnexpectedCondition() {
    return isUnexpectedCondition;
  }
}
