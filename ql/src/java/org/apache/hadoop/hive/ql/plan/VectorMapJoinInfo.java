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

import java.util.Arrays;

import org.apache.hadoop.hive.ql.exec.vector.VectorColumnOutputMapping;
import org.apache.hadoop.hive.ql.exec.vector.VectorColumnSourceMapping;
import org.apache.hadoop.hive.ql.exec.vector.expressions.VectorExpression;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

/**
 * VectorMapJoinInfo.
 *
 * A convenience data structure that has information needed to vectorize map join.
 *
 * It is created by the Vectorizer when it is determining whether it can specialize so the
 * information doesn't have to be recreated again and again by the VectorMapJoinOperator's
 * constructors and later during execution.
 */
public class VectorMapJoinInfo {

  private static final long serialVersionUID = 1L;

  private int[] bigTableKeyColumnMap;
  private String[] bigTableKeyColumnNames;
  private TypeInfo[] bigTableKeyTypeInfos;
  private VectorExpression[] slimmedBigTableKeyExpressions;

  private int[] bigTableValueColumnMap;
  private String[] bigTableValueColumnNames;
  private TypeInfo[] bigTableValueTypeInfos;
  private VectorExpression[] slimmedBigTableValueExpressions;

  private VectorColumnOutputMapping bigTableRetainedMapping;
  private VectorColumnOutputMapping bigTableOuterKeyMapping;
  private VectorColumnSourceMapping smallTableMapping;

  private VectorColumnSourceMapping projectionMapping;

  public VectorMapJoinInfo() {
    bigTableKeyColumnMap = null;
    bigTableKeyColumnNames = null;
    bigTableKeyTypeInfos = null;
    slimmedBigTableKeyExpressions = null;

    bigTableValueColumnMap = null;
    bigTableValueColumnNames = null;
    bigTableValueTypeInfos = null;
    slimmedBigTableValueExpressions = null;

    bigTableRetainedMapping = null;
    bigTableOuterKeyMapping = null;
    smallTableMapping = null;

    projectionMapping = null;
  }

  public int[] getBigTableKeyColumnMap() {
    return bigTableKeyColumnMap;
  }

  public void setBigTableKeyColumnMap(int[] bigTableKeyColumnMap) {
    this.bigTableKeyColumnMap = bigTableKeyColumnMap;
  }

  public String[] getBigTableKeyColumnNames() {
    return bigTableKeyColumnNames;
  }

  public void setBigTableKeyColumnNames(String[] bigTableKeyColumnNames) {
    this.bigTableKeyColumnNames = bigTableKeyColumnNames;
  }

  public TypeInfo[] getBigTableKeyTypeInfos() {
    return bigTableKeyTypeInfos;
  }

  public void setBigTableKeyTypeInfos(TypeInfo[] bigTableKeyTypeInfos) {
    this.bigTableKeyTypeInfos = bigTableKeyTypeInfos;
  }

  public VectorExpression[] getSlimmedBigTableKeyExpressions() {
    return slimmedBigTableKeyExpressions;
  }

  public void setSlimmedBigTableKeyExpressions(VectorExpression[] slimmedBigTableKeyExpressions) {
    this.slimmedBigTableKeyExpressions = slimmedBigTableKeyExpressions;
  }


  public int[] getBigTableValueColumnMap() {
    return bigTableValueColumnMap;
  }

  public void setBigTableValueColumnMap(int[] bigTableValueColumnMap) {
    this.bigTableValueColumnMap = bigTableValueColumnMap;
  }

  public String[] getBigTableValueColumnNames() {
    return bigTableValueColumnNames;
  }

  public void setBigTableValueColumnNames(String[] bigTableValueColumnNames) {
    this.bigTableValueColumnNames = bigTableValueColumnNames;
  }

  public TypeInfo[] getBigTableValueTypeInfos() {
    return bigTableValueTypeInfos;
  }

  public void setBigTableValueTypeInfos(TypeInfo[] bigTableValueTypeInfos) {
    this.bigTableValueTypeInfos = bigTableValueTypeInfos;
  }

  public VectorExpression[] getSlimmedBigTableValueExpressions() {
    return slimmedBigTableValueExpressions;
  }

  public void setSlimmedBigTableValueExpressions(
      VectorExpression[] slimmedBigTableValueExpressions) {
    this.slimmedBigTableValueExpressions = slimmedBigTableValueExpressions;
  }

  public void setBigTableRetainedMapping(VectorColumnOutputMapping bigTableRetainedMapping) {
    this.bigTableRetainedMapping = bigTableRetainedMapping;
  }

  public VectorColumnOutputMapping getBigTableRetainedMapping() {
    return bigTableRetainedMapping;
  }

  public void setBigTableOuterKeyMapping(VectorColumnOutputMapping bigTableOuterKeyMapping) {
    this.bigTableOuterKeyMapping = bigTableOuterKeyMapping;
  }

  public VectorColumnOutputMapping getBigTableOuterKeyMapping() {
    return bigTableOuterKeyMapping;
  }

  public void setSmallTableMapping(VectorColumnSourceMapping smallTableMapping) {
    this.smallTableMapping = smallTableMapping;
  }

  public VectorColumnSourceMapping getSmallTableMapping() {
    return smallTableMapping;
  }

  public void setProjectionMapping(VectorColumnSourceMapping projectionMapping) {
    this.projectionMapping = projectionMapping;
  }

  public VectorColumnSourceMapping getProjectionMapping() {
    return projectionMapping;
  }
}